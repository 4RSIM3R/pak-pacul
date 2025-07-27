use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    executor::{
        insert::{Inserter, TableInserter},
        predicate::Predicate,
        scan::Scanner,
        sequential_scan::SequentialScanner
    },
    storage::{
        bplus_tree::BPlusTree,
        header::BambangHeader,
        schema::{SchemaManager, TableSchema, ColumnSchema},
        BAMBANG_HEADER_SIZE
    },
    types::{
        error::DatabaseError,
        page::{Page, PageType},
        row::Row,
        value::{Value, DataType},
        PageId,
        PAGE_SIZE
    },
};

pub struct DatabaseInfo {
    pub path: PathBuf,
    pub header: BambangHeader,
    pub page_count: u64,
    pub file_size: u64,
}


pub struct StorageManager {
    pub db_info: DatabaseInfo,
    pub file: File,
    pub table_roots: HashMap<String, PageId>,
    pub schema_manager: SchemaManager,
}

impl StorageManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        let path = path.as_ref();
        let db_info = if path.exists() {
            println!("Opening existing database at path: {}", path.display());
            Self::open_existing(path)?
        } else {
            println!("Creating new database at path: {}", path.display());
            Self::create_new(path)?
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_info.path)?;
        let mut storage_manager = Self {
            db_info,
            file,
            table_roots: HashMap::new(),
            schema_manager: SchemaManager::new(),
        };
        storage_manager.load_table_roots_and_schemas()?;
        Ok(storage_manager)
    }

    fn page_offset(&self, page_id: PageId) -> u64 {
        BAMBANG_HEADER_SIZE as u64 + (page_id - 1) * PAGE_SIZE as u64
    }

    fn read_page(&mut self, page_id: PageId) -> Result<Page, DatabaseError> {
        let mut buffer = vec![0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(self.page_offset(page_id)))?;
        self.file.read_exact(&mut buffer)?;
        Page::from_bytes(&buffer)
    }

    fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<(), DatabaseError> {
        let page_bytes = page.to_bytes()?;
        self.file.seek(SeekFrom::Start(self.page_offset(page_id)))?;
        self.file.write_all(&page_bytes)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> Result<DatabaseInfo, DatabaseError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;
        let header = BambangHeader::default();
        file.write_all(&header.to_bytes())?;
        let schema_page = Self::init_schema_page();
        let page_bytes = schema_page.to_bytes()?;
        file.write_all(&page_bytes)?;
        file.flush()?;
        let file_size = file.metadata()?.len();
        Ok(DatabaseInfo {
            path: path.to_path_buf(),
            header,
            page_count: 1,
            file_size,
        })
    }

    pub fn open_existing<P: AsRef<Path>>(path: P) -> Result<DatabaseInfo, DatabaseError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut header_buffer = vec![0u8; BAMBANG_HEADER_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header_buffer)?;
        let header = BambangHeader::from_bytes(&header_buffer)?;
        if header.file_format_write_version > 2 || header.file_format_read_version > 2 {
            return Err(DatabaseError::UnsupportedFileFormat {
                version: header.file_format_write_version,
            });
        }
        let file_size = file.metadata()?.len();
        let data_size = file_size - BAMBANG_HEADER_SIZE as u64;
        let page_count = data_size / PAGE_SIZE as u64;
        if page_count != header.database_size_pages.into() {
            return Err(DatabaseError::CorruptedDatabase {
                reason: "File size doesn't match header".to_string(),
            });
        }
        Ok(DatabaseInfo {
            path: path.to_path_buf(),
            header,
            page_count,
            file_size,
        })
    }

    fn load_table_roots_and_schemas(&mut self) -> Result<(), DatabaseError> {
        let schema_page = self.read_page(1)?;
        let mut table_schemas: HashMap<String, (PageId, String, Vec<ColumnSchema>)> = HashMap::new();
        
        for i in 0..schema_page.slot_directory.slots.len() {
            if let Some(cell_data) = schema_page.get_cell(i) {
                let row = Row::from_bytes(cell_data)?;
                if row.values.len() >= 5 {
                    match &row.values[0] {
                        Value::Text(entry_type) if entry_type == "table" => {
                            // Table entry: type, name, tbl_name, rootpage, sql
                            if let (Value::Text(table_name), Value::Integer(root_page), Value::Text(sql)) =
                                (&row.values[1], &row.values[3], &row.values[4])
                            {
                                self.table_roots.insert(table_name.clone(), *root_page as PageId);
                                table_schemas.insert(
                                    table_name.clone(),
                                    (*root_page as PageId, sql.clone(), Vec::new())
                                );
                            }
                        }
                        Value::Text(entry_type) if entry_type == "column" => {
                            // Column entry: type, name, tbl_name, position, data_type, nullable, default, primary_key, unique
                            if row.values.len() >= 9 {
                                if let Value::Text(table_name) = &row.values[2] {
                                    let column_schema = ColumnSchema::from_schema_row(&row)?;
                                    if let Some((_, _, columns)) = table_schemas.get_mut(table_name) {
                                        columns.push(column_schema);
                                    }
                                }
                            }
                        }
                        _ => {} // Ignore other entry types
                    }
                }
            }
        }
        
        // Create TableSchema objects and add them to schema manager
        for (table_name, (root_page_id, sql, mut columns)) in table_schemas {
            // Sort columns by position
            columns.sort_by_key(|col| col.position);
            let table_schema = TableSchema::new(table_name.clone(), columns, root_page_id, sql);
            self.schema_manager.add_table_schema(table_schema);
        }
        
        Ok(())
    }

    pub fn create_table(&mut self, table_name: &str, sql: &str) -> Result<PageId, DatabaseError> {
        let new_root_page_id = self.allocate_new_page(PageType::LeafTable)?;
        let schema_row = Row::new(vec![
            Value::Text("table".to_string()),
            Value::Text(table_name.to_string()),
            Value::Text(table_name.to_string()),
            Value::Integer(new_root_page_id as i64),
            Value::Text(sql.to_string()),
        ]);
        let schema_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.db_info.path)?;
        let mut schema_btree =
            BPlusTree::new_with_extras(schema_file, 1, Some(BAMBANG_HEADER_SIZE as u64))?;
        if let Some(new_root) = schema_btree.insert(schema_row, Some(BAMBANG_HEADER_SIZE as u64))? {
            self.table_roots
                .insert("sqlite_schema".to_string(), new_root);
        }
        self.table_roots
            .insert(table_name.to_string(), new_root_page_id);
        Ok(new_root_page_id)
    }

    pub fn insert_into_table(&mut self, table_name: &str, row: Row) -> Result<(), DatabaseError> {
        // Create a TableInserter and delegate the insertion
        let mut inserter = TableInserter::new(self, table_name.to_string())?;
        inserter.insert(row)?;
        
        // Update the root page ID if it changed during insertion
        let new_root_page_id = inserter.root_page_id();
        if let Some(current_root) = self.table_roots.get(table_name) {
            if *current_root != new_root_page_id {
                self.update_table_root(table_name, new_root_page_id)?;
            }
        }
        
        Ok(())
    }

    fn update_table_root(
        &mut self,
        table_name: &str,
        new_root_page_id: PageId,
    ) -> Result<(), DatabaseError> {
        self.table_roots
            .insert(table_name.to_string(), new_root_page_id);
        println!(
            "Updated root page for table '{}' to page {}",
            table_name, new_root_page_id
        );
        Ok(())
    }

    pub fn allocate_new_page(&mut self, page_type: PageType) -> Result<PageId, DatabaseError> {
        let new_page_id = self.db_info.page_count + 1;
        let new_page = Page::new(new_page_id, page_type);
        self.write_page(new_page_id, &new_page)?;
        self.db_info.page_count = new_page_id;
        self.db_info.file_size += PAGE_SIZE as u64;
        self.db_info.header.database_size_pages = new_page_id as u32;
        self.update_header_in_file()?;
        Ok(new_page_id)
    }

    fn update_header_in_file(&mut self) -> Result<(), DatabaseError> {
        let header_bytes = self.db_info.header.to_bytes();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        self.file.flush()?;
        Ok(())
    }

    fn init_schema_page() -> Page {
        let mut schema_page = Page::new(1, PageType::LeafTable);
        let schema_table_row = Row::new(vec![
            Value::Text("table".to_string()),
            Value::Text("sqlite_schema".to_string()),
            Value::Text("sqlite_schema".to_string()),
            Value::Integer(1),
            Value::Text("CREATE TABLE sqlite_schema(type text,name text,tbl_name text,rootpage integer,sql text)".to_string()),
        ]);
        let row_bytes = schema_table_row.to_bytes();
        let _ = schema_page.insert_cell(&row_bytes, None);
        schema_page
    }

    /// Create a sequential scanner for the specified table
    pub fn create_scanner(
        &self,
        table_name: &str,
        batch_size: Option<usize>,
    ) -> Result<SequentialScanner, DatabaseError> {
        SequentialScanner::new(self, table_name.to_string(), batch_size)
    }

    /// Scan all rows from a table using the scanner, optionally with predicate filtering
    pub fn scan_table(&self, table_name: &str, predicate: Option<Predicate>) -> Result<Vec<Row>, DatabaseError> {
        let mut scanner = self.create_scanner(table_name, None)?;
        let mut rows = Vec::new();

        // Get table schema for predicate validation and evaluation if predicate is provided
        let table_schema = if predicate.is_some() {
            Some(self.get_table_schema(table_name)
                .ok_or_else(|| DatabaseError::TableNotFound {
                    name: table_name.to_string(),
                })?)
        } else {
            None
        };

        // Validate predicate against schema if provided
        if let (Some(pred), Some(schema)) = (&predicate, &table_schema) {
            pred.validate_against_schema(schema)?;
        }

        while let Some(row) = scanner.scan()? {
            // Apply predicate filtering if provided
            let matches = if let (Some(pred), Some(schema)) = (&predicate, &table_schema) {
                pred.evaluate(&row, schema)?
            } else {
                true // No predicate means all rows match
            };

            if matches {
                rows.push(row);
            }
        }

        Ok(rows)
    }

    /// Create a table inserter for the specified table
    pub fn create_inserter(&self, table_name: &str) -> Result<TableInserter, DatabaseError> {
        TableInserter::new(self, table_name.to_string())
    }

    /// Insert multiple rows into a table using batch insertion
    pub fn insert_batch_into_table(&mut self, table_name: &str, rows: Vec<Row>) -> Result<(), DatabaseError> {
        if rows.is_empty() {
            return Ok(());
        }

        // Create a TableInserter and delegate the batch insertion
        let mut inserter = TableInserter::new(self, table_name.to_string())?;
        inserter.insert_batch(rows)?;
        
        // Update the root page ID if it changed during insertion
        let new_root_page_id = inserter.root_page_id();
        if let Some(current_root) = self.table_roots.get(table_name) {
            if *current_root != new_root_page_id {
                self.update_table_root(table_name, new_root_page_id)?;
            }
        }
        
        Ok(())
    }

    /// Get table schema by name
    pub fn get_table_schema(&self, table_name: &str) -> Option<&TableSchema> {
        self.schema_manager.get_table_schema(table_name)
    }

    /// Add a new table schema and persist it
    pub fn add_table_schema(&mut self, schema: TableSchema) -> Result<(), DatabaseError> {
        // Store table entry in sqlite_schema
        let table_row = Row::new(vec![
            Value::Text("table".to_string()),
            Value::Text(schema.table_name.clone()),
            Value::Text(schema.table_name.clone()),
            Value::Integer(schema.root_page_id as i64),
            Value::Text(schema.sql.clone()),
        ]);

        let schema_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.db_info.path)?;
        let mut schema_btree =
            BPlusTree::new_with_extras(schema_file, 1, Some(BAMBANG_HEADER_SIZE as u64))?;
        
        // Insert table entry
        if let Some(new_root) = schema_btree.insert(table_row, Some(BAMBANG_HEADER_SIZE as u64))? {
            self.table_roots.insert("sqlite_schema".to_string(), new_root);
        }

        // Store column entries
        for column in &schema.columns {
            let column_row = column.to_schema_row(&schema.table_name);
            let schema_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.db_info.path)?;
            let mut schema_btree =
                BPlusTree::new_with_extras(schema_file, 1, Some(BAMBANG_HEADER_SIZE as u64))?;
            
            if let Some(new_root) = schema_btree.insert(column_row, Some(BAMBANG_HEADER_SIZE as u64))? {
                self.table_roots.insert("sqlite_schema".to_string(), new_root);
            }
        }

        // Add to in-memory schema manager
        self.table_roots.insert(schema.table_name.clone(), schema.root_page_id);
        self.schema_manager.add_table_schema(schema);
        
        Ok(())
    }

    /// Validate a row against table schema
    pub fn validate_row(&self, table_name: &str, row: &Row) -> Result<(), DatabaseError> {
        if let Some(schema) = self.get_table_schema(table_name) {
            schema.validate_row(row)
        } else {
            Err(DatabaseError::TableNotFound {
                name: table_name.to_string(),
            })
        }
    }

    /// Apply default values to a row based on table schema
    pub fn apply_defaults(&self, table_name: &str, row: &mut Row) -> Result<(), DatabaseError> {
        if let Some(schema) = self.get_table_schema(table_name) {
            schema.apply_defaults(row)
        } else {
            Err(DatabaseError::TableNotFound {
                name: table_name.to_string(),
            })
        }
    }

    /// Check if a table exists
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.schema_manager.table_exists(table_name)
    }

    /// Get all table names
    pub fn get_table_names(&self) -> Vec<String> {
        self.schema_manager.table_names().iter().map(|s| s.to_string()).collect()
    }
}
