use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    executor::{scan::Scanner, sequential_scan::SequentialScanner},
    storage::{bplus_tree::BPlusTree, header::BambangHeader, BAMBANG_HEADER_SIZE},
    types::{
        error::DatabaseError, page::{Page, PageType}, row::Row, value::Value, PageId, PAGE_SIZE
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
        };
        storage_manager.load_table_roots()?;
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

    fn load_table_roots(&mut self) -> Result<(), DatabaseError> {
        let schema_page = self.read_page(1)?;
        for i in 0..schema_page.slot_directory.slots.len() {
            if let Some(cell_data) = schema_page.get_cell(i) {
                let row = Row::from_bytes(cell_data)?;
                if row.values.len() >= 4 {
                    if let (Value::Text(table_name), Value::Integer(root_page)) =
                        (&row.values[1], &row.values[3])
                    {
                        self.table_roots
                            .insert(table_name.clone(), *root_page as PageId);
                    }
                }
            }
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
        let root_page_id = self.table_roots.get(table_name).copied().ok_or_else(|| {
            DatabaseError::TableNotFound {
                name: table_name.to_string(),
            }
        })?;
        let table_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.db_info.path)?;
        let mut table_btree =
            BPlusTree::new_with_extras(table_file, root_page_id, Some(BAMBANG_HEADER_SIZE as u64))?;
        if let Some(new_root) = table_btree.insert(row, Some(BAMBANG_HEADER_SIZE as u64))? {
            self.update_table_root(table_name, new_root)?;
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

    fn allocate_new_page(&mut self, page_type: PageType) -> Result<PageId, DatabaseError> {
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

    /// Scan all rows from a table using the scanner
    pub fn scan_table(&self, table_name: &str) -> Result<Vec<Row>, DatabaseError> {
        let mut scanner = self.create_scanner(table_name, None)?;
        let mut rows = Vec::new();

        while let Some(row) = scanner.scan()? {
            rows.push(row);
        }

        Ok(rows)
    }
}
