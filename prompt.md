## Question

I already develop a storage manager, and had the test, looks like it confusing to parse the root table, because we include
that header, BAMBANG_HEADER_SIZE (100 bytes) on that first page, it is really make hard to parse the page, are you have option
how to solve it? ensure approach used is re-usable on future usage and not break another function

-- storage_manager.rs
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    storage::{BAMBANG_HEADER_SIZE, bplus_tree::BPlusTree, header::BambangHeader},
    types::{
        PAGE_SIZE, PageId,
        error::DatabaseError,
        page::{Page, PageType},
        row::Row,
        value::Value,
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

    pub fn create_new<P: AsRef<Path>>(path: P) -> Result<DatabaseInfo, DatabaseError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;
        let header = BambangHeader::default();
        let schema_page = Self::init_schema_page();
        let mut page_bytes = schema_page.to_bytes()?;
        page_bytes[0..BAMBANG_HEADER_SIZE].copy_from_slice(&header.to_bytes());
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
        file.read_exact(&mut header_buffer)?;
        let header = BambangHeader::from_bytes(&header_buffer)?;
        if header.file_format_write_version > 2 || header.file_format_read_version > 2 {
            return Err(DatabaseError::UnsupportedFileFormat {
                version: header.file_format_write_version,
            });
        }
        let file_size = file.metadata()?.len();
        let page_count = file_size / PAGE_SIZE as u64;
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
        let mut buffer = vec![0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut buffer)?;
        let schema_page = Page::from_bytes(&buffer)?;
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
        let mut schema_btree = BPlusTree::new(schema_file, 1)?;
        if let Some(new_root) = schema_btree.insert(schema_row)? {
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
        let mut table_btree = BPlusTree::new(table_file, root_page_id)?;
        if let Some(new_root) = table_btree.insert(row)? {
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
        let page_bytes = new_page.to_bytes()?;
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&page_bytes)?;
        self.file.flush()?;
        self.db_info.page_count = new_page_id;
        self.db_info.file_size += PAGE_SIZE as u64;
        Ok(new_page_id)
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
}
