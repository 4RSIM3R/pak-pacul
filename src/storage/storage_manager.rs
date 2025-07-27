use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use crate::{
    storage::{BAMBANG_HEADER_SIZE, header::BambangHeader},
    types::{
        PAGE_SIZE,
        error::DatabaseError,
        page::{Page, PageType},
        row::Row,
        value::Value,
    },
};

pub struct DatabaseInfo {
    pub path: PathBuf,
    pub header: BambangHeader,
    pub page_count: u32,
    pub file_size: u64,
}

pub struct StorageManager;

impl StorageManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<DatabaseInfo, DatabaseError> {
        let path = path.as_ref();

        if path.exists() {
            Self::open_existing(path)
        } else {
            Self::create_new(path)
        }
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
        let page_count = (file_size / PAGE_SIZE as u64) as u32;

        if page_count != header.database_size_pages {
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

    pub fn validate_file<P: AsRef<Path>>(path: P) -> Result<bool, DatabaseError> {
        let path = path.as_ref();

        if !path.exists() {
            return Ok(false);
        }

        let mut file = File::open(path)?;
        let mut header_buffer = vec![0u8; BAMBANG_HEADER_SIZE];

        match file.read_exact(&mut header_buffer) {
            Ok(_) => match BambangHeader::from_bytes(&header_buffer) {
                Ok(_) => Ok(true),
                Err(_) => Ok(false),
            },
            Err(_) => Ok(false),
        }
    }

    pub fn get_file_info<P: AsRef<Path>>(path: P) -> Result<DatabaseInfo, DatabaseError> {
        let path = path.as_ref();
        let mut file = File::open(path)?;

        let mut header_buffer = vec![0u8; BAMBANG_HEADER_SIZE];
        file.read_exact(&mut header_buffer)?;

        let header = BambangHeader::from_bytes(&header_buffer)?;
        let file_size = file.metadata()?.len();
        let page_count = (file_size / PAGE_SIZE as u64) as u32;

        Ok(DatabaseInfo {
            path: path.to_path_buf(),
            header,
            page_count,
            file_size,
        })
    }

    fn init_schema_page() -> Page {
        let mut schema_page = Page::new(1, PageType::LeafTable);

        // Adjust free space offset to account for SQLite header (100 bytes)
        schema_page.free_space_offset = (PAGE_SIZE - BAMBANG_HEADER_SIZE) as u16;

        let schema_table_row = Row::new(vec![
            Value::Text("table".to_string()),
            Value::Text("sqlite_schema".to_string()),
            Value::Text("sqlite_schema".to_string()),
            Value::Integer(1),
            Value::Text("CREATE TABLE sqlite_schema(type text,name text,tbl_name text,rootpage integer,sql text)".to_string()),
        ]);

        let row_bytes = schema_table_row.to_bytes();
        let _ = schema_page.insert_cell(&row_bytes, Some(1));
        

        schema_page
    }
}
