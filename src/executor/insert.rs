use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};

use crate::{
    storage::{bplus_tree::BPlusTree, storage_manager::StorageManager, BAMBANG_HEADER_SIZE},
    types::{
        error::DatabaseError,
        row::Row,
        PageId,
    },
};

/// Trait for inserting data into database tables
pub trait Inserter {
    /// Insert a single row into the table
    fn insert(&mut self, row: Row) -> Result<(), DatabaseError>;
    
    /// Insert multiple rows in a batch operation
    fn insert_batch(&mut self, rows: Vec<Row>) -> Result<(), DatabaseError>;
    
    /// Get the table name this inserter operates on
    fn table_name(&self) -> &str;
}

/// Table inserter implementation that handles insertion operations for a specific table
pub struct TableInserter {
    table_name: String,
    root_page_id: PageId,
    db_file_path: PathBuf,
    extras: Option<u64>,
}

impl TableInserter {
    /// Create a new TableInserter for the specified table
    pub fn new(
        storage_manager: &StorageManager,
        table_name: String,
    ) -> Result<Self, DatabaseError> {
        let root_page_id = storage_manager
            .table_roots
            .get(&table_name)
            .copied()
            .ok_or_else(|| DatabaseError::TableNotFound {
                name: table_name.clone(),
            })?;

        let db_file_path = storage_manager.db_info.path.clone();
        let extras = Some(BAMBANG_HEADER_SIZE as u64);

        Ok(Self {
            table_name,
            root_page_id,
            db_file_path,
            extras,
        })
    }

    /// Update the root page ID for this table (used when B+ tree splits cause root changes)
    pub fn update_root_page_id(&mut self, new_root_page_id: PageId) {
        self.root_page_id = new_root_page_id;
    }

    /// Get the current root page ID
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Open the database file for writing
    fn open_db_file(&self) -> Result<File, DatabaseError> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.db_file_path)
            .map_err(DatabaseError::from)
    }

    /// Create a B+ tree instance for this table
    fn create_btree(&self) -> Result<BPlusTree, DatabaseError> {
        let file = self.open_db_file()?;
        BPlusTree::new_with_extras(file, self.root_page_id, self.extras)
    }
}

impl Inserter for TableInserter {
    fn insert(&mut self, row: Row) -> Result<(), DatabaseError> {
        // Validate row data before insertion
        let row_bytes = row.to_bytes();
        if row_bytes.is_empty() {
            return Err(DatabaseError::SerializationError {
                details: "Cannot insert empty row".to_string(),
            });
        }

        // Create B+ tree instance and perform insertion
        let mut btree = self.create_btree()?;
        
        // Insert the row and handle potential root page changes
        if let Some(new_root_page_id) = btree.insert(row, self.extras)? {
            self.update_root_page_id(new_root_page_id);
        }

        Ok(())
    }

    fn insert_batch(&mut self, rows: Vec<Row>) -> Result<(), DatabaseError> {
        if rows.is_empty() {
            return Ok(());
        }

        // Validate all rows before starting batch insertion
        for (index, row) in rows.iter().enumerate() {
            let row_bytes = row.to_bytes();
            if row_bytes.is_empty() {
                return Err(DatabaseError::SerializationError {
                    details: format!("Cannot insert empty row at index {}", index),
                });
            }
        }

        // Create B+ tree instance once for the entire batch
        let mut btree = self.create_btree()?;
        
        // Insert all rows in the batch
        for (index, row) in rows.into_iter().enumerate() {
            match btree.insert(row, self.extras) {
                Ok(Some(new_root_page_id)) => {
                    self.update_root_page_id(new_root_page_id);
                }
                Ok(None) => {
                    // Normal insertion, no root change
                }
                Err(e) => {
                    return Err(DatabaseError::CorruptedDatabase {
                        reason: format!("Failed to insert row at index {}: {}", index, e),
                    });
                }
            }
        }

        Ok(())
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }
}

/// Iterator wrapper for batch insertion operations
pub struct InsertIterator<I: Inserter> {
    inserter: I,
}

impl<I: Inserter> InsertIterator<I> {
    pub fn new(inserter: I) -> Self {
        Self { inserter }
    }

    /// Insert a single row using the wrapped inserter
    pub fn insert_row(&mut self, row: Row) -> Result<(), DatabaseError> {
        self.inserter.insert(row)
    }

    /// Insert multiple rows using the wrapped inserter
    pub fn insert_rows(&mut self, rows: Vec<Row>) -> Result<(), DatabaseError> {
        self.inserter.insert_batch(rows)
    }

    /// Get the table name from the wrapped inserter
    pub fn table_name(&self) -> &str {
        self.inserter.table_name()
    }
}
