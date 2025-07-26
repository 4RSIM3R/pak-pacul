use thiserror::Error;

use crate::types::PageId;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Page is full (page_id: {page_id})")]
    PageFull { page_id: crate::types::PageId },
    
    #[error("Invalid slot index {index} (max: {max})")]
    InvalidSlotIndex { index: usize, max: usize },
    
    #[error("Column index {index} out of bounds")]
    ColumnIndexOutOfBounds { index: usize },
    
    #[error("Serialization/deserialization error: {details}")]
    SerializationError { details: String },
    
    #[error("Table '{name}' not found")]
    TableNotFound { name: String },
    
    #[error("Column '{name}' not found in table '{table}'")]
    ColumnNotFound { name: String, table: String },
    
    #[error("SQL parsing error: {details}")]
    SqlParseError { details: String },
    
    #[error("Query execution error: {details}")]
    ExecutionError { details: String },
    
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
    
    #[error("Concurrent access violation")]
    ConcurrencyError,
    
    #[error("Buffer pool exhausted")]
    BufferPoolExhausted,
    
    #[error("Transaction aborted: {reason}")]
    TransactionAborted { reason: String },

    #[error("Invalid page size: {expected} bytes, got {actual} bytes")]
    InvalidPageSize { expected: usize, actual: usize },

    #[error("Corrupted page: page_id={page_id}, reason={reason}")]
    CorruptedPage { page_id: PageId, reason: String },

    #[error("Invalid page type: {0}")]
    InvalidPageType(u8),
}

pub type Result<T> = std::result::Result<T, DatabaseError>;