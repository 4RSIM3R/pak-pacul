use std::{
    collections::VecDeque,
    fs::File,
    io::{Read, Seek, SeekFrom},
};
use crate::{
    storage::storage_manager::StorageManager,
    types::{
        PAGE_SIZE, PageId,
        error::DatabaseError,
        page::{Page, PageType},
        row::Row,
    },
};

pub trait Scanner {
    fn scan(&mut self) -> Result<Option<Row>, DatabaseError>;
    fn scan_batch(&mut self, batch_size: usize) -> Result<Vec<Row>, DatabaseError>;
    fn reset(&mut self) -> Result<(), DatabaseError>;
}


pub struct ScanIterator<S: Scanner> {
    scanner: S,
}

impl<S: Scanner> ScanIterator<S> {
    pub fn new(scanner: S) -> Self {
        Self { scanner }
    }
}

impl<S: Scanner> Iterator for ScanIterator<S> {
    type Item = Result<Row, DatabaseError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.scanner.scan() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}