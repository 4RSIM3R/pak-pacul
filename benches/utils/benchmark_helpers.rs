use std::time::{Duration, Instant};
use bambang::{
    executor::{scan::Scanner, sequential_scan::SequentialScanner},
    storage::storage_manager::StorageManager,
    types::error::DatabaseError,
    utils::mock::TempDatabase,
};
use crate::utils::data_generator::{DataGenerator, RowType};

/// Setup a pre-populated database for benchmarking
pub struct BenchmarkDatabase {
    pub temp_db: TempDatabase,
    pub table_name: String,
    pub row_count: usize,
    pub row_type: RowType,
}

impl BenchmarkDatabase {
    pub fn new(table_name: &str, row_count: usize, row_type: RowType) -> Result<Self, Box<dyn std::error::Error>> {
        let mut temp_db = TempDatabase::with_prefix("bench_optimized");
        let storage = temp_db.create_storage_manager()?;
        
        // Create table
        let create_sql = match row_type {
            RowType::Small => "CREATE TABLE test_table(id INTEGER, name TEXT)",
            RowType::Medium => "CREATE TABLE test_table(id INTEGER, name TEXT, score REAL, active BOOLEAN)",
            RowType::Large => "CREATE TABLE test_table(id INTEGER, description TEXT, data BLOB, metadata TEXT)",
        };
        storage.create_table(table_name, create_sql)?;
        
        // Insert data
        let data_generator = DataGenerator::new();
        for i in 1..=row_count {
            let row = data_generator.generate_row(i as i64, row_type);
            storage.insert_into_table(table_name, row)?;
        }
        
        Ok(Self {
            temp_db,
            table_name: table_name.to_string(),
            row_count,
            row_type,
        })
    }
    
    pub fn get_storage(&mut self) -> Option<&mut StorageManager> {
        self.temp_db.get_storage_manager()
    }
}

/// Measure only the scan operation, excluding setup
pub fn measure_scan_operation(
    storage: &StorageManager,
    table_name: &str,
    expected_count: usize,
) -> Result<Duration, DatabaseError> {
    let mut scanner = SequentialScanner::new(storage, table_name.to_string(), None)?;
    
    let start = Instant::now();
    let mut count = 0;
    while let Some(_row) = scanner.scan()? {
        count += 1;
    }
    let duration = start.elapsed();
    
    assert_eq!(count, expected_count);
    Ok(duration)
}

/// Measure batch scan operation
pub fn measure_batch_scan_operation(
    storage: &StorageManager,
    table_name: &str,
    batch_size: usize,
    expected_count: usize,
) -> Result<Duration, DatabaseError> {
    let mut scanner = SequentialScanner::new(storage, table_name.to_string(), Some(batch_size))?;
    
    let start = Instant::now();
    let mut total_rows = 0;
    loop {
        let batch = scanner.scan_batch(batch_size)?;
        if batch.is_empty() {
            break;
        }
        total_rows += batch.len();
    }
    let duration = start.elapsed();
    
    assert_eq!(total_rows, expected_count);
    Ok(duration)
}

/// Measure scanner reset operation
pub fn measure_reset_operation(
    storage: &StorageManager,
    table_name: &str,
    partial_scan_count: usize,
) -> Result<Duration, DatabaseError> {
    let mut scanner = SequentialScanner::new(storage, table_name.to_string(), None)?;
    
    // Perform partial scan
    for _ in 0..partial_scan_count {
        let _ = scanner.scan()?;
    }
    
    // Measure reset operation
    let start = Instant::now();
    scanner.reset()?;
    let reset_duration = start.elapsed();
    
    Ok(reset_duration)
}