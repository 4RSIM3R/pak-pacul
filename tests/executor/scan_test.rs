use bambang::{
    executor::{
        scan::{ScanIterator, Scanner},
        sequential_scan::SequentialScanner,
    },
    types::{error::DatabaseError, row::Row, value::Value},
    utils::mock::TempDatabase,
};
use std::collections::HashSet;

#[test]
fn test_sequential_scanner_basic_functionality() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_basic");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table(
        "test_table",
        "CREATE TABLE test_table(id INTEGER, name TEXT)",
    )?;
    let test_data = vec![
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "Diana"),
        (5, "Eve"),
    ];
    for (id, name) in &test_data {
        let row = Row::new(vec![Value::Integer(*id), Value::Text(name.to_string())]);
        storage.insert_into_table("test_table", row)?;
    }
    let mut scanner = SequentialScanner::new(storage, "test_table".to_string(), None)?;
    let mut scanned_rows = Vec::new();
    while let Some(row) = scanner.scan()? {
        scanned_rows.push(row);
    }
    assert_eq!(scanned_rows.len(), test_data.len());
    let mut scanned_ids: Vec<i64> = scanned_rows
        .iter()
        .map(|row| match &row.values[0] {
            Value::Integer(id) => *id,
            _ => panic!("Expected integer ID"),
        })
        .collect();
    scanned_ids.sort();
    let expected_ids: Vec<i64> = test_data.iter().map(|(id, _)| *id).collect();
    assert_eq!(scanned_ids, expected_ids);
    Ok(())
}

#[test]
fn test_scanner_reset_functionality() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_reset");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table("reset_test", "CREATE TABLE reset_test(id INTEGER)")?;
    for i in 1..=3 {
        let row = Row::new(vec![Value::Integer(i)]);
        storage.insert_into_table("reset_test", row)?;
    }
    let mut scanner = SequentialScanner::new(storage, "reset_test".to_string(), None)?;
    let first_row = scanner.scan()?;
    assert!(first_row.is_some());
    scanner.reset()?;
    let mut count = 0;
    while let Some(_) = scanner.scan()? {
        count += 1;
    }
    assert_eq!(count, 3);
    Ok(())
}

#[test]
fn test_batch_scanning() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_batch");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table(
        "batch_test",
        "CREATE TABLE batch_test(id INTEGER, value TEXT)",
    )?;
    for i in 1..=10 {
        let row = Row::new(vec![Value::Integer(i), Value::Text(format!("value_{}", i))]);
        storage.insert_into_table("batch_test", row)?;
    }
    let mut scanner = SequentialScanner::new(storage, "batch_test".to_string(), Some(3))?;
    let mut total_rows = 0;
    let mut batch_count = 0;
    loop {
        let batch = scanner.scan_batch(3)?;
        if batch.is_empty() {
            break;
        }
        batch_count += 1;
        total_rows += batch.len();
        if batch_count < 4 {
            assert_eq!(batch.len(), 3);
        } else {
            assert!(batch.len() <= 3);
        }
    }
    assert_eq!(total_rows, 10);
    assert_eq!(batch_count, 4);
    Ok(())
}

#[test]
fn test_scan_iterator_wrapper() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_iterator");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table("iterator_test", "CREATE TABLE iterator_test(id INTEGER)")?;
    for i in 1..=5 {
        let row = Row::new(vec![Value::Integer(i)]);
        storage.insert_into_table("iterator_test", row)?;
    }
    let scanner = SequentialScanner::new(storage, "iterator_test".to_string(), None)?;
    let iter = ScanIterator::new(scanner);
    let rows: Result<Vec<_>, _> = iter.collect();
    let rows = rows?;
    assert_eq!(rows.len(), 5);
    let scanner2 = SequentialScanner::new(storage, "iterator_test".to_string(), None)?;
    let iter2 = ScanIterator::new(scanner2);
    let count = iter2.count();
    assert_eq!(count, 5);
    Ok(())
}

// #[test] TODO: Fix this
// fn test_scanner_with_large_dataset() -> Result<(), DatabaseError> {
//     let mut temp_db = TempDatabase::with_prefix("scan_large");
//     let storage = temp_db.create_storage_manager().unwrap();
//     storage.create_table(
//         "large_test",
//         "CREATE TABLE large_test(id INTEGER, data TEXT)",
//     )?;
//     for i in 1..=6_000 {
//         let row = Row::new(vec![
//             Value::Integer(i),
//             Value::Text(format!("data_string_for_row_{}_with_some_padding", i)),
//         ]);
//         storage.insert_into_table("large_test", row)?;
//     }
//     let mut scanner = SequentialScanner::new(storage, "large_test".to_string(), None)?;
//     let mut count = 0;
//     let mut seen_ids = HashSet::new();
//     while let Some(row) = scanner.scan()? {
//         count += 1;
//         assert_eq!(row.values.len(), 2);
//         if let Value::Integer(id) = &row.values[0] {
//             assert!(!seen_ids.contains(id), "Duplicate ID found: {}", id);
//             seen_ids.insert(*id);
//             assert!(*id >= 1 && *id <= 6_000);
//         } else {
//             panic!("Expected integer ID");
//         }
//         if let Value::Text(data) = &row.values[1] {
//             assert!(data.contains("data_string_for_row_"));
//         } else {
//             panic!("Expected text data");
//         }
//     }
//     // assert_eq!(count, 6_000);
//     assert_eq!(seen_ids.len(), 6_000);
//     Ok(())
// }

#[test]
fn test_scanner_with_empty_table() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_empty");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table("empty_test", "CREATE TABLE empty_test(id INTEGER)")?;
    let mut scanner = SequentialScanner::new(storage, "empty_test".to_string(), None)?;
    let result = scanner.scan()?;
    assert!(result.is_none());
    let batch = scanner.scan_batch(10)?;
    assert!(batch.is_empty());
    Ok(())
}

#[test]
fn test_scanner_nonexistent_table() {
    let mut temp_db = TempDatabase::with_prefix("scan_nonexistent");
    let storage = temp_db.create_storage_manager().unwrap();
    let result = SequentialScanner::new(storage, "nonexistent_table".to_string(), None);
    match result {
        Err(DatabaseError::TableNotFound { name }) => {
            assert_eq!(name, "nonexistent_table");
        }
        _ => panic!("Expected TableNotFound error"),
    }
}

#[test]
fn test_scanner_with_mixed_data_types() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_mixed");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table(
        "mixed_test",
        "CREATE TABLE mixed_test(id INTEGER, name TEXT, score REAL, active BOOLEAN)",
    )?;
    let test_data = vec![
        (1, "Alice", 95.5, true),
        (2, "Bob", 87.2, false),
        (3, "Charlie", 92.0, true),
    ];
    for (id, name, score, active) in &test_data {
        let row = Row::new(vec![
            Value::Integer(*id),
            Value::Text(name.to_string()),
            Value::Real(*score),
            Value::Boolean(*active),
        ]);
        storage.insert_into_table("mixed_test", row)?;
    }
    let mut scanner = SequentialScanner::new(storage, "mixed_test".to_string(), None)?;
    let mut scanned_count = 0;
    while let Some(row) = scanner.scan()? {
        scanned_count += 1;
        assert_eq!(row.values.len(), 4);
        assert!(matches!(row.values[0], Value::Integer(_)));
        assert!(matches!(row.values[1], Value::Text(_)));
        assert!(matches!(row.values[2], Value::Real(_)));
        assert!(matches!(row.values[3], Value::Boolean(_)));
    }
    assert_eq!(scanned_count, test_data.len());
    Ok(())
}

#[test]
fn test_scanner_memory_efficiency() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_memory");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table(
        "memory_test",
        "CREATE TABLE memory_test(id INTEGER, large_text TEXT)",
    )?;
    for i in 1..=20 {
        let large_text = "x".repeat(1000);
        let row = Row::new(vec![Value::Integer(i), Value::Text(large_text)]);
        storage.insert_into_table("memory_test", row)?;
    }
    let mut scanner = SequentialScanner::new(storage, "memory_test".to_string(), Some(5))?;
    let mut total_scanned = 0;
    while let Some(_row) = scanner.scan()? {
        total_scanned += 1;
    }
    assert_eq!(total_scanned, 20);
    Ok(())
}

#[test]
fn test_scanner_integration_with_storage_manager() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_integration");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table(
        "integration_test",
        "CREATE TABLE integration_test(id INTEGER, name TEXT)",
    )?;
    for i in 1..=5 {
        let row = Row::new(vec![Value::Integer(i), Value::Text(format!("name_{}", i))]);
        storage.insert_into_table("integration_test", row)?;
    }
    let mut scanner = storage.create_scanner("integration_test", Some(2))?;
    let mut count = 0;
    while let Some(_row) = scanner.scan()? {
        count += 1;
    }
    assert_eq!(count, 5);
    let all_rows = storage.scan_table("integration_test", None)?;
    assert_eq!(all_rows.len(), 5);
    for row in &all_rows {
        assert_eq!(row.values.len(), 2);
        assert!(matches!(row.values[0], Value::Integer(_)));
        assert!(matches!(row.values[1], Value::Text(_)));
    }
    Ok(())
}

#[test]
fn test_scanner_with_b_plus_tree_splits() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_splits");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table(
        "split_test",
        "CREATE TABLE split_test(id INTEGER, data TEXT)",
    )?;
    for i in 1..=50 {
        let row = Row::new(vec![
            Value::Integer(i),
            Value::Text(format!("data_for_row_{}_with_padding_to_increase_size", i)),
        ]);
        storage.insert_into_table("split_test", row)?;
    }
    let mut scanner = SequentialScanner::new(storage, "split_test".to_string(), None)?;
    let mut scanned_ids = Vec::new();
    while let Some(row) = scanner.scan()? {
        if let Value::Integer(id) = &row.values[0] {
            scanned_ids.push(*id);
        }
    }
    assert_eq!(scanned_ids.len(), 50);
    scanned_ids.sort();
    let expected_ids: Vec<i64> = (1..=50).collect();
    assert_eq!(scanned_ids, expected_ids);
    Ok(())
}

#[test]
fn test_scanner_leaf_page_traversal() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_traversal");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table(
        "traversal_test",
        "CREATE TABLE traversal_test(id INTEGER, data TEXT)",
    )?;
    for i in 1..=30 {
        let row = Row::new(vec![
            Value::Integer(i),
            Value::Text(format!(
                "large_data_string_for_row_{}_to_fill_pages_efficiently",
                i
            )),
        ]);
        storage.insert_into_table("traversal_test", row)?;
    }
    let mut scanner = SequentialScanner::new(storage, "traversal_test".to_string(), None)?;
    let mut rows_found = Vec::new();
    while let Some(row) = scanner.scan()? {
        if let Value::Integer(id) = &row.values[0] {
            rows_found.push(*id);
        }
    }
    assert_eq!(rows_found.len(), 30);
    let mut unique_ids = rows_found.clone();
    unique_ids.sort();
    unique_ids.dedup();
    assert_eq!(unique_ids.len(), 30);
    Ok(())
}

#[test]
fn test_scanner_slot_directory_efficiency() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("scan_slots");
    let storage = temp_db.create_storage_manager().unwrap();
    storage.create_table(
        "slot_test",
        "CREATE TABLE slot_test(id INTEGER, small_data TEXT)",
    )?;
    for i in 1..=15 {
        let row = Row::new(vec![Value::Integer(i), Value::Text(format!("data_{}", i))]);
        storage.insert_into_table("slot_test", row)?;
    }
    let mut scanner = SequentialScanner::new(storage, "slot_test".to_string(), None)?;
    let mut count = 0;
    while let Some(row) = scanner.scan()? {
        count += 1;
        assert_eq!(row.values.len(), 2);
        assert!(matches!(row.values[0], Value::Integer(_)));
        assert!(matches!(row.values[1], Value::Text(_)));
    }
    assert_eq!(count, 15);
    Ok(())
}
