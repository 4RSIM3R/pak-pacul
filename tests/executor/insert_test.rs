use bambang::{
    executor::insert::{Inserter, TableInserter, InsertIterator},
    types::{error::DatabaseError, row::Row, value::Value},
    utils::mock::TempDatabase,
};

#[test]
fn test_table_inserter_creation() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("inserter_creation");
    let storage = temp_db.create_storage_manager().unwrap();

    // Create a test table
    storage.create_table(
        "test_table",
        "CREATE TABLE test_table(id INTEGER, name TEXT)",
    )?;

    // Create inserter
    let inserter = TableInserter::new(storage, "test_table".to_string())?;
    assert_eq!(inserter.table_name(), "test_table");
    assert!(inserter.root_page_id() > 0);

    Ok(())
}

#[test]
fn test_table_inserter_nonexistent_table() {
    let mut temp_db = TempDatabase::with_prefix("inserter_nonexistent");
    let storage = temp_db.create_storage_manager().unwrap();

    // Try to create inserter for non-existent table
    let result = TableInserter::new(storage, "nonexistent_table".to_string());

    match result {
        Err(DatabaseError::TableNotFound { name }) => {
            assert_eq!(name, "nonexistent_table");
        }
        _ => panic!("Expected TableNotFound error"),
    }
}

#[test]
fn test_single_row_insertion() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("inserter_single");
    let storage = temp_db.create_storage_manager().unwrap();

    // Create a test table
    storage.create_table(
        "test_table",
        "CREATE TABLE test_table(id INTEGER, name TEXT)",
    )?;

    // Create inserter and insert a row
    let mut inserter = TableInserter::new(storage, "test_table".to_string())?;
    let row = Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]);

    inserter.insert(row)?;

    Ok(())
}

#[test]
fn test_insert_iterator_wrapper() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("inserter_iterator");
    let storage = temp_db.create_storage_manager().unwrap();
    
    // Create a test table
    storage.create_table("test_table", "CREATE TABLE test_table(id INTEGER, name TEXT)")?;
    
    // Create inserter and wrap it in iterator
    let inserter = TableInserter::new(storage, "test_table".to_string())?;
    let mut insert_iter = InsertIterator::new(inserter);
    
    assert_eq!(insert_iter.table_name(), "test_table");
    
    // Insert single row
    let row = Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]);
    insert_iter.insert_row(row)?;
    
    // Insert multiple rows
    let rows = vec![
        Row::new(vec![Value::Integer(2), Value::Text("Bob".to_string())]),
        Row::new(vec![Value::Integer(3), Value::Text("Charlie".to_string())]),
    ];
    insert_iter.insert_rows(rows)?;
    
    Ok(())
}

#[test]
fn test_large_batch_insertion() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("inserter_large_batch");
    let storage = temp_db.create_storage_manager().unwrap();
    
    // Create a test table
    storage.create_table("large_test", "CREATE TABLE large_test(id INTEGER, data TEXT)")?;
    
    // Create inserter and insert a large batch
    let mut inserter = TableInserter::new(storage, "large_test".to_string())?;
    
    let mut rows = Vec::new();
    for i in 1..=100 {
        rows.push(Row::new(vec![
            Value::Integer(i),
            Value::Text(format!("data_string_for_row_{}_with_some_padding", i)),
        ]));
    }
    
    inserter.insert_batch(rows)?;
    
    Ok(())
}

#[test]
fn test_insertion_with_mixed_data_types() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("inserter_mixed");
    let storage = temp_db.create_storage_manager().unwrap();
    
    // Create a test table with mixed data types
    storage.create_table("mixed_test", "CREATE TABLE mixed_test(id INTEGER, name TEXT, score REAL, active BOOLEAN)")?;
    
    let mut inserter = TableInserter::new(storage, "mixed_test".to_string())?;
    
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
        inserter.insert(row)?;
    }
    
    Ok(())
}

#[test]
fn test_empty_batch_insertion() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("inserter_empty_batch");
    let storage = temp_db.create_storage_manager().unwrap();
    
    // Create a test table
    storage.create_table("empty_test", "CREATE TABLE empty_test(id INTEGER)")?;
    
    let mut inserter = TableInserter::new(storage, "empty_test".to_string())?;
    
    // Insert empty batch should succeed
    let empty_rows = Vec::new();
    inserter.insert_batch(empty_rows)?;
    
    Ok(())
}

#[test]
fn test_insertion_with_b_plus_tree_splits() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("inserter_splits");
    let storage = temp_db.create_storage_manager().unwrap();
    
    // Create a test table
    storage.create_table("split_test", "CREATE TABLE split_test(id INTEGER, data TEXT)")?;
    
    let mut inserter = TableInserter::new(storage, "split_test".to_string())?;
    let initial_root = inserter.root_page_id();
    
    // Insert enough data to potentially cause B+ tree splits
    for i in 1..=50 {
        let row = Row::new(vec![
            Value::Integer(i),
            Value::Text(format!("data_for_row_{}_with_padding_to_increase_size", i)),
        ]);
        inserter.insert(row)?;
    }
    
    // Root page ID might have changed due to splits
    let final_root = inserter.root_page_id();
    // We can't guarantee a split happened, but the test should complete successfully
    assert!(final_root >= initial_root);
    
    Ok(())
}

#[test]
fn test_inserter_integration_with_scanner() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("inserter_scanner_integration");
    let storage = temp_db.create_storage_manager().unwrap();
    
    // Create a test table
    storage.create_table("integration_test", "CREATE TABLE integration_test(id INTEGER, name TEXT)")?;
    
    // Insert data using the new inserter
    let mut inserter = TableInserter::new(storage, "integration_test".to_string())?;
    
    let test_data = vec![
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "Diana"),
        (5, "Eve"),
    ];
    
    for (id, name) in &test_data {
        let row = Row::new(vec![
            Value::Integer(*id),
            Value::Text(name.to_string()),
        ]);
        inserter.insert(row)?;
    }
    
    // Verify data was inserted correctly by scanning
    let scanned_rows = storage.scan_table("integration_test", None)?;
    assert_eq!(scanned_rows.len(), test_data.len());
    
    // Verify all IDs are present
    let mut scanned_ids: Vec<i64> = scanned_rows.iter()
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
fn test_batch_insertion() -> Result<(), DatabaseError> {
    let mut temp_db = TempDatabase::with_prefix("inserter_batch");
    let storage = temp_db.create_storage_manager().unwrap();

    // Create a test table
    storage.create_table(
        "test_table",
        "CREATE TABLE test_table(id INTEGER, name TEXT)",
    )?;

    // Create inserter and insert multiple rows
    let mut inserter = TableInserter::new(storage, "test_table".to_string())?;
    let rows = vec![
        Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string())]),
        Row::new(vec![Value::Integer(2), Value::Text("Bob".to_string())]),
        Row::new(vec![Value::Integer(3), Value::Text("Charlie".to_string())]),
    ];

    inserter.insert_batch(rows)?;

    Ok(())
}
