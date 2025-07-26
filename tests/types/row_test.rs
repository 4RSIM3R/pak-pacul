use bambang::types::{error::DatabaseError, row::Row, value::Value};

#[test]
fn test_serialization_waste() {
    // Small row with minimal data
    let row = Row::new(vec![
        Value::Integer(42),   // Should be ~9 bytes (1 type + 8 data)
        Value::Boolean(true), // Should be ~2 bytes (1 type + 1 data)
        Value::Null,          // Should be ~1 byte (just type)
    ]);

    let serialized = row.to_bytes();
    let calculated_size = row.size();

    println!("Calculated size: {} bytes", calculated_size);
    println!("Actual serialized size: {} bytes", serialized.len());
    println!(
        "Waste ratio: {:.2}x",
        serialized.len() as f64 / calculated_size as f64
    );

    assert_eq!(
        serialized.len(),
        calculated_size,
        "Serialization correctness check passed"
    );
}

// Helper function to create test rows
fn create_test_row() -> Row {
    Row::new(vec![
        Value::Integer(42),
        Value::Text("hello".to_string()),
        Value::Real(3.14),
        Value::Boolean(true),
        Value::Null,
    ])
}

fn create_large_test_row() -> Row {
    Row::new(vec![
        Value::Integer(i64::MAX),
        Value::Text("a".repeat(1000)), // Large text
        Value::Real(f64::MAX),
        Value::Blob(vec![0u8; 500]), // Large blob
        Value::Boolean(false),
        Value::Timestamp(1640995200), // 2022-01-01 00:00:00 UTC
    ])
}

#[test]
fn test_new_row_creation() {
    let values = vec![Value::Integer(123), Value::Text("test".to_string())];
    let row = Row::new(values.clone());

    assert_eq!(row.row_id, None);
    assert_eq!(row.values, values);
}

#[test]
fn test_row_with_id_creation() {
    let row_id = 42;
    let values = vec![Value::Integer(123)];
    let row = Row::with_row_id(row_id, values.clone());

    assert_eq!(row.row_id, Some(row_id));
    assert_eq!(row.values, values);
}

#[test]
fn test_empty_row() {
    let row = Row::new(vec![]);
    assert_eq!(row.values.len(), 0);
    assert_eq!(row.row_id, None);
}

#[test]
fn test_get_value_valid_index() {
    let row = create_test_row();

    assert_eq!(row.get_value(0), Some(&Value::Integer(42)));
    assert_eq!(row.get_value(1), Some(&Value::Text("hello".to_string())));
    assert_eq!(row.get_value(2), Some(&Value::Real(3.14)));
    assert_eq!(row.get_value(3), Some(&Value::Boolean(true)));
    assert_eq!(row.get_value(4), Some(&Value::Null));
}

#[test]
fn test_get_value_invalid_index() {
    let row = create_test_row();
    assert_eq!(row.get_value(10), None);
    assert_eq!(row.get_value(5), None);
}

#[test]
fn test_set_value_valid_index() {
    let mut row = create_test_row();
    let new_value = Value::Text("updated".to_string());

    let result = row.set_value(1, new_value.clone());
    assert!(result.is_ok());
    assert_eq!(row.get_value(1), Some(&new_value));
}

#[test]
fn test_set_value_invalid_index() {
    let mut row = create_test_row();
    let new_value = Value::Integer(999);

    let result = row.set_value(10, new_value);
    assert!(result.is_err());

    match result {
        Err(DatabaseError::ColumnIndexOutOfBounds { index }) => {
            assert_eq!(index, 10);
        }
        _ => panic!("Expected ColumnIndexOutOfBounds error"),
    }
}

#[test]
fn test_set_value_boundary_conditions() {
    let mut row = create_test_row();
    let values_len = row.values.len();

    // Test setting at last valid index
    let result = row.set_value(values_len - 1, Value::Integer(999));
    assert!(result.is_ok());

    // Test setting at first invalid index
    let result = row.set_value(values_len, Value::Integer(999));
    assert!(result.is_err());
}

#[test]
fn test_row_size_calculation() {
    let row = create_test_row();
    let calculated_size = row.size();

    // Verify size is reasonable (should be > 0 and < some upper bound)
    assert!(calculated_size > 0);
    assert!(calculated_size < 10000); // Reasonable upper bound for test data
}

#[test]
fn test_row_size_with_row_id() {
    let values = vec![Value::Integer(42)];
    let row_without_id = Row::new(values.clone());
    let row_with_id = Row::with_row_id(1, values);

    // Row with ID should be larger by 8 bytes (size of RowId)
    assert!(row_with_id.size() > row_without_id.size());
}

#[test]
fn test_row_size_consistency() {
    let row = create_large_test_row();
    let size1 = row.size();
    let size2 = row.size();

    // Size calculation should be consistent
    assert_eq!(size1, size2);
}

#[test]
fn test_serialization_deserialization_round_trip() {
    let original_row = create_test_row();

    // Serialize
    let bytes = original_row.to_bytes();
    assert!(!bytes.is_empty());

    // Deserialize
    let deserialized_row = Row::from_bytes(&bytes).expect("Deserialization failed");

    // Verify equality
    assert_eq!(original_row, deserialized_row);
}

#[test]
fn test_serialization_with_row_id() {
    let original_row = Row::with_row_id(
        42,
        vec![Value::Integer(123), Value::Text("test".to_string())],
    );

    let bytes = original_row.to_bytes();
    let deserialized_row = Row::from_bytes(&bytes).expect("Deserialization failed");

    assert_eq!(original_row, deserialized_row);
    assert_eq!(deserialized_row.row_id, Some(42));
}

#[test]
fn test_serialization_empty_row() {
    let empty_row = Row::new(vec![]);

    let bytes = empty_row.to_bytes();
    let deserialized_row = Row::from_bytes(&bytes).expect("Deserialization failed");

    assert_eq!(empty_row, deserialized_row);
}

#[test]
fn test_serialization_large_data() {
    let large_row = create_large_test_row();

    let bytes = large_row.to_bytes();
    let deserialized_row = Row::from_bytes(&bytes).expect("Deserialization failed");

    assert_eq!(large_row, deserialized_row);
}

#[test]
fn test_deserialization_empty_bytes() {
    let result = Row::from_bytes(&[]);
    assert!(result.is_err());
}

#[test]
fn test_row_clone() {
    let original = create_test_row();
    let cloned = original.clone();

    assert_eq!(original, cloned);
    assert_ne!(original.values.as_ptr(), cloned.values.as_ptr()); // Different memory locations
}

#[test]
fn test_row_debug_formatting() {
    let row = create_test_row();
    let debug_str = format!("{:?}", row);

    assert!(debug_str.contains("Row"));
    assert!(debug_str.contains("row_id"));
    assert!(debug_str.contains("values"));
}

#[test]
fn test_all_value_types() {
    let row = Row::new(vec![
        Value::Null,
        Value::Integer(42),
        Value::Real(3.14159),
        Value::Text("Hello, 世界!".to_string()), // Unicode text
        Value::Blob(vec![0x00, 0xFF, 0xAA, 0x55]),
        Value::Boolean(true),
        Value::Boolean(false),
        Value::Timestamp(1640995200),
    ]);

    // Test serialization/deserialization with all types
    let bytes = row.to_bytes();
    let deserialized = Row::from_bytes(&bytes).expect("Failed to deserialize all types");
    assert_eq!(row, deserialized);

    // Test individual value access
    assert_eq!(row.get_value(0), Some(&Value::Null));
    assert_eq!(row.get_value(1), Some(&Value::Integer(42)));
    assert_eq!(row.get_value(2), Some(&Value::Real(3.14159)));
    assert_eq!(
        row.get_value(3),
        Some(&Value::Text("Hello, 世界!".to_string()))
    );
    assert_eq!(
        row.get_value(4),
        Some(&Value::Blob(vec![0x00, 0xFF, 0xAA, 0x55]))
    );
    assert_eq!(row.get_value(5), Some(&Value::Boolean(true)));
    assert_eq!(row.get_value(6), Some(&Value::Boolean(false)));
    assert_eq!(row.get_value(7), Some(&Value::Timestamp(1640995200)));
}

#[test]
fn test_row_mutation_safety() {
    let mut row = create_test_row();
    let original_size = row.values.len();

    // Modify a value
    row.set_value(0, Value::Integer(999))
        .expect("Failed to set value");

    // Ensure row structure integrity
    assert_eq!(row.values.len(), original_size);
    assert_eq!(row.get_value(0), Some(&Value::Integer(999)));

    // Other values should remain unchanged
    assert_eq!(row.get_value(1), Some(&Value::Text("hello".to_string())));
}

#[test]
fn test_concurrent_access_patterns() {
    use std::sync::{Arc, Mutex};
    use std::thread;

    let row = Arc::new(Mutex::new(create_test_row()));
    let mut handles = vec![];

    // Spawn multiple threads to read the row
    for i in 0..10 {
        let row_clone = Arc::clone(&row);
        let handle = thread::spawn(move || {
            let row_guard = row_clone.lock().unwrap();
            let value = row_guard.get_value(0).cloned();
            assert_eq!(value, Some(Value::Integer(42)));
            i // Just to use the variable
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

#[test]
fn test_memory_usage_patterns() {
    // Test that creating many rows doesn't cause obvious memory issues
    let mut rows = Vec::new();

    for i in 0..1000 {
        let row = Row::new(vec![
            Value::Integer(i as i64),
            Value::Text(format!("row_{}", i)),
        ]);
        rows.push(row);
    }

    // Verify all rows are created correctly
    assert_eq!(rows.len(), 1000);
    assert_eq!(rows[999].get_value(0), Some(&Value::Integer(999)));
    assert_eq!(
        rows[999].get_value(1),
        Some(&Value::Text("row_999".to_string()))
    );
}

#[test]
fn test_edge_case_values() {
    let edge_case_row = Row::new(vec![
        Value::Integer(i64::MIN),
        Value::Integer(i64::MAX),
        Value::Real(f64::MIN),
        Value::Real(f64::MAX),
        Value::Real(f64::INFINITY),
        Value::Real(f64::NEG_INFINITY),
        Value::Real(f64::NAN),
        Value::Text(String::new()),        // Empty string
        Value::Text("🦀🚀💾".to_string()), // Emoji
        Value::Blob(vec![]),               // Empty blob
    ]);

    // Test serialization with edge cases
    let bytes = edge_case_row.to_bytes();
    let deserialized = Row::from_bytes(&bytes).expect("Failed to deserialize edge cases");

    // Note: NaN comparison is special
    assert_eq!(deserialized.get_value(0), Some(&Value::Integer(i64::MIN)));
    assert_eq!(deserialized.get_value(1), Some(&Value::Integer(i64::MAX)));
    assert_eq!(deserialized.get_value(2), Some(&Value::Real(f64::MIN)));
    assert_eq!(deserialized.get_value(3), Some(&Value::Real(f64::MAX)));
    assert_eq!(deserialized.get_value(4), Some(&Value::Real(f64::INFINITY)));
    assert_eq!(
        deserialized.get_value(5),
        Some(&Value::Real(f64::NEG_INFINITY))
    );

    // Special handling for NaN
    if let Some(Value::Real(val)) = deserialized.get_value(6) {
        assert!(val.is_nan());
    } else {
        panic!("Expected NaN value");
    }

    assert_eq!(deserialized.get_value(7), Some(&Value::Text(String::new())));
    assert_eq!(
        deserialized.get_value(8),
        Some(&Value::Text("🦀🚀💾".to_string()))
    );
    assert_eq!(deserialized.get_value(9), Some(&Value::Blob(vec![])));
}

#[test]
fn test_row_partial_eq_implementation() {
    let row1 = create_test_row();
    let row2 = create_test_row();
    let mut row3 = create_test_row();
    row3.set_value(0, Value::Integer(999)).unwrap();

    assert_eq!(row1, row2);
    assert_ne!(row1, row3);

    // Test with different row IDs
    let row4 = Row::with_row_id(1, row1.values.clone());
    let row5 = Row::with_row_id(2, row1.values.clone());
    assert_ne!(row4, row5);
}

// Integration-style tests that simulate real database operations

#[test]
fn test_row_update_simulation() {
    // Simulate a database UPDATE operation
    let mut row = Row::with_row_id(
        1,
        vec![
            Value::Integer(100),
            Value::Text("John".to_string()),
            Value::Integer(25),
        ],
    );

    // Update age from 25 to 26
    row.set_value(2, Value::Integer(26))
        .expect("Failed to update age");

    // Serialize (as if storing to disk)
    let serialized = row.to_bytes();

    // Deserialize (as if loading from disk)
    let loaded_row = Row::from_bytes(&serialized).expect("Failed to load row");

    assert_eq!(loaded_row.row_id, Some(1));
    assert_eq!(loaded_row.get_value(2), Some(&Value::Integer(26)));
}

#[test]
fn test_row_batch_operations() {
    // Simulate batch insert operation
    let mut rows = Vec::new();

    for i in 0..100 {
        let row = Row::with_row_id(
            i,
            vec![
                Value::Integer(i as i64),
                Value::Text(format!("user_{}", i)),
                Value::Boolean(i % 2 == 0),
            ],
        );
        rows.push(row);
    }

    // Verify batch integrity
    assert_eq!(rows.len(), 100);

    // Test random access
    assert_eq!(rows[50].row_id, Some(50));
    assert_eq!(
        rows[50].get_value(1),
        Some(&Value::Text("user_50".to_string()))
    );
    assert_eq!(rows[50].get_value(2), Some(&Value::Boolean(true)));
}

#[test]
fn test_row_type_consistency() {
    // Test that we can store different types in same column across rows
    // (demonstrating schemaless flexibility)
    let mut rows = vec![
        Row::new(vec![Value::Integer(42)]),
        Row::new(vec![Value::Text("hello".to_string())]),
        Row::new(vec![Value::Boolean(true)]),
        Row::new(vec![Value::Null]),
    ];

    // All should serialize/deserialize correctly
    for row in &mut rows {
        let bytes = row.to_bytes();
        let deserialized = Row::from_bytes(&bytes).expect("Failed to deserialize");
        assert_eq!(*row, deserialized);
    }
}

// Performance characteristic tests

#[test]
fn test_serialization_performance_characteristics() {
    use std::time::Instant;

    let large_row = Row::new(vec![
        Value::Text("x".repeat(10000)),
        Value::Blob(vec![0u8; 10000]),
        Value::Integer(i64::MAX),
    ]);

    let start = Instant::now();
    let bytes = large_row.to_bytes();
    let serialize_time = start.elapsed();

    let start = Instant::now();
    let _deserialized = Row::from_bytes(&bytes).expect("Deserialization failed");
    let deserialize_time = start.elapsed();

    // These are just sanity checks - actual performance requirements depend on use case
    assert!(
        serialize_time.as_millis() < 100,
        "Serialization too slow: {:?}",
        serialize_time
    );
    assert!(
        deserialize_time.as_millis() < 100,
        "Deserialization too slow: {:?}",
        deserialize_time
    );
    assert!(!bytes.is_empty());
}
