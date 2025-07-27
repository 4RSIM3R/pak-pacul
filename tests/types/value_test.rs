use bambang::types::value::{DataType, Value};

#[test]
fn test_value_creation_and_data_types() {
    let null_val = Value::Null;
    let int_val = Value::Integer(42);
    let real_val = Value::Real(3.14);
    let text_val = Value::Text("hello".to_string());
    let blob_val = Value::Blob(vec![1, 2, 3, 4]);
    let bool_val = Value::Boolean(true);
    let ts_val = Value::Timestamp(1640995200); // 2022-01-01 00:00:00 UTC

    assert_eq!(null_val.data_type(), DataType::Null);
    assert_eq!(int_val.data_type(), DataType::Integer);
    assert_eq!(real_val.data_type(), DataType::Real);
    assert_eq!(text_val.data_type(), DataType::Text);
    assert_eq!(blob_val.data_type(), DataType::Blob);
    assert_eq!(bool_val.data_type(), DataType::Boolean);
    assert_eq!(ts_val.data_type(), DataType::Timestamp);
}

#[test]
fn test_value_comparison_for_indexing() {
    // Integer comparisons
    assert!(Value::Integer(5) < Value::Integer(10));
    assert!(Value::Integer(10) > Value::Integer(5));
    assert!(Value::Integer(5) == Value::Integer(5));

    // Mixed numeric comparisons (important for queries)
    assert!(Value::Integer(5) < Value::Real(5.5));
    assert!(Value::Real(3.14) < Value::Integer(4));

    // Text comparisons (lexicographic)
    assert!(Value::Text("apple".to_string()) < Value::Text("banana".to_string()));

    // Null handling (nulls are always smallest)
    assert!(Value::Null < Value::Integer(0));
    assert!(Value::Null < Value::Text("".to_string()));
    assert!(Value::Null == Value::Null);

    // Timestamp comparisons
    let ts1 = Value::Timestamp(1640995200); // 2022-01-01
    let ts2 = Value::Timestamp(1672531200); // 2023-01-01
    assert!(ts1 < ts2);
}

#[test]
fn test_value_sizes_for_storage() {
    assert_eq!(Value::Null.size(), 0);
    assert_eq!(Value::Integer(123).size(), 8);
    assert_eq!(Value::Real(3.14).size(), 8);
    assert_eq!(Value::Text("hello".to_string()).size(), 5);
    assert_eq!(Value::Blob(vec![1, 2, 3]).size(), 3);
    assert_eq!(Value::Boolean(true).size(), 1);
    assert_eq!(Value::Timestamp(1640995200).size(), 8);

    // Test with larger text and blob values
    let large_text = Value::Text("a".repeat(1000));
    assert_eq!(large_text.size(), 1000);

    let large_blob = Value::Blob(vec![0; 2048]);
    assert_eq!(large_blob.size(), 2048);
}

#[test]
fn test_memory_usage_patterns() {
    // Test that cloning doesn't cause excessive memory usage
    let large_blob = Value::Blob(vec![0; 10000]);
    let cloned = large_blob.clone();

    assert_eq!(large_blob.size(), cloned.size());
    assert_eq!(large_blob, cloned);

    // Test that we can create many small values efficiently
    let small_values: Vec<Value> = (0..1000).map(|i| Value::Integer(i)).collect();

    assert_eq!(small_values.len(), 1000);
    assert_eq!(small_values[999], Value::Integer(999));

    // Test memory efficiency of text values
    let repeated_text = Value::Text("test".repeat(1000));
    assert_eq!(repeated_text.size(), 4000);
}

#[test]
fn test_query_scenarios() {
    // Simulate a WHERE clause evaluation
    let age = Value::Integer(25);
    let min_age = Value::Integer(18);
    let max_age = Value::Integer(65);

    assert!(age >= min_age && age <= max_age);

    // Simulate an ORDER BY operation
    let mut salaries = vec![
        Value::Real(50000.0),
        Value::Real(75000.0),
        Value::Real(60000.0),
        Value::Real(45000.0),
    ];

    salaries.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    assert_eq!(salaries[0], Value::Real(45000.0));
    assert_eq!(salaries[3], Value::Real(75000.0));

    // Simulate a GROUP BY with aggregation
    let categories = vec![
        ("A", Value::Integer(100)),
        ("B", Value::Integer(200)),
        ("A", Value::Integer(150)),
        ("B", Value::Integer(250)),
    ];

    let mut group_a_sum = 0.0;
    let mut group_b_sum = 0.0;

    for (category, value) in categories {
        if let Some(num) = value.coerce_to_number() {
            match category {
                "A" => group_a_sum += num,
                "B" => group_b_sum += num,
                _ => {}
            }
        }
    }

    assert_eq!(group_a_sum, 250.0);
    assert_eq!(group_b_sum, 450.0);
}

#[test]
fn test_cross_type_comparisons() {
    // Test numeric promotions
    assert!(Value::Integer(5) < Value::Real(5.5));
    assert!(Value::Real(5.0) == Value::Integer(5));

    // Test incomparable types
    let text_val = Value::Text("hello".to_string());
    let blob_val = Value::Blob(vec![1, 2, 3]);
    assert!(text_val.partial_cmp(&blob_val).is_none());

    // Test coercion-based comparisons
    let numeric_text = Value::Text("42".to_string());
    let integer = Value::Integer(41);
    assert!(numeric_text > integer);

    // Test boolean comparisons
    assert!(Value::Boolean(true) > Value::Boolean(false));
    assert!(
        Value::Boolean(true)
            .partial_cmp(&Value::Integer(1))
            .is_some()
    );
}

#[test]
fn test_edge_cases() {
    // Test extremely large values
    let max_int = Value::Integer(i64::MAX);
    let min_int = Value::Integer(i64::MIN);
    assert!(max_int > min_int);

    // Test very large text
    let large_text = Value::Text("x".repeat(1_000_000));
    assert_eq!(large_text.size(), 1_000_000);

    // Test empty containers
    let empty_text = Value::Text(String::new());
    let empty_blob = Value::Blob(Vec::new());
    assert_eq!(empty_text.size(), 0);
    assert_eq!(empty_blob.size(), 0);

    // Test floating point edge cases
    let inf = Value::Real(f64::INFINITY);
    let neg_inf = Value::Real(f64::NEG_INFINITY);
    let nan = Value::Real(f64::NAN);

    assert!(inf > Value::Real(1000.0));
    assert!(neg_inf < Value::Real(-1000.0));
    assert!(nan.partial_cmp(&Value::Real(0.0)).is_none());
}

#[test]
fn test_display_formatting() {
    assert_eq!(format!("{}", Value::Null), "NULL");
    assert_eq!(format!("{}", Value::Integer(42)), "42");
    assert_eq!(format!("{}", Value::Real(3.14)), "3.14");
    assert_eq!(format!("{}", Value::Text("hello".to_string())), "hello");
    assert_eq!(format!("{}", Value::Blob(vec![1, 2, 3])), "BLOB(3 bytes)");
    assert_eq!(format!("{}", Value::Boolean(true)), "TRUE");
    assert_eq!(format!("{}", Value::Boolean(false)), "FALSE");

    let ts = Value::Timestamp(1640995200);
    let display = format!("{}", ts);
    assert!(display.contains("2022-01-01"));
    assert!(display.contains("00:00:00 UTC"));

    // Test invalid timestamp display
    let invalid_ts = Value::Timestamp(i64::MAX);
    let invalid_display = format!("{}", invalid_ts);
    assert!(invalid_display.contains("INVALID_TIMESTAMP"));
}


#[test]
fn test_timestamp_operations() {
    // Test current timestamp
    let now = Value::now();
    assert!(matches!(now, Value::Timestamp(_)));

    // Test Unix timestamp creation
    let unix_ts = Value::timestamp_from_unix(1640995200);
    assert_eq!(unix_ts, Value::Timestamp(1640995200));

    // Test string parsing - RFC3339
    let rfc3339_result = Value::timestamp_from_str("2022-01-01T00:00:00Z");
    assert!(rfc3339_result.is_ok());

    // Test string parsing - SQL format
    let sql_result = Value::timestamp_from_str("2022-01-01 12:30:45");
    assert!(sql_result.is_ok());

    // Test string parsing - Date only
    let date_result = Value::timestamp_from_str("2022-01-01");
    assert!(date_result.is_ok());

    // Test invalid timestamp
    let invalid_result = Value::timestamp_from_str("invalid-date");
    assert!(invalid_result.is_err());

    // Test datetime conversion
    let ts = Value::Timestamp(1640995200);
    let dt = ts.to_datetime();
    assert!(dt.is_some());

    let formatted = ts.format_timestamp("%Y-%m-%d %H:%M:%S");
    assert_eq!(formatted, Some("2022-01-01 00:00:00".to_string()));
}
