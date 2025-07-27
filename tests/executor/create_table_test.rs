use std::fs;
use tempfile::tempdir;

use bambang::{
    executor::create_table::{CreateTableExecutor, TableCreator, TableSchemaBuilder},
    storage::{
        storage_manager::StorageManager,
        schema::{ColumnSchema, TableSchema},
    },
    types::{
        error::DatabaseError,
        value::{DataType, Value},
        row::Row,
    },
};

fn setup_test_db() -> (StorageManager, tempfile::TempDir) {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test.db");
    let storage_manager = StorageManager::new(&db_path).expect("Failed to create storage manager");
    (storage_manager, temp_dir)
}

#[test]
fn test_create_simple_table() {
    let (mut storage_manager, _temp_dir) = setup_test_db();
    
    // Create a simple table with basic columns
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, 0).primary_key(),
        ColumnSchema::new("name".to_string(), DataType::Text, 1).not_null(),
        ColumnSchema::new("age".to_string(), DataType::Integer, 2),
    ];
    
    let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)".to_string();
    
    let result = storage_manager.create_table_with_schema(
        "users".to_string(),
        columns,
        sql,
    );
    
    assert!(result.is_ok());
    let root_page_id = result.unwrap();
    assert!(root_page_id > 1); // Should be greater than schema page
    
    // Verify table exists
    assert!(storage_manager.table_exists("users"));
    
    // Verify schema is stored correctly
    let schema = storage_manager.get_table_schema("users").unwrap();
    assert_eq!(schema.table_name, "users");
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.root_page_id, root_page_id);
    
    // Check column details
    let id_col = schema.get_column("id").unwrap();
    assert_eq!(id_col.data_type, DataType::Integer);
    assert_eq!(id_col.position, 0);
    assert!(id_col.primary_key);
    assert!(!id_col.nullable);
    
    let name_col = schema.get_column("name").unwrap();
    assert_eq!(name_col.data_type, DataType::Text);
    assert_eq!(name_col.position, 1);
    assert!(!name_col.primary_key);
    assert!(!name_col.nullable);
    
    let age_col = schema.get_column("age").unwrap();
    assert_eq!(age_col.data_type, DataType::Integer);
    assert_eq!(age_col.position, 2);
    assert!(!age_col.primary_key);
    assert!(age_col.nullable);
}

#[test]
fn test_create_table_with_defaults() {
    let (mut storage_manager, _temp_dir) = setup_test_db();
    
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, 0).primary_key(),
        ColumnSchema::new("status".to_string(), DataType::Text, 1)
            .with_default(Value::Text("active".to_string())),
        ColumnSchema::new("created_at".to_string(), DataType::Timestamp, 2)
            .not_null()
            .with_default(Value::now()),
    ];
    
    let sql = "CREATE TABLE records (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)".to_string();
    
    let result = storage_manager.create_table_with_schema(
        "records".to_string(),
        columns,
        sql,
    );
    
    assert!(result.is_ok());
    
    let schema = storage_manager.get_table_schema("records").unwrap();
    let status_col = schema.get_column("status").unwrap();
    assert!(status_col.default_value.is_some());
    assert_eq!(status_col.default_value.as_ref().unwrap(), &Value::Text("active".to_string()));
    
    let created_at_col = schema.get_column("created_at").unwrap();
    assert!(created_at_col.default_value.is_some());
    assert!(!created_at_col.nullable);
}

#[test]
fn test_create_table_with_unique_constraints() {
    let (mut storage_manager, _temp_dir) = setup_test_db();
    
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, 0).primary_key(),
        ColumnSchema::new("email".to_string(), DataType::Text, 1).not_null().unique(),
        ColumnSchema::new("username".to_string(), DataType::Text, 2).unique(),
    ];
    
    let sql = "CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE, username TEXT UNIQUE)".to_string();
    
    let result = storage_manager.create_table_with_schema(
        "accounts".to_string(),
        columns,
        sql,
    );
    
    assert!(result.is_ok());
    
    let schema = storage_manager.get_table_schema("accounts").unwrap();
    let email_col = schema.get_column("email").unwrap();
    assert!(email_col.unique);
    assert!(!email_col.nullable);
    
    let username_col = schema.get_column("username").unwrap();
    assert!(username_col.unique);
    assert!(username_col.nullable);
}

#[test]
fn test_table_schema_builder() {
    let (mut storage_manager, _temp_dir) = setup_test_db();
    
    let builder = TableSchemaBuilder::new("products".to_string())
        .add_column("id".to_string(), DataType::Integer)
        .add_column_with_constraints(
            "name".to_string(),
            DataType::Text,
            false, // not nullable
            None,  // no default
            false, // not primary key
            true,  // unique
        )
        .add_column("price".to_string(), DataType::Real)
        .add_column("in_stock".to_string(), DataType::Boolean)
        .with_sql("CREATE TABLE products (id INTEGER, name TEXT NOT NULL UNIQUE, price REAL, in_stock BOOLEAN)".to_string());
    
    let result = storage_manager.create_table_with_builder(builder);
    assert!(result.is_ok());
    
    let schema = storage_manager.get_table_schema("products").unwrap();
    assert_eq!(schema.columns.len(), 4);
    
    let name_col = schema.get_column("name").unwrap();
    assert!(!name_col.nullable);
    assert!(name_col.unique);
    assert_eq!(name_col.position, 1);
}

#[test]
fn test_create_table_duplicate_name() {
    let (mut storage_manager, _temp_dir) = setup_test_db();
    
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, 0),
    ];
    
    let sql = "CREATE TABLE test_table (id INTEGER)".to_string();
    
    // Create first table
    let result1 = storage_manager.create_table_with_schema(
        "test_table".to_string(),
        columns.clone(),
        sql.clone(),
    );
    assert!(result1.is_ok());
    
    // Try to create table with same name
    let result2 = storage_manager.create_table_with_schema(
        "test_table".to_string(),
        columns,
        sql,
    );
    assert!(result2.is_err());
    
    if let Err(DatabaseError::ExecutionError { details }) = result2 {
        assert!(details.contains("already exists"));
    } else {
        panic!("Expected ExecutionError for duplicate table name");
    }
}

#[test]
fn test_create_table_empty_columns() {
    let (mut storage_manager, _temp_dir) = setup_test_db();
    
    let columns = vec![];
    let sql = "CREATE TABLE empty_table ()".to_string();
    
    let result = storage_manager.create_table_with_schema(
        "empty_table".to_string(),
        columns,
        sql,
    );
    
    assert!(result.is_err());
    if let Err(DatabaseError::InvalidData { details }) = result {
        assert!(details.contains("at least one column"));
    } else {
        panic!("Expected InvalidData error for empty columns");
    }
}

#[test]
fn test_create_table_duplicate_column_names() {
    let (mut storage_manager, _temp_dir) = setup_test_db();
    
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, 0),
        ColumnSchema::new("id".to_string(), DataType::Text, 1), // Duplicate name
    ];
    
    let sql = "CREATE TABLE bad_table (id INTEGER, id TEXT)".to_string();
    
    let result = storage_manager.create_table_with_schema(
        "bad_table".to_string(),
        columns,
        sql,
    );
    
    assert!(result.is_err());
    if let Err(DatabaseError::InvalidData { details }) = result {
        assert!(details.contains("Duplicate column name"));
    } else {
        panic!("Expected InvalidData error for duplicate column names");
    }
}

#[test]
fn test_create_table_invalid_positions() {
    let (mut storage_manager, _temp_dir) = setup_test_db();
    
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, 0),
        ColumnSchema::new("name".to_string(), DataType::Text, 2), // Gap in positions
    ];
    
    let sql = "CREATE TABLE bad_positions (id INTEGER, name TEXT)".to_string();
    
    let result = storage_manager.create_table_with_schema(
        "bad_positions".to_string(),
        columns,
        sql,
    );
    
    assert!(result.is_err());
    if let Err(DatabaseError::InvalidData { details }) = result {
        assert!(details.contains("sequential"));
    } else {
        panic!("Expected InvalidData error for invalid positions");
    }
}

#[test]
fn test_create_table_multiple_primary_keys() {
    let (mut storage_manager, _temp_dir) = setup_test_db();
    
    let columns = vec![
        ColumnSchema::new("id1".to_string(), DataType::Integer, 0).primary_key(),
        ColumnSchema::new("id2".to_string(), DataType::Integer, 1).primary_key(), // Multiple PKs
    ];
    
    let sql = "CREATE TABLE bad_pk (id1 INTEGER PRIMARY KEY, id2 INTEGER PRIMARY KEY)".to_string();
    
    let result = storage_manager.create_table_with_schema(
        "bad_pk".to_string(),
        columns,
        sql,
    );
    
    assert!(result.is_err());
    if let Err(DatabaseError::InvalidData { details }) = result {
        assert!(details.contains("at most one primary key"));
    } else {
        panic!("Expected InvalidData error for multiple primary keys");
    }
}

#[test]
fn test_schema_persistence_across_reopens() {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("persistent_test.db");
    
    // Create table in first session
    {
        let mut storage_manager = StorageManager::new(&db_path).expect("Failed to create storage manager");
        
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, 0).primary_key(),
            ColumnSchema::new("data".to_string(), DataType::Text, 1),
        ];
        
        let sql = "CREATE TABLE persistent_table (id INTEGER PRIMARY KEY, data TEXT)".to_string();
        
        let result = storage_manager.create_table_with_schema(
            "persistent_table".to_string(),
            columns,
            sql,
        );
        assert!(result.is_ok());
    }
    
    // Reopen database and verify table exists
    {
        let storage_manager = StorageManager::new(&db_path).expect("Failed to reopen storage manager");
        
        assert!(storage_manager.table_exists("persistent_table"));
        
        let schema = storage_manager.get_table_schema("persistent_table").unwrap();
        assert_eq!(schema.table_name, "persistent_table");
        assert_eq!(schema.columns.len(), 2);
        
        let id_col = schema.get_column("id").unwrap();
        assert!(id_col.primary_key);
        assert_eq!(id_col.data_type, DataType::Integer);
    }
}

#[test]
fn test_column_schema_serialization() {
    let column = ColumnSchema::new("test_col".to_string(), DataType::Text, 0)
        .not_null()
        .with_default(Value::Text("default_value".to_string()))
        .unique();
    
    let row = column.to_schema_row("test_table");
    assert_eq!(row.values.len(), 9);
    
    // Test round-trip serialization
    let deserialized = ColumnSchema::from_schema_row(&row).unwrap();
    assert_eq!(deserialized.name, column.name);
    assert_eq!(deserialized.data_type, column.data_type);
    assert_eq!(deserialized.position, column.position);
    assert_eq!(deserialized.nullable, column.nullable);
    assert_eq!(deserialized.default_value, column.default_value);
    assert_eq!(deserialized.unique, column.unique);
}

#[test]
fn test_table_schema_validation() {
    let (mut storage_manager, _temp_dir) = setup_test_db();
    
    // Create table with schema
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, 0).primary_key(),
        ColumnSchema::new("name".to_string(), DataType::Text, 1).not_null(),
        ColumnSchema::new("age".to_string(), DataType::Integer, 2),
    ];
    
    let sql = "CREATE TABLE validation_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)".to_string();
    
    storage_manager.create_table_with_schema(
        "validation_test".to_string(),
        columns,
        sql,
    ).unwrap();
    
    // Test valid row
    let valid_row = Row::new(vec![
        Value::Integer(1),
        Value::Text("John".to_string()),
        Value::Integer(25),
    ]);
    
    let result = storage_manager.validate_row("validation_test", &valid_row);
    assert!(result.is_ok());
    
    // Test invalid row (NULL in NOT NULL column)
    let invalid_row = Row::new(vec![
        Value::Integer(1),
        Value::Null, // name is NOT NULL
        Value::Integer(25),
    ]);
    
    let result = storage_manager.validate_row("validation_test", &invalid_row);
    assert!(result.is_err());
    
    // Test wrong column count
    let wrong_count_row = Row::new(vec![
        Value::Integer(1),
        Value::Text("John".to_string()),
        // Missing age column
    ]);
    
    let result = storage_manager.validate_row("validation_test", &wrong_count_row);
    assert!(result.is_err());
}