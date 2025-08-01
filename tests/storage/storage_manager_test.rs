use std::fs;

use bambang::{
    executor::predicate::Predicate,
    storage::{schema::ColumnSchema, storage_manager::StorageManager},
    types::{row::Row, value::{DataType, Value}},
    utils::mock::{TempDatabase, create_temp_db_path_with_prefix},
};

fn create_user_row(id: i64, name: &str, email: &str) -> Row {
    Row::new(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
        Value::Text(email.to_string()),
    ])
}

fn create_product_row(id: i64, name: &str, price: f64) -> Row {
    Row::new(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
        Value::Real(price),
    ])
}

#[test]
fn test_storage_manager_creation_new_database() {
    let mut temp_db = TempDatabase::with_prefix("new_db_test");
    let storage_manager = temp_db.create_storage_manager().unwrap();
    assert_eq!(storage_manager.db_info.page_count, 1);
    assert!(storage_manager.db_info.file_size > 0);
    assert!(storage_manager.table_roots.contains_key("sqlite_schema"));
    assert_eq!(storage_manager.table_roots["sqlite_schema"], 1);
    assert!(temp_db.path.exists());
}

#[test]
fn test_storage_manager_open_existing_database() {
    let temp_path = create_temp_db_path_with_prefix("existing_db_test");
    {
        let _ = StorageManager::new(&temp_path).unwrap();
    }
    let storage_manager = StorageManager::new(&temp_path).unwrap();
    assert!(storage_manager.table_roots.contains_key("sqlite_schema"));
    assert_eq!(storage_manager.table_roots["sqlite_schema"], 1);
    drop(storage_manager);
    let _ = fs::remove_file(&temp_path);
}

#[test]
fn test_multiple_tables() {
    let mut temp_db = TempDatabase::with_prefix("multi_table_test");
    let storage_manager = temp_db.create_storage_manager().unwrap();
    let users_root = storage_manager
        .create_table(
            "users",
            "CREATE TABLE users(id INTEGER, name TEXT, email TEXT)",
        )
        .unwrap();
    let products_root = storage_manager
        .create_table(
            "products",
            "CREATE TABLE products(id INTEGER, name TEXT, price REAL)",
        )
        .unwrap();
    assert!(storage_manager.table_roots.contains_key("users"));
    assert!(storage_manager.table_roots.contains_key("products"));
    assert_ne!(users_root, products_root);
    let user = create_user_row(1, "Alice", "alice@example.com");
    let product = create_product_row(1, "Laptop", 999.99);
    storage_manager.insert_into_table("users", user).unwrap();
    storage_manager
        .insert_into_table("products", product)
        .unwrap();
}

#[test]
fn test_database_persistence() {
    let temp_path = create_temp_db_path_with_prefix("persistence_test");
    {
        let mut storage_manager = StorageManager::new(&temp_path).unwrap();
        storage_manager
            .create_table(
                "test_table",
                "CREATE TABLE test_table(id INTEGER, data TEXT)",
            )
            .unwrap();
        let test_row = Row::new(vec![
            Value::Integer(42),
            Value::Text("test data".to_string()),
        ]);
        storage_manager
            .insert_into_table("test_table", test_row)
            .unwrap();
    }
    {
        let storage_manager = StorageManager::new(&temp_path).unwrap();
        assert!(storage_manager.table_roots.contains_key("test_table"));
        assert!(storage_manager.table_roots.contains_key("sqlite_schema"));
    }
    let _ = fs::remove_file(&temp_path);
}

#[test]
fn test_error_handling_nonexistent_table() {
    let mut temp_db = TempDatabase::with_prefix("error_test");
    let storage_manager = temp_db.create_storage_manager().unwrap();
    let test_row = create_user_row(1, "Test", "test@example.com");
    let result = storage_manager.insert_into_table("nonexistent_table", test_row);
    assert!(result.is_err());
}

fn setup_test_table_with_schema(temp_db: &mut TempDatabase) -> &StorageManager {
    let storage_manager = temp_db.create_storage_manager().unwrap();
    
    let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, active BOOLEAN)";
    storage_manager.create_table("users", sql).unwrap();

    let test_rows = vec![
        Row::new(vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Integer(25),
            Value::Boolean(true),
        ]),
        Row::new(vec![
            Value::Integer(2),
            Value::Text("Bob".to_string()),
            Value::Integer(30),
            Value::Boolean(false),
        ]),
        Row::new(vec![
            Value::Integer(3),
            Value::Text("Charlie".to_string()),
            Value::Integer(35),
            Value::Boolean(true),
        ]),
        Row::new(vec![
            Value::Integer(4),
            Value::Text("Diana".to_string()),
            Value::Integer(28),
            Value::Boolean(true),
        ]),
    ];

    for row in test_rows {
        storage_manager.insert_into_table("users", row).unwrap();
    }

    storage_manager
}

#[test]
fn test_scan_table_without_predicate() {
    let mut temp_db = TempDatabase::with_prefix("scan_no_predicate_test");
    let storage_manager = setup_test_table_with_schema(&mut temp_db);
    
    let all_rows = storage_manager.scan_table("users", None).unwrap();
    assert_eq!(all_rows.len(), 4);
}

#[test]
fn test_scan_table_with_predicate_functionality() {
    let mut temp_db = TempDatabase::with_prefix("scan_predicate_test");
    let storage_manager = temp_db.create_storage_manager().unwrap();
    
    storage_manager.create_table("test_table", "CREATE TABLE test_table(id INTEGER, name TEXT, value INTEGER)").unwrap();
    
    let test_rows = vec![
        Row::new(vec![Value::Integer(1), Value::Text("Alice".to_string()), Value::Integer(100)]),
        Row::new(vec![Value::Integer(2), Value::Text("Bob".to_string()), Value::Integer(200)]),
        Row::new(vec![Value::Integer(3), Value::Text("Charlie".to_string()), Value::Integer(300)]),
    ];
    
    for row in test_rows {
        storage_manager.insert_into_table("test_table", row).unwrap();
    }
    
    let all_rows = storage_manager.scan_table("test_table", None).unwrap();
    assert_eq!(all_rows.len(), 3);
    
    let predicate = Predicate::eq("name".to_string(), Value::Text("Alice".to_string()));
    let result = storage_manager.scan_table("test_table", Some(predicate));
    
    assert!(result.is_ok());
 
}

#[test]
fn test_multiple_inserts() {
    let mut temp_db = TempDatabase::with_prefix("multi_insert_test");
    let db_path = temp_db.path.clone();
    let storage_manager = temp_db.create_storage_manager().unwrap();
    let users_root = storage_manager
        .create_table(
            "users",
            "CREATE TABLE users(id INTEGER, name TEXT, email TEXT)",
        )
        .unwrap();
    assert!(storage_manager.table_roots.contains_key("users"));
    assert_eq!(storage_manager.table_roots["users"], users_root);
    let insert_count = 25;
    for i in 1..=insert_count {
        let user = create_user_row(i, &format!("User{}", i), &format!("user{}@example.com", i));
        storage_manager.insert_into_table("users", user).unwrap();
        assert!(storage_manager.table_roots.contains_key("users"));
    }
    drop(storage_manager);
    let reopened_storage = StorageManager::new(&db_path).unwrap();
    assert!(reopened_storage.table_roots.contains_key("users"));
    assert!(reopened_storage.table_roots.contains_key("sqlite_schema"));
    drop(reopened_storage);
    drop(temp_db);
}