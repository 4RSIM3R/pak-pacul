use std::{
    env::temp_dir,
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use bambang::{
    storage::storage_manager::StorageManager,
    types::{row::Row, value::Value},
};

fn get_unix_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}

fn create_temp_db_path() -> PathBuf {
    let mut temp_path = temp_dir();
    let unique_name = format!("bambang_test_{}.db", get_unix_timestamp_millis());
    temp_path.push(unique_name);
    temp_path
}

fn create_temp_db_path_with_prefix(prefix: &str) -> PathBuf {
    let mut temp_path = temp_dir();
    let unique_name = format!("{}_{}.db", prefix, get_unix_timestamp_millis());
    temp_path.push(unique_name);
    temp_path
}

struct TempDatabase {
    pub path: PathBuf,
    pub storage_manager: Option<StorageManager>,
}

impl TempDatabase {
    fn new() -> Self {
        Self {
            path: create_temp_db_path(),
            storage_manager: None,
        }
    }

    fn with_prefix(prefix: &str) -> Self {
        Self {
            path: create_temp_db_path_with_prefix(prefix),
            storage_manager: None,
        }
    }

    fn create_storage_manager(&mut self) -> Result<&mut StorageManager, Box<dyn std::error::Error>> {
        let sm = StorageManager::new(&self.path)?;
        self.storage_manager = Some(sm);
        Ok(self.storage_manager.as_mut().unwrap())
    }

    fn get_storage_manager(&mut self) -> Option<&mut StorageManager> {
        self.storage_manager.as_mut()
    }
}

impl Drop for TempDatabase {
    fn drop(&mut self) {
        self.storage_manager = None;
        if self.path.exists() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

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
    let storage_manager = temp_db.create_storage_manager().expect("Failed to create new storage manager");
    assert_eq!(storage_manager.db_info.page_count, 1);
    assert!(storage_manager.db_info.file_size > 0);
    assert!(storage_manager.table_roots.contains_key("sqlite_schema"));
    assert_eq!(storage_manager.table_roots["sqlite_schema"], 1);
    assert!(temp_db.path.exists());
    println!("Created test database at: {}", temp_db.path.display());
}

#[test]
fn test_storage_manager_open_existing_database() {
    let temp_path = create_temp_db_path_with_prefix("existing_db_test");
    {
        let _storage_manager = StorageManager::new(&temp_path).expect("Failed to create initial database");
    }
    let storage_manager = StorageManager::new(&temp_path).expect("Failed to open existing database");
    assert!(storage_manager.table_roots.contains_key("sqlite_schema"));
    assert_eq!(storage_manager.table_roots["sqlite_schema"], 1);
    drop(storage_manager);
    let _ = fs::remove_file(&temp_path);
}

// #[test]
// fn test_create_and_use_table() {
//     let mut temp_db = TempDatabase::with_prefix("table_test");
//     let storage_manager = temp_db.create_storage_manager().expect("Failed to create storage manager");
//     let users_root = storage_manager.create_table(
//         "users",
//         "CREATE TABLE users(id INTEGER, name TEXT, email TEXT)"
//     ).expect("Failed to create users table");
//     assert!(storage_manager.table_roots.contains_key("users"));
//     assert_eq!(storage_manager.table_roots["users"], users_root);
//     let user1 = create_user_row(1, "Alice", "alice@example.com");
//     let user2 = create_user_row(2, "Bob", "bob@example.com");
//     storage_manager.insert_into_table("users", user1).expect("Failed to insert user1");
//     storage_manager.insert_into_table("users", user2).expect("Failed to insert user2");
//     println!("Successfully created table and inserted data");
// }

// #[test]
// fn test_multiple_tables() {
//     let mut temp_db = TempDatabase::with_prefix("multi_table_test");
//     let storage_manager = temp_db.create_storage_manager().expect("Failed to create storage manager");
//     let users_root = storage_manager.create_table(
//         "users",
//         "CREATE TABLE users(id INTEGER, name TEXT, email TEXT)"
//     ).expect("Failed to create users table");
//     let products_root = storage_manager.create_table(
//         "products", 
//         "CREATE TABLE products(id INTEGER, name TEXT, price REAL)"
//     ).expect("Failed to create products table");
//     assert!(storage_manager.table_roots.contains_key("users"));
//     assert!(storage_manager.table_roots.contains_key("products"));
//     assert_ne!(users_root, products_root);
//     let user = create_user_row(1, "Alice", "alice@example.com");
//     let product = create_product_row(1, "Laptop", 999.99);
//     storage_manager.insert_into_table("users", user).expect("Failed to insert user");
//     storage_manager.insert_into_table("products", product).expect("Failed to insert product");
//     println!("Successfully created and used multiple tables");
// }

// #[test]
// fn test_database_persistence() {
//     let temp_path = create_temp_db_path_with_prefix("persistence_test");
//     {
//         let mut storage_manager = StorageManager::new(&temp_path).expect("Failed to create storage manager");
//         storage_manager.create_table(
//             "test_table",
//             "CREATE TABLE test_table(id INTEGER, data TEXT)"
//         ).expect("Failed to create test table");
//         let test_row = Row::new(vec![
//             Value::Integer(42),
//             Value::Text("test data".to_string()),
//         ]);
//         storage_manager.insert_into_table("test_table", test_row).expect("Failed to insert test row");
//     }
//     {
//         let storage_manager = StorageManager::new(&temp_path).expect("Failed to reopen database");
//         assert!(storage_manager.table_roots.contains_key("test_table"));
//         assert!(storage_manager.table_roots.contains_key("sqlite_schema"));
//         println!("Database persistence test passed");
//     }
//     let _ = fs::remove_file(&temp_path);
// }

// #[test] 
// fn test_error_handling_nonexistent_table() {
//     let mut temp_db = TempDatabase::with_prefix("error_test");
//     let storage_manager = temp_db.create_storage_manager().expect("Failed to create storage manager");
//     let test_row = create_user_row(1, "Test", "test@example.com");
//     let result = storage_manager.insert_into_table("nonexistent_table", test_row);
//     assert!(result.is_err());
//     println!("Error handling test passed: {:?}", result.err());
// }
