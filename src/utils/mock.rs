use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use tempfile::env::temp_dir;

use crate::storage::storage_manager::StorageManager;

pub fn get_unix_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}

pub fn create_temp_db_path() -> PathBuf {
    let mut temp_path = temp_dir();
    temp_path.push(format!("bambang_test_{}.db", get_unix_timestamp_millis()));
    temp_path
}

pub fn create_temp_db_path_with_prefix(prefix: &str) -> PathBuf {
    let mut temp_path = temp_dir();
    temp_path.push(format!("{}_{}.db", prefix, get_unix_timestamp_millis()));
    temp_path
}

pub struct TempDatabase {
    pub path: PathBuf,
    pub storage_manager: Option<StorageManager>,
}

impl TempDatabase {
    pub fn new() -> Self {
        Self {
            path: create_temp_db_path(),
            storage_manager: None,
        }
    }

    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            path: create_temp_db_path_with_prefix(prefix),
            storage_manager: None,
        }
    }

    pub fn create_storage_manager(
        &mut self,
    ) -> Result<&mut StorageManager, Box<dyn std::error::Error>> {
        let sm = StorageManager::new(&self.path)?;
        self.storage_manager = Some(sm);
        Ok(self.storage_manager.as_mut().unwrap())
    }

    pub fn get_storage_manager(&mut self) -> Option<&mut StorageManager> {
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
