use std::{
    fs::OpenOptions,
    path::PathBuf,
};

use crate::{
    storage::{
        storage_manager::StorageManager,
        schema::{TableSchema, ColumnSchema},
        BAMBANG_HEADER_SIZE,
    },
    types::{
        error::DatabaseError,
        page::PageType,
        value::{DataType, Value},
        PageId,
    },
};

/// Trait for creating tables in the database
pub trait TableCreator {
    /// Create a new table with the specified schema
    fn create_table(&mut self, table_name: String, columns: Vec<ColumnSchema>, sql: String) -> Result<PageId, DatabaseError>;
    
    /// Check if a table exists
    fn table_exists(&self, table_name: &str) -> bool;
}

/// Table creator implementation that handles table creation operations
pub struct CreateTableExecutor {
    db_file_path: PathBuf,
    extras: Option<u64>,
}

impl CreateTableExecutor {
    /// Create a new CreateTableExecutor
    pub fn new(storage_manager: &StorageManager) -> Result<Self, DatabaseError> {
        let db_file_path = storage_manager.db_info.path.clone();
        let extras = Some(BAMBANG_HEADER_SIZE as u64);

        Ok(Self {
            db_file_path,
            extras,
        })
    }

    /// Validate column definitions
    fn validate_columns(&self, columns: &[ColumnSchema]) -> Result<(), DatabaseError> {
        if columns.is_empty() {
            return Err(DatabaseError::InvalidData {
                details: "Table must have at least one column".to_string(),
            });
        }

        // Check for duplicate column names
        let mut column_names = std::collections::HashSet::new();
        for column in columns {
            if !column_names.insert(&column.name) {
                return Err(DatabaseError::InvalidData {
                    details: format!("Duplicate column name: {}", column.name),
                });
            }
        }

        // Check for duplicate positions
        let mut positions = std::collections::HashSet::new();
        for column in columns {
            if !positions.insert(column.position) {
                return Err(DatabaseError::InvalidData {
                    details: format!("Duplicate column position: {}", column.position),
                });
            }
        }

        // Validate position sequence (should be 0, 1, 2, ...)
        let mut sorted_positions: Vec<usize> = columns.iter().map(|c| c.position).collect();
        sorted_positions.sort();
        for (i, &pos) in sorted_positions.iter().enumerate() {
            if pos != i {
                return Err(DatabaseError::InvalidData {
                    details: format!("Column positions must be sequential starting from 0, found gap at position {}", i),
                });
            }
        }

        // Validate primary key constraints
        let primary_key_count = columns.iter().filter(|c| c.primary_key).count();
        if primary_key_count > 1 {
            return Err(DatabaseError::InvalidData {
                details: "Table can have at most one primary key column".to_string(),
            });
        }

        Ok(())
    }

    /// Allocate a new page for the table
    fn allocate_table_page(&self, storage_manager: &mut StorageManager) -> Result<PageId, DatabaseError> {
        storage_manager.allocate_new_page(PageType::LeafTable)
    }

    /// Create table schema and validate it
    fn create_table_schema(
        &self,
        table_name: String,
        columns: Vec<ColumnSchema>,
        root_page_id: PageId,
        sql: String,
    ) -> Result<TableSchema, DatabaseError> {
        self.validate_columns(&columns)?;
        Ok(TableSchema::new(table_name, columns, root_page_id, sql))
    }
}

impl TableCreator for CreateTableExecutor {
    fn create_table(&mut self, table_name: String, columns: Vec<ColumnSchema>, sql: String) -> Result<PageId, DatabaseError> {
        // Note: We need a mutable reference to StorageManager to allocate pages and add schemas
        // This is a limitation of the current design - we'll need to refactor this
        // For now, we'll return an error indicating this needs to be handled differently
        Err(DatabaseError::ExecutionError {
            details: "CreateTableExecutor needs access to mutable StorageManager. Use StorageManager::create_table_with_schema instead.".to_string(),
        })
    }

    fn table_exists(&self, _table_name: &str) -> bool {
        // This would require access to StorageManager
        // For now, return false - this should be checked before calling create_table
        false
    }
}

/// Builder for creating table schemas
pub struct TableSchemaBuilder {
    table_name: String,
    columns: Vec<ColumnSchema>,
    sql: Option<String>,
}

impl TableSchemaBuilder {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            columns: Vec::new(),
            sql: None,
        }
    }

    pub fn add_column(mut self, name: String, data_type: DataType) -> Self {
        let position = self.columns.len();
        let column = ColumnSchema::new(name, data_type, position);
        self.columns.push(column);
        self
    }

    pub fn add_column_with_constraints(
        mut self,
        name: String,
        data_type: DataType,
        nullable: bool,
        default_value: Option<Value>,
        primary_key: bool,
        unique: bool,
    ) -> Self {
        let position = self.columns.len();
        let mut column = ColumnSchema::new(name, data_type, position);
        column.nullable = nullable;
        column.default_value = default_value;
        column.primary_key = primary_key;
        column.unique = unique;
        self.columns.push(column);
        self
    }

    pub fn with_sql(mut self, sql: String) -> Self {
        self.sql = Some(sql);
        self
    }

    pub fn build(self) -> Result<(String, Vec<ColumnSchema>, String), DatabaseError> {
        if self.columns.is_empty() {
            return Err(DatabaseError::InvalidData {
                details: "Table must have at least one column".to_string(),
            });
        }

        let sql = self.sql.unwrap_or_else(|| {
            let column_defs: Vec<String> = self.columns.iter().map(|col| {
                let mut def = format!("{} {}", col.name, col.data_type);
                if !col.nullable {
                    def.push_str(" NOT NULL");
                }
                if col.primary_key {
                    def.push_str(" PRIMARY KEY");
                }
                if col.unique && !col.primary_key {
                    def.push_str(" UNIQUE");
                }
                if let Some(ref default) = col.default_value {
                    def.push_str(&format!(" DEFAULT {}", default));
                }
                def
            }).collect();
            
            format!("CREATE TABLE {} ({})", self.table_name, column_defs.join(", "))
        });

        Ok((self.table_name, self.columns, sql))
    }
}

/// Extension methods for StorageManager to work with CreateTableExecutor
impl StorageManager {
    /// Create a table with schema using the executor pattern
    pub fn create_table_with_schema(
        &mut self,
        table_name: String,
        columns: Vec<ColumnSchema>,
        sql: String,
    ) -> Result<PageId, DatabaseError> {
        // Check if table already exists
        if self.table_exists(&table_name) {
            return Err(DatabaseError::ExecutionError {
                details: format!("Table '{}' already exists", table_name),
            });
        }

        // Create executor for validation
        let executor = CreateTableExecutor::new(self)?;
        
        // Validate columns
        executor.validate_columns(&columns)?;

        // Allocate new page for the table
        let root_page_id = self.allocate_new_page(PageType::LeafTable)?;

        // Create table schema
        let table_schema = executor.create_table_schema(table_name.clone(), columns, root_page_id, sql)?;

        // Add schema to storage manager
        self.add_table_schema(table_schema)?;

        Ok(root_page_id)
    }

    /// Create a table using the builder pattern
    pub fn create_table_with_builder(
        &mut self,
        builder: TableSchemaBuilder,
    ) -> Result<PageId, DatabaseError> {
        let (table_name, columns, sql) = builder.build()?;
        self.create_table_with_schema(table_name, columns, sql)
    }
}