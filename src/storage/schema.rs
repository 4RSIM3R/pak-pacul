use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::types::{
    value::{DataType, Value},
    error::DatabaseError,
    row::Row,
    PageId,
};

/// Represents a column definition in a table schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub position: usize,
    pub nullable: bool,
    pub default_value: Option<Value>,
    pub primary_key: bool,
    pub unique: bool,
}

impl ColumnSchema {
    pub fn new(name: String, data_type: DataType, position: usize) -> Self {
        Self {
            name,
            data_type,
            position,
            nullable: true,
            default_value: None,
            primary_key: false,
            unique: false,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn with_default(mut self, default_value: Value) -> Self {
        self.default_value = Some(default_value);
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false; // Primary keys are always NOT NULL
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Convert column schema to a row for storage in sqlite_schema
    pub fn to_schema_row(&self, table_name: &str) -> Row {
        Row::new(vec![
            Value::Text("column".to_string()),
            Value::Text(self.name.clone()),
            Value::Text(table_name.to_string()),
            Value::Integer(self.position as i64),
            Value::Text(self.data_type.to_string()),
            Value::Integer(if self.nullable { 1 } else { 0 }),
            Value::Text(
                self.default_value
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            ),
            Value::Integer(if self.primary_key { 1 } else { 0 }),
            Value::Integer(if self.unique { 1 } else { 0 }),
        ])
    }

    /// Create column schema from a schema row
    pub fn from_schema_row(row: &Row) -> Result<Self, DatabaseError> {
        if row.values.len() < 9 {
            return Err(DatabaseError::CorruptedDatabase {
                reason: "Invalid column schema row format".to_string(),
            });
        }

        let name = match &row.values[1] {
            Value::Text(name) => name.clone(),
            _ => return Err(DatabaseError::CorruptedDatabase {
                reason: "Invalid column name in schema".to_string(),
            }),
        };

        let position = match &row.values[3] {
            Value::Integer(pos) => *pos as usize,
            _ => return Err(DatabaseError::CorruptedDatabase {
                reason: "Invalid column position in schema".to_string(),
            }),
        };

        let data_type = match &row.values[4] {
            Value::Text(type_str) => DataType::from_string(type_str)?,
            _ => return Err(DatabaseError::CorruptedDatabase {
                reason: "Invalid column data type in schema".to_string(),
            }),
        };

        let nullable = match &row.values[5] {
            Value::Integer(nullable) => *nullable != 0,
            _ => return Err(DatabaseError::CorruptedDatabase {
                reason: "Invalid nullable flag in schema".to_string(),
            }),
        };

        let default_value = match &row.values[6] {
            Value::Text(default_str) if default_str != "NULL" => {
                Some(Value::from_string(default_str, &data_type)?)
            },
            _ => None,
        };

        let primary_key = match &row.values[7] {
            Value::Integer(pk) => *pk != 0,
            _ => return Err(DatabaseError::CorruptedDatabase {
                reason: "Invalid primary key flag in schema".to_string(),
            }),
        };

        let unique = match &row.values[8] {
            Value::Integer(unique) => *unique != 0,
            _ => return Err(DatabaseError::CorruptedDatabase {
                reason: "Invalid unique flag in schema".to_string(),
            }),
        };

        Ok(Self {
            name,
            data_type,
            position,
            nullable,
            default_value,
            primary_key,
            unique,
        })
    }
}

/// Represents a complete table schema with all column definitions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnSchema>,
    pub root_page_id: PageId,
    pub sql: String,
}

impl TableSchema {
    pub fn new(table_name: String, columns: Vec<ColumnSchema>, root_page_id: PageId, sql: String) -> Self {
        Self {
            table_name,
            columns,
            root_page_id,
            sql,
        }
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|col| col.name == name)
    }

    /// Get column by position
    pub fn get_column_by_position(&self, position: usize) -> Option<&ColumnSchema> {
        self.columns.iter().find(|col| col.position == position)
    }

    /// Get column index by name
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == name)
    }

    /// Get all column names in order
    pub fn column_names(&self) -> Vec<String> {
        let mut sorted_columns = self.columns.clone();
        sorted_columns.sort_by_key(|col| col.position);
        sorted_columns.iter().map(|col| col.name.clone()).collect()
    }

    /// Get primary key columns
    pub fn primary_key_columns(&self) -> Vec<&ColumnSchema> {
        self.columns.iter().filter(|col| col.primary_key).collect()
    }

    /// Validate a row against this schema
    pub fn validate_row(&self, row: &Row) -> Result<(), DatabaseError> {
        // Check column count
        if row.values.len() != self.columns.len() {
            return Err(DatabaseError::InvalidData {
                details: format!(
                    "Row has {} values but table '{}' expects {} columns",
                    row.values.len(),
                    self.table_name,
                    self.columns.len()
                ),
            });
        }

        // Validate each column
        for (i, value) in row.values.iter().enumerate() {
            if let Some(column) = self.get_column_by_position(i) {
                // Check null constraints
                if !column.nullable && matches!(value, Value::Null) {
                    return Err(DatabaseError::InvalidData {
                        details: format!(
                            "Column '{}' cannot be NULL",
                            column.name
                        ),
                    });
                }

                // Check data type compatibility
                if !matches!(value, Value::Null) && !value.is_compatible_with_type(&column.data_type) {
                    return Err(DatabaseError::InvalidData {
                        details: format!(
                            "Value {:?} is not compatible with column '{}' of type {:?}",
                            value, column.name, column.data_type
                        ),
                    });
                }
            }
        }

        Ok(())
    }

    /// Apply default values to a row where values are missing or null
    pub fn apply_defaults(&self, row: &mut Row) -> Result<(), DatabaseError> {
        // Extend row if it has fewer values than columns
        while row.values.len() < self.columns.len() {
            row.values.push(Value::Null);
        }

        // Apply default values
        for column in &self.columns {
            if let Some(default_value) = &column.default_value {
                if row.values.len() > column.position {
                    if matches!(row.values[column.position], Value::Null) {
                        row.values[column.position] = default_value.clone();
                    }
                }
            }
        }

        Ok(())
    }
}

/// Schema manager for handling table and column schemas
#[derive(Debug, Clone)]
pub struct SchemaManager {
    pub table_schemas: HashMap<String, TableSchema>,
}

impl SchemaManager {
    pub fn new() -> Self {
        Self {
            table_schemas: HashMap::new(),
        }
    }

    /// Add a table schema
    pub fn add_table_schema(&mut self, schema: TableSchema) {
        self.table_schemas.insert(schema.table_name.clone(), schema);
    }

    /// Get a table schema by name
    pub fn get_table_schema(&self, table_name: &str) -> Option<&TableSchema> {
        self.table_schemas.get(table_name)
    }

    /// Remove a table schema
    pub fn remove_table_schema(&mut self, table_name: &str) -> Option<TableSchema> {
        self.table_schemas.remove(table_name)
    }

    /// Get all table names
    pub fn table_names(&self) -> Vec<&str> {
        self.table_schemas.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a table exists
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.table_schemas.contains_key(table_name)
    }
}

impl Default for SchemaManager {
    fn default() -> Self {
        Self::new()
    }
}