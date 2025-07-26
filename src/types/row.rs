use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::types::{RowId, error::DatabaseError, value::Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct Row {
    pub row_id: Option<RowId>,
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self {
            row_id: None,
            values,
        }
    }

    pub fn with_row_id(row_id: RowId, values: Vec<Value>) -> Self {
        Self {
            row_id: Some(row_id),
            values,
        }
    }

    pub fn get_value(&self, column_index: usize) -> Option<&Value> {
        self.values.get(column_index)
    }

    pub fn set_value(&mut self, column_index: usize, value: Value) -> Result<(), DatabaseError> {
        if column_index >= self.values.len() {
            return Err(DatabaseError::ColumnIndexOutOfBounds {
                index: column_index,
            });
        }
        self.values[column_index] = value;
        Ok(())
    }

    pub fn size(&self) -> usize {
        let row_id_size = if self.row_id.is_some() { 8 } else { 0 };
        let values_size: usize = self.values.iter().map(|v| v.size()).sum();
        row_id_size + values_size + (self.values.len() * 4) // 4 bytes per value header
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let config = bincode::config::standard();
        bincode::encode_to_vec(self, config).unwrap_or_default()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DatabaseError> {
        let config = bincode::config::standard();
        bincode::decode_from_slice(bytes, config)
            .map(|(row, _len)| row)
            .map_err(|_| DatabaseError::SerializationError {
                details: "Failed to deserialize row".to_string(),
            })
    }
}
