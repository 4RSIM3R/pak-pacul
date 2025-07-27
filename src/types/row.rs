use serde::{Deserialize, Serialize};

use crate::types::{RowId, error::DatabaseError, value::Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        let mut size = 1; // has_row_id flag

        if self.row_id.is_some() {
            size += 8; // row_id (8 bytes for u64/i64)
        }

        size += 4; // value_count (4 bytes for u32)

        // Use Value's serialized_size method
        for value in &self.values {
            size += value.serialized_size();
        }

        size
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        // Row ID presence and value
        match self.row_id {
            Some(id) => {
                buffer.push(1); // Has row ID
                buffer.extend_from_slice(&id.to_le_bytes());
            }
            None => {
                buffer.push(0); // No row ID
            }
        }

        // Value count
        buffer.extend_from_slice(&(self.values.len() as u32).to_le_bytes());

        // Values - use Value's to_bytes method
        for value in &self.values {
            let value_bytes = value.to_bytes();
            buffer.extend_from_slice(&value_bytes);
        }

        buffer
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DatabaseError> {
        if bytes.is_empty() {
            return Err(DatabaseError::SerializationError {
                details: "Empty bytes".to_string(),
            });
        }

        let mut cursor = 0;

        // Parse row ID
        let row_id = if bytes[cursor] == 1 {
            cursor += 1;
            if cursor + 8 > bytes.len() {
                return Err(DatabaseError::SerializationError {
                    details: "Incomplete row ID".to_string(),
                });
            }
            let id = RowId::from_le_bytes([
                bytes[cursor],
                bytes[cursor + 1],
                bytes[cursor + 2],
                bytes[cursor + 3],
                bytes[cursor + 4],
                bytes[cursor + 5],
                bytes[cursor + 6],
                bytes[cursor + 7],
            ]);
            cursor += 8;
            Some(id)
        } else {
            cursor += 1;
            None
        };

        // Parse value count
        if cursor + 4 > bytes.len() {
            return Err(DatabaseError::SerializationError {
                details: "Incomplete value count".to_string(),
            });
        }
        let value_count = u32::from_le_bytes([
            bytes[cursor],
            bytes[cursor + 1],
            bytes[cursor + 2],
            bytes[cursor + 3],
        ]) as usize;
        cursor += 4;

        // Parse values using Value's from_bytes method
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            let (value, consumed) = Self::deserialize_value(&bytes[cursor..])?;
            values.push(value);
            cursor += consumed;
        }

        Ok(Row { row_id, values })
    }

    /// Helper function to deserialize a value and return the number of bytes consumed
    fn deserialize_value(bytes: &[u8]) -> Result<(Value, usize), DatabaseError> {
        if bytes.is_empty() {
            return Err(DatabaseError::SerializationError {
                details: "Empty value bytes".to_string(),
            });
        }

        let type_discriminant = bytes[0];
        
        // Calculate expected size based on type discriminant
        let expected_size = match type_discriminant {
            0 => 1, // Null
            1 => 1 + 8, // Integer
            2 => 1 + 8, // Real
            3 => {
                // Text - need to read length first
                if bytes.len() < 5 {
                    return Err(DatabaseError::SerializationError {
                        details: "Incomplete text length".to_string(),
                    });
                }
                let length = u32::from_le_bytes([
                    bytes[1],
                    bytes[2],
                    bytes[3],
                    bytes[4],
                ]) as usize;
                1 + 4 + length
            }
            4 => {
                // Blob - need to read length first
                if bytes.len() < 5 {
                    return Err(DatabaseError::SerializationError {
                        details: "Incomplete blob length".to_string(),
                    });
                }
                let length = u32::from_le_bytes([
                    bytes[1],
                    bytes[2],
                    bytes[3],
                    bytes[4],
                ]) as usize;
                1 + 4 + length
            }
            5 => 1 + 1, // Boolean
            6 => 1 + 8, // Timestamp
            _ => {
                return Err(DatabaseError::SerializationError {
                    details: format!("Unknown type discriminant: {}", type_discriminant),
                });
            }
        };

        // Ensure we have enough bytes
        if bytes.len() < expected_size {
            return Err(DatabaseError::SerializationError {
                details: format!("Insufficient bytes for value type {}: expected {}, got {}", 
                    type_discriminant, expected_size, bytes.len()),
            });
        }

        // Extract the exact bytes for this value and deserialize
        let value_bytes = &bytes[0..expected_size];
        let value = Value::from_bytes(value_bytes)?;
        
        Ok((value, expected_size))
    }
}
