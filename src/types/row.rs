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

    // pub fn size(&self) -> usize {
    //     let row_id_size = if self.row_id.is_some() { 8 } else { 0 };
    //     let values_size: usize = self.values.iter().map(|v| v.size()).sum();
    //     row_id_size + values_size + (self.values.len() * 4) // 4 bytes per value header
    // }

    pub fn size(&self) -> usize {
        let mut size = 1; // has_row_id flag

        if self.row_id.is_some() {
            size += 8; // row_id
        }

        size += 4; // value_count

        for value in &self.values {
            size += match value {
                Value::Null => 1,
                Value::Integer(_) => 9,                   // 1 type + 8 data
                Value::Real(_) => 9,                      // 1 type + 8 data
                Value::Text(s) => 5 + s.as_bytes().len(), // 1 type + 4 length + data
                Value::Blob(b) => 5 + b.len(),            // 1 type + 4 length + data
                Value::Boolean(_) => 2,                   // 1 type + 1 data
                Value::Timestamp(_) => 9,                 // 1 type + 8 data
            };
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

        // Values
        for value in &self.values {
            self.serialize_value_compact(value, &mut buffer);
        }

        buffer
    }

    fn serialize_value_compact(&self, value: &Value, buffer: &mut Vec<u8>) {
        match value {
            Value::Null => {
                buffer.push(0); // Type discriminant
            }
            Value::Integer(i) => {
                buffer.push(1);
                buffer.extend_from_slice(&i.to_le_bytes());
            }
            Value::Real(f) => {
                buffer.push(2);
                buffer.extend_from_slice(&f.to_le_bytes());
            }
            Value::Text(s) => {
                buffer.push(3);
                let bytes = s.as_bytes();
                buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buffer.extend_from_slice(bytes);
            }
            Value::Blob(b) => {
                buffer.push(4);
                buffer.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buffer.extend_from_slice(b);
            }
            Value::Boolean(b) => {
                buffer.push(5);
                buffer.push(if *b { 1 } else { 0 });
            }
            Value::Timestamp(t) => {
                buffer.push(6);
                buffer.extend_from_slice(&t.to_le_bytes());
            }
        }
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

        // Parse values
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            let (value, consumed) = Row::deserialize_value_compact(&bytes[cursor..])?;
            values.push(value);
            cursor += consumed;
        }

        Ok(Row { row_id, values })
    }

    fn deserialize_value_compact(bytes: &[u8]) -> Result<(Value, usize), DatabaseError> {
        if bytes.is_empty() {
            return Err(DatabaseError::SerializationError {
                details: "Empty value bytes".to_string(),
            });
        }

        let type_discriminant = bytes[0];
        let mut cursor = 1;

        match type_discriminant {
            0 => Ok((Value::Null, 1)),
            1 => {
                if cursor + 8 > bytes.len() {
                    return Err(DatabaseError::SerializationError {
                        details: "Incomplete integer".to_string(),
                    });
                }
                let value = i64::from_le_bytes([
                    bytes[cursor],
                    bytes[cursor + 1],
                    bytes[cursor + 2],
                    bytes[cursor + 3],
                    bytes[cursor + 4],
                    bytes[cursor + 5],
                    bytes[cursor + 6],
                    bytes[cursor + 7],
                ]);
                Ok((Value::Integer(value), 9))
            }
            2 => {
                if cursor + 8 > bytes.len() {
                    return Err(DatabaseError::SerializationError {
                        details: "Incomplete real".to_string(),
                    });
                }
                let value = f64::from_le_bytes([
                    bytes[cursor],
                    bytes[cursor + 1],
                    bytes[cursor + 2],
                    bytes[cursor + 3],
                    bytes[cursor + 4],
                    bytes[cursor + 5],
                    bytes[cursor + 6],
                    bytes[cursor + 7],
                ]);
                Ok((Value::Real(value), 9))
            }
            3 => {
                if cursor + 4 > bytes.len() {
                    return Err(DatabaseError::SerializationError {
                        details: "Incomplete text length".to_string(),
                    });
                }
                let length = u32::from_le_bytes([
                    bytes[cursor],
                    bytes[cursor + 1],
                    bytes[cursor + 2],
                    bytes[cursor + 3],
                ]) as usize;
                cursor += 4;

                if cursor + length > bytes.len() {
                    return Err(DatabaseError::SerializationError {
                        details: "Incomplete text data".to_string(),
                    });
                }

                let text =
                    String::from_utf8(bytes[cursor..cursor + length].to_vec()).map_err(|_| {
                        DatabaseError::SerializationError {
                            details: "Invalid UTF-8 in text".to_string(),
                        }
                    })?;

                Ok((Value::Text(text), 5 + length))
            }
            4 => {
                if cursor + 4 > bytes.len() {
                    return Err(DatabaseError::SerializationError {
                        details: "Incomplete blob length".to_string(),
                    });
                }
                let length = u32::from_le_bytes([
                    bytes[cursor],
                    bytes[cursor + 1],
                    bytes[cursor + 2],
                    bytes[cursor + 3],
                ]) as usize;
                cursor += 4;

                if cursor + length > bytes.len() {
                    return Err(DatabaseError::SerializationError {
                        details: "Incomplete blob data".to_string(),
                    });
                }

                let blob = bytes[cursor..cursor + length].to_vec();
                Ok((Value::Blob(blob), 5 + length))
            }
            5 => {
                if cursor >= bytes.len() {
                    return Err(DatabaseError::SerializationError {
                        details: "Incomplete boolean".to_string(),
                    });
                }
                let value = bytes[cursor] != 0;
                Ok((Value::Boolean(value), 2))
            }
            6 => {
                if cursor + 8 > bytes.len() {
                    return Err(DatabaseError::SerializationError {
                        details: "Incomplete timestamp".to_string(),
                    });
                }
                let value = i64::from_le_bytes([
                    bytes[cursor],
                    bytes[cursor + 1],
                    bytes[cursor + 2],
                    bytes[cursor + 3],
                    bytes[cursor + 4],
                    bytes[cursor + 5],
                    bytes[cursor + 6],
                    bytes[cursor + 7],
                ]);
                Ok((Value::Timestamp(value), 9))
            }
            _ => Err(DatabaseError::SerializationError {
                details: format!("Unknown type discriminant: {}", type_discriminant),
            }),
        }
    }

}
