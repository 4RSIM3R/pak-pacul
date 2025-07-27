use crate::types::{PageId, error::DatabaseError, value::Value};

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub key: Value,
    pub page_id: PageId,
}

impl Entry {
    pub fn new(key: Value, page_id: PageId) -> Self {
        Self { key, page_id }
    }

    /// Serialize entry to bytes for storage in interior pages
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        match &self.key {
            Value::Integer(i) => {
                bytes.push(1); // Type marker for Integer
                bytes.extend_from_slice(&i.to_le_bytes());
            }
            Value::Text(s) => {
                bytes.push(2); // Type marker for Text
                let text_bytes = s.as_bytes();
                bytes.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(text_bytes);
            }
            Value::Real(f) => {
                bytes.push(3); // Type marker for Real
                bytes.extend_from_slice(&f.to_le_bytes());
            }
            _ => {
                bytes.push(0); // Null or unsupported
            }
        };

        // Serialize page_id
        bytes.extend_from_slice(&self.page_id.to_le_bytes());

        bytes
    }

    /// Deserialize entry from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), DatabaseError> {
        if bytes.is_empty() {
            return Err(DatabaseError::SerializationError {
                details: "Empty bytes for Entry".to_string(),
            });
        }

        let mut offset = 0;
        let type_marker = bytes[offset];
        offset += 1;

        let key = match type_marker {
            1 => {
                if bytes.len() < offset + 8 {
                    return Err(DatabaseError::SerializationError {
                        details: "Insufficient bytes for Integer key".to_string(),
                    });
                }
                let value = i64::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                    bytes[offset + 4],
                    bytes[offset + 5],
                    bytes[offset + 6],
                    bytes[offset + 7],
                ]);
                offset += 8;
                Value::Integer(value)
            }
            2 => {
                if bytes.len() < offset + 4 {
                    return Err(DatabaseError::SerializationError {
                        details: "Insufficient bytes for Text length".to_string(),
                    });
                }
                let len = u32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]) as usize;
                offset += 4;

                if bytes.len() < offset + len {
                    return Err(DatabaseError::SerializationError {
                        details: "Insufficient bytes for Text data".to_string(),
                    });
                }

                let text =
                    String::from_utf8(bytes[offset..offset + len].to_vec()).map_err(|_| {
                        DatabaseError::SerializationError {
                            details: "Invalid UTF-8 in Text key".to_string(),
                        }
                    })?;
                offset += len;
                Value::Text(text)
            }
            3 => {
                if bytes.len() < offset + 8 {
                    return Err(DatabaseError::SerializationError {
                        details: "Insufficient bytes for Real key".to_string(),
                    });
                }
                let value = f64::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                    bytes[offset + 4],
                    bytes[offset + 5],
                    bytes[offset + 6],
                    bytes[offset + 7],
                ]);
                offset += 8;
                Value::Real(value)
            }
            _ => Value::Null,
        };

        if bytes.len() < offset + 8 {
            return Err(DatabaseError::SerializationError {
                details: "Insufficient bytes for page_id".to_string(),
            });
        }

        let page_id = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        Ok((Self::new(key, page_id), offset))
    }
}
