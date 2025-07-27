use std::cmp::Ordering;

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::types::error::DatabaseError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Null,
    Integer,
    Real,
    Text,
    Blob,
    Boolean,
    Timestamp,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Null => write!(f, "NULL"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Real => write!(f, "REAL"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Blob => write!(f, "BLOB"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
        }
    }
}

impl DataType {
    /// Create DataType from string representation
    pub fn from_string(s: &str) -> Result<Self, DatabaseError> {
        match s.to_uppercase().as_str() {
            "NULL" => Ok(DataType::Null),
            "INTEGER" | "INT" => Ok(DataType::Integer),
            "REAL" | "FLOAT" | "DOUBLE" => Ok(DataType::Real),
            "TEXT" | "STRING" | "VARCHAR" => Ok(DataType::Text),
            "BLOB" | "BINARY" => Ok(DataType::Blob),
            "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
            "TIMESTAMP" | "DATETIME" => Ok(DataType::Timestamp),
            _ => Err(DatabaseError::SerializationError {
                details: format!("Unknown data type: {}", s),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
    Timestamp(i64),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Integer(_) => DataType::Integer,
            Value::Real(_) => DataType::Real,
            Value::Text(_) => DataType::Text,
            Value::Blob(_) => DataType::Blob,
            Value::Boolean(_) => DataType::Boolean,
            Value::Timestamp(_) => DataType::Timestamp,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Value::Null => 0,
            Value::Integer(_) => 8,
            Value::Real(_) => 8,
            Value::Text(s) => s.len(),
            Value::Blob(b) => b.len(),
            Value::Boolean(_) => 1,
            Value::Timestamp(_) => 8, // 8 bytes for timestamp (Unix timestamp as i64)
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn coerce_to_number(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Real(r) => Some(*r),
            Value::Text(s) => s.parse().ok(),
            Value::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            Value::Timestamp(ts) => Some(*ts as f64),
            _ => None,
        }
    }

    /// Convert value to boolean following SQL-like semantics
    pub fn coerce_to_boolean(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::Integer(i) => Some(*i != 0),
            Value::Real(r) => Some(*r != 0.0),
            Value::Text(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "yes" | "y" | "1" => Some(true),
                "false" | "f" | "no" | "n" | "0" => Some(false),
                _ => None,
            },
            Value::Null => Some(false), // NULL is falsy in SQL
            _ => None,
        }
    }

    /// Create a timestamp from various input formats
    pub fn timestamp_from_str(s: &str) -> Result<Value, DatabaseError> {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(Value::Timestamp(dt.timestamp()));
        }

        // Try datetime format (e.g., "2022-01-01 12:30:45")
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            let utc_dt = Utc.from_utc_datetime(&dt);
            return Ok(Value::Timestamp(utc_dt.timestamp()));
        }

        // Try date-only format (e.g., "2022-01-01") - this was the bug!
        if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            // Convert date to datetime at midnight UTC
            let dt = date.and_hms_opt(0, 0, 0).unwrap(); // Safe unwrap for midnight
            let utc_dt = Utc.from_utc_datetime(&dt);
            return Ok(Value::Timestamp(utc_dt.timestamp()));
        }

        Err(DatabaseError::SerializationError {
            details: "Failed to parse timestamp".to_string(),
        })
    }

    /// Create a timestamp from Unix timestamp (seconds since epoch)
    pub fn timestamp_from_unix(timestamp: i64) -> Value {
        Value::Timestamp(timestamp)
    }

    /// Get current timestamp as Unix timestamp
    pub fn now() -> Value {
        Value::Timestamp(Utc::now().timestamp())
    }

    /// Convert timestamp to DateTime<Utc> for display/formatting purposes
    pub fn to_datetime(&self) -> Option<DateTime<Utc>> {
        match self {
            Value::Timestamp(ts) => Utc.timestamp_opt(*ts, 0).single(),
            _ => None,
        }
    }

    /// Format timestamp as string (convenience method)
    pub fn format_timestamp(&self, format: &str) -> Option<String> {
        self.to_datetime().map(|dt| dt.format(format).to_string())
    }

    /// Convert Value to bytes using custom binary format
    ///
    /// Binary format:
    /// - 1 byte: type discriminant (0=Null, 1=Integer, 2=Real, 3=Text, 4=Blob, 5=Boolean, 6=Timestamp)
    /// - Variable length data based on type
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        match self {
            Value::Null => {
                bytes.push(0); // Type discriminant for Null
                // No additional data
            }
            Value::Integer(i) => {
                bytes.push(1); // Type discriminant for Integer
                bytes.extend_from_slice(&i.to_le_bytes());
            }
            Value::Real(r) => {
                bytes.push(2); // Type discriminant for Real
                bytes.extend_from_slice(&r.to_le_bytes());
            }
            Value::Text(s) => {
                bytes.push(3); // Type discriminant for Text
                let text_bytes = s.as_bytes();
                // Store length as 4-byte little-endian integer
                bytes.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(text_bytes);
            }
            Value::Blob(b) => {
                bytes.push(4); // Type discriminant for Blob
                // Store length as 4-byte little-endian integer
                bytes.extend_from_slice(&(b.len() as u32).to_le_bytes());
                bytes.extend_from_slice(b);
            }
            Value::Boolean(b) => {
                bytes.push(5); // Type discriminant for Boolean
                bytes.push(if *b { 1 } else { 0 });
            }
            Value::Timestamp(ts) => {
                bytes.push(6); // Type discriminant for Timestamp
                bytes.extend_from_slice(&ts.to_le_bytes());
            }
        }

        bytes
    }

    /// Create Value from bytes using custom binary format
    pub fn from_bytes(bytes: &[u8]) -> Result<Value, DatabaseError> {
        if bytes.is_empty() {
            return Err(DatabaseError::SerializationError {
                details: "Empty byte array".to_string(),
            });
        }

        let type_discriminant = bytes[0];
        let data = &bytes[1..];

        match type_discriminant {
            0 => Ok(Value::Null),
            1 => {
                // Integer
                if data.len() != 8 {
                    return Err(DatabaseError::SerializationError {
                        details: "Invalid integer data length".to_string(),
                    });
                }
                let mut int_bytes = [0u8; 8];
                int_bytes.copy_from_slice(data);
                Ok(Value::Integer(i64::from_le_bytes(int_bytes)))
            }
            2 => {
                // Real
                if data.len() != 8 {
                    return Err(DatabaseError::SerializationError {
                        details: "Invalid real data length".to_string(),
                    });
                }
                let mut real_bytes = [0u8; 8];
                real_bytes.copy_from_slice(data);
                Ok(Value::Real(f64::from_le_bytes(real_bytes)))
            }
            3 => {
                // Text
                if data.len() < 4 {
                    return Err(DatabaseError::SerializationError {
                        details: "Invalid text data: missing length".to_string(),
                    });
                }
                let mut len_bytes = [0u8; 4];
                len_bytes.copy_from_slice(&data[0..4]);
                let text_len = u32::from_le_bytes(len_bytes) as usize;

                if data.len() != 4 + text_len {
                    return Err(DatabaseError::SerializationError {
                        details: "Invalid text data: length mismatch".to_string(),
                    });
                }

                let text_bytes = &data[4..4 + text_len];
                match String::from_utf8(text_bytes.to_vec()) {
                    Ok(s) => Ok(Value::Text(s)),
                    Err(_) => Err(DatabaseError::SerializationError {
                        details: "Invalid UTF-8 in text data".to_string(),
                    }),
                }
            }
            4 => {
                // Blob
                if data.len() < 4 {
                    return Err(DatabaseError::SerializationError {
                        details: "Invalid blob data: missing length".to_string(),
                    });
                }
                let mut len_bytes = [0u8; 4];
                len_bytes.copy_from_slice(&data[0..4]);
                let blob_len = u32::from_le_bytes(len_bytes) as usize;

                if data.len() != 4 + blob_len {
                    return Err(DatabaseError::SerializationError {
                        details: "Invalid blob data: length mismatch".to_string(),
                    });
                }

                let blob_data = data[4..4 + blob_len].to_vec();
                Ok(Value::Blob(blob_data))
            }
            5 => {
                // Boolean
                if data.len() != 1 {
                    return Err(DatabaseError::SerializationError {
                        details: "Invalid boolean data length".to_string(),
                    });
                }
                Ok(Value::Boolean(data[0] != 0))
            }
            6 => {
                // Timestamp
                if data.len() != 8 {
                    return Err(DatabaseError::SerializationError {
                        details: "Invalid timestamp data length".to_string(),
                    });
                }
                let mut ts_bytes = [0u8; 8];
                ts_bytes.copy_from_slice(data);
                Ok(Value::Timestamp(i64::from_le_bytes(ts_bytes)))
            }
            _ => Err(DatabaseError::SerializationError {
                details: format!("Unknown type discriminant: {}", type_discriminant),
            }),
        }
    }

    /// Get the serialized size in bytes (useful for storage planning)
    pub fn serialized_size(&self) -> usize {
        match self {
            Value::Null => 1,                  // Just the type discriminant
            Value::Integer(_) => 1 + 8,        // Type + 8 bytes for i64
            Value::Real(_) => 1 + 8,           // Type + 8 bytes for f64
            Value::Text(s) => 1 + 4 + s.len(), // Type + length (4 bytes) + string bytes
            Value::Blob(b) => 1 + 4 + b.len(), // Type + length (4 bytes) + blob bytes
            Value::Boolean(_) => 1 + 1,        // Type + 1 byte for boolean
            Value::Timestamp(_) => 1 + 8,      // Type + 8 bytes for i64
        }
    }

    /// Create Value from string representation with specified data type
    pub fn from_string(s: &str, data_type: &DataType) -> Result<Self, DatabaseError> {
        if s == "NULL" {
            return Ok(Value::Null);
        }

        match data_type {
            DataType::Null => Ok(Value::Null),
            DataType::Integer => {
                s.parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|_| DatabaseError::SerializationError {
                        details: format!("Cannot parse '{}' as integer", s),
                    })
            }
            DataType::Real => {
                s.parse::<f64>()
                    .map(Value::Real)
                    .map_err(|_| DatabaseError::SerializationError {
                        details: format!("Cannot parse '{}' as real", s),
                    })
            }
            DataType::Text => Ok(Value::Text(s.to_string())),
            DataType::Blob => {
                // For simplicity, treat as hex string or convert string to bytes
                if s.starts_with("0x") || s.starts_with("0X") {
                    let hex_str = &s[2..];
                    // Simple hex decode without external dependency
                    let mut bytes = Vec::new();
                    let chars: Vec<char> = hex_str.chars().collect();
                    if chars.len() % 2 != 0 {
                        return Err(DatabaseError::SerializationError {
                            details: format!("Invalid hex string length: {}", s),
                        });
                    }
                    for chunk in chars.chunks(2) {
                        let hex_byte = format!("{}{}", chunk[0], chunk[1]);
                        match u8::from_str_radix(&hex_byte, 16) {
                            Ok(byte) => bytes.push(byte),
                            Err(_) => return Err(DatabaseError::SerializationError {
                                details: format!("Cannot parse '{}' as hex blob", s),
                            }),
                        }
                    }
                    Ok(Value::Blob(bytes))
                } else {
                    Ok(Value::Blob(s.as_bytes().to_vec()))
                }
            }
            DataType::Boolean => {
                match s.to_lowercase().as_str() {
                    "true" | "t" | "yes" | "y" | "1" => Ok(Value::Boolean(true)),
                    "false" | "f" | "no" | "n" | "0" => Ok(Value::Boolean(false)),
                    _ => Err(DatabaseError::SerializationError {
                        details: format!("Cannot parse '{}' as boolean", s),
                    }),
                }
            }
            DataType::Timestamp => Value::timestamp_from_str(s),
        }
    }

    /// Check if this value is compatible with the specified data type
    pub fn is_compatible_with_type(&self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (Value::Null, _) => true, // NULL is compatible with any type
            (Value::Integer(_), DataType::Integer) => true,
            (Value::Real(_), DataType::Real) => true,
            (Value::Text(_), DataType::Text) => true,
            (Value::Blob(_), DataType::Blob) => true,
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Timestamp(_), DataType::Timestamp) => true,
            // Allow some cross-type compatibility
            (Value::Integer(_), DataType::Real) => true, // Integer can be promoted to Real
            (Value::Boolean(_), DataType::Integer) => true, // Boolean can be converted to Integer
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Real(b)) => (*a as f64).partial_cmp(b),
            (Value::Real(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Real(a), Value::Real(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (a, b) => {
                match (a.coerce_to_number(), b.coerce_to_number()) {
                    (Some(x), Some(y)) => x.partial_cmp(&y),
                    _ => None, // Incomparable types
                }
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Exact type matches
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,

            // Cross-type numeric comparisons
            (Value::Integer(a), Value::Real(b)) => (*a as f64) == *b,
            (Value::Real(a), Value::Integer(b)) => *a == (*b as f64),

            // Try coercing to numbers for other cross-type comparisons
            (a, b) => {
                // Don't compare null with non-null values
                if a.is_null() || b.is_null() {
                    return false;
                }

                match (a.coerce_to_number(), b.coerce_to_number()) {
                    (Some(x), Some(y)) => x == y,
                    _ => false, // Incomparable types are not equal
                }
            }
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Real(r) => write!(f, "{}", r),
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => write!(f, "BLOB({} bytes)", b.len()),
            Value::Boolean(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
            Value::Timestamp(ts) => {
                if let Some(dt) = Utc.timestamp_opt(*ts, 0).single() {
                    write!(f, "{}", dt.format("%Y-%m-%d %H:%M:%S UTC"))
                } else {
                    write!(f, "INVALID_TIMESTAMP({})", ts)
                }
            }
        }
    }
}
