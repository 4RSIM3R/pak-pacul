use std::cmp::Ordering;

use bincode::{Decode, Encode};
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

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
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
