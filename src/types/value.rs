use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

pub enum DataType {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Integer(_) => DataType::Integer,
            Value::Real(_) => DataType::Real,
            Value::Text(_) => DataType::Text,
            Value::Blob(_) => DataType::Blob,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Value::Null => 0,
            Value::Integer(_) => 8,
            Value::Real(_) => 8,
            Value::Text(s) => s.len(),
            Value::Blob(b) => b.len(),
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
            _ => None,
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
            _ => None, // Mixed types
        }
    }
}
