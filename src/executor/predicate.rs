use std::collections::HashMap;

use crate::{
    storage::schema::TableSchema,
    types::{
        error::DatabaseError,
        row::Row,
        value::{DataType, Value},
    },
};

/// Comparison operators for predicates
#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    IsNull,
    IsNotNull,
    Like,
    NotLike,
    In,
    NotIn,
}

/// Logical operators for combining predicates
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOp {
    And,
    Or,
    Not,
}

/// A predicate expression for filtering rows
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Column comparison: column_name op value
    Comparison {
        column_name: String,
        op: ComparisonOp,
        value: Value,
    },
    /// Column comparison with multiple values (for IN/NOT IN)
    InList {
        column_name: String,
        values: Vec<Value>,
        negated: bool,
    },
    /// Logical combination of predicates
    Logical {
        op: LogicalOp,
        left: Box<Predicate>,
        right: Option<Box<Predicate>>, // None for NOT operator
    },
    /// Always true predicate
    True,
    /// Always false predicate
    False,
}

impl Predicate {
    /// Create an equality predicate
    pub fn eq(column_name: String, value: Value) -> Self {
        Self::Comparison {
            column_name,
            op: ComparisonOp::Equal,
            value,
        }
    }

    /// Create a not equal predicate
    pub fn ne(column_name: String, value: Value) -> Self {
        Self::Comparison {
            column_name,
            op: ComparisonOp::NotEqual,
            value,
        }
    }

    /// Create a less than predicate
    pub fn lt(column_name: String, value: Value) -> Self {
        Self::Comparison {
            column_name,
            op: ComparisonOp::LessThan,
            value,
        }
    }

    /// Create a less than or equal predicate
    pub fn le(column_name: String, value: Value) -> Self {
        Self::Comparison {
            column_name,
            op: ComparisonOp::LessThanOrEqual,
            value,
        }
    }

    /// Create a greater than predicate
    pub fn gt(column_name: String, value: Value) -> Self {
        Self::Comparison {
            column_name,
            op: ComparisonOp::GreaterThan,
            value,
        }
    }

    /// Create a greater than or equal predicate
    pub fn ge(column_name: String, value: Value) -> Self {
        Self::Comparison {
            column_name,
            op: ComparisonOp::GreaterThanOrEqual,
            value,
        }
    }

    /// Create an IS NULL predicate
    pub fn is_null(column_name: String) -> Self {
        Self::Comparison {
            column_name,
            op: ComparisonOp::IsNull,
            value: Value::Null,
        }
    }

    /// Create an IS NOT NULL predicate
    pub fn is_not_null(column_name: String) -> Self {
        Self::Comparison {
            column_name,
            op: ComparisonOp::IsNotNull,
            value: Value::Null,
        }
    }

    /// Create an IN predicate
    pub fn in_list(column_name: String, values: Vec<Value>) -> Self {
        Self::InList {
            column_name,
            values,
            negated: false,
        }
    }

    /// Create a NOT IN predicate
    pub fn not_in_list(column_name: String, values: Vec<Value>) -> Self {
        Self::InList {
            column_name,
            values,
            negated: true,
        }
    }

    /// Create an AND predicate
    pub fn and(left: Predicate, right: Predicate) -> Self {
        Self::Logical {
            op: LogicalOp::And,
            left: Box::new(left),
            right: Some(Box::new(right)),
        }
    }

    /// Create an OR predicate
    pub fn or(left: Predicate, right: Predicate) -> Self {
        Self::Logical {
            op: LogicalOp::Or,
            left: Box::new(left),
            right: Some(Box::new(right)),
        }
    }

    /// Create a NOT predicate
    pub fn not(predicate: Predicate) -> Self {
        Self::Logical {
            op: LogicalOp::Not,
            left: Box::new(predicate),
            right: None,
        }
    }

    /// Evaluate the predicate against a row using the table schema
    pub fn evaluate(&self, row: &Row, schema: &TableSchema) -> Result<bool, DatabaseError> {
        match self {
            Predicate::Comparison { column_name, op, value } => {
                let column_index = schema.get_column_index(column_name)
                    .ok_or_else(|| DatabaseError::ColumnNotFound {
                        name: column_name.clone(),
                        table: schema.table_name.clone(),
                    })?;

                if column_index >= row.values.len() {
                    return Err(DatabaseError::ColumnIndexOutOfBounds { index: column_index });
                }

                let row_value = &row.values[column_index];
                self.compare_values(row_value, op, value)
            }
            Predicate::InList { column_name, values, negated } => {
                let column_index = schema.get_column_index(column_name)
                    .ok_or_else(|| DatabaseError::ColumnNotFound {
                        name: column_name.clone(),
                        table: schema.table_name.clone(),
                    })?;

                if column_index >= row.values.len() {
                    return Err(DatabaseError::ColumnIndexOutOfBounds { index: column_index });
                }

                let row_value = &row.values[column_index];
                let in_list = values.iter().any(|v| self.values_equal(row_value, v));
                Ok(if *negated { !in_list } else { in_list })
            }
            Predicate::Logical { op, left, right } => {
                match op {
                    LogicalOp::And => {
                        let left_result = left.evaluate(row, schema)?;
                        if !left_result {
                            return Ok(false); // Short-circuit evaluation
                        }
                        if let Some(right_pred) = right {
                            right_pred.evaluate(row, schema)
                        } else {
                            Err(DatabaseError::ExecutionError {
                                details: "AND operator requires two operands".to_string(),
                            })
                        }
                    }
                    LogicalOp::Or => {
                        let left_result = left.evaluate(row, schema)?;
                        if left_result {
                            return Ok(true); // Short-circuit evaluation
                        }
                        if let Some(right_pred) = right {
                            right_pred.evaluate(row, schema)
                        } else {
                            Err(DatabaseError::ExecutionError {
                                details: "OR operator requires two operands".to_string(),
                            })
                        }
                    }
                    LogicalOp::Not => {
                        let result = left.evaluate(row, schema)?;
                        Ok(!result)
                    }
                }
            }
            Predicate::True => Ok(true),
            Predicate::False => Ok(false),
        }
    }

    /// Compare two values using the specified operator
    fn compare_values(&self, left: &Value, op: &ComparisonOp, right: &Value) -> Result<bool, DatabaseError> {
        match op {
            ComparisonOp::Equal => Ok(self.values_equal(left, right)),
            ComparisonOp::NotEqual => Ok(!self.values_equal(left, right)),
            ComparisonOp::LessThan => {
                match left.partial_cmp(right) {
                    Some(std::cmp::Ordering::Less) => Ok(true),
                    Some(_) => Ok(false),
                    None => Ok(false), // Incomparable types
                }
            }
            ComparisonOp::LessThanOrEqual => {
                match left.partial_cmp(right) {
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => Ok(true),
                    Some(_) => Ok(false),
                    None => Ok(false),
                }
            }
            ComparisonOp::GreaterThan => {
                match left.partial_cmp(right) {
                    Some(std::cmp::Ordering::Greater) => Ok(true),
                    Some(_) => Ok(false),
                    None => Ok(false),
                }
            }
            ComparisonOp::GreaterThanOrEqual => {
                match left.partial_cmp(right) {
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal) => Ok(true),
                    Some(_) => Ok(false),
                    None => Ok(false),
                }
            }
            ComparisonOp::IsNull => Ok(matches!(left, Value::Null)),
            ComparisonOp::IsNotNull => Ok(!matches!(left, Value::Null)),
            ComparisonOp::Like => {
                match (left, right) {
                    (Value::Text(text), Value::Text(pattern)) => {
                        Ok(self.like_match(text, pattern))
                    }
                    _ => Ok(false),
                }
            }
            ComparisonOp::NotLike => {
                match (left, right) {
                    (Value::Text(text), Value::Text(pattern)) => {
                        Ok(!self.like_match(text, pattern))
                    }
                    _ => Ok(true),
                }
            }
            ComparisonOp::In | ComparisonOp::NotIn => {
                Err(DatabaseError::ExecutionError {
                    details: "IN/NOT IN should be handled by InList predicate".to_string(),
                })
            }
        }
    }

    /// Check if two values are equal (with type coercion)
    fn values_equal(&self, left: &Value, right: &Value) -> bool {
        left == right
    }

    /// Simple LIKE pattern matching (supports % and _ wildcards)
    fn like_match(&self, text: &str, pattern: &str) -> bool {
        let regex_pattern = pattern
            .replace('%', ".*")
            .replace('_', ".");
        
        // Simple regex-like matching without external dependencies
        self.simple_pattern_match(text, &regex_pattern)
    }

    /// Simple pattern matching implementation
    fn simple_pattern_match(&self, text: &str, pattern: &str) -> bool {
        // For now, implement basic pattern matching
        // This is a simplified version - a full implementation would use proper regex
        if pattern == ".*" {
            return true;
        }
        
        if pattern.starts_with(".*") && pattern.ends_with(".*") {
            let middle = &pattern[2..pattern.len()-2];
            return text.contains(middle);
        }
        
        if pattern.starts_with(".*") {
            let suffix = &pattern[2..];
            return text.ends_with(suffix);
        }
        
        if pattern.ends_with(".*") {
            let prefix = &pattern[..pattern.len()-2];
            return text.starts_with(prefix);
        }
        
        text == pattern
    }

    /// Get all column names referenced in this predicate
    pub fn get_referenced_columns(&self) -> Vec<String> {
        let mut columns = Vec::new();
        self.collect_columns(&mut columns);
        columns.sort();
        columns.dedup();
        columns
    }

    /// Recursively collect column names from the predicate tree
    fn collect_columns(&self, columns: &mut Vec<String>) {
        match self {
            Predicate::Comparison { column_name, .. } => {
                columns.push(column_name.clone());
            }
            Predicate::InList { column_name, .. } => {
                columns.push(column_name.clone());
            }
            Predicate::Logical { left, right, .. } => {
                left.collect_columns(columns);
                if let Some(right_pred) = right {
                    right_pred.collect_columns(columns);
                }
            }
            Predicate::True | Predicate::False => {}
        }
    }

    /// Validate that all referenced columns exist in the schema
    pub fn validate_against_schema(&self, schema: &TableSchema) -> Result<(), DatabaseError> {
        let referenced_columns = self.get_referenced_columns();
        for column_name in referenced_columns {
            if schema.get_column(&column_name).is_none() {
                return Err(DatabaseError::ColumnNotFound {
                    name: column_name,
                    table: schema.table_name.clone(),
                });
            }
        }
        Ok(())
    }
}

/// Builder for creating complex predicates
pub struct PredicateBuilder {
    predicate: Option<Predicate>,
}

impl PredicateBuilder {
    pub fn new() -> Self {
        Self { predicate: None }
    }

    pub fn eq(mut self, column_name: String, value: Value) -> Self {
        let pred = Predicate::eq(column_name, value);
        self.predicate = Some(self.combine_with_and(pred));
        self
    }

    pub fn ne(mut self, column_name: String, value: Value) -> Self {
        let pred = Predicate::ne(column_name, value);
        self.predicate = Some(self.combine_with_and(pred));
        self
    }

    pub fn lt(mut self, column_name: String, value: Value) -> Self {
        let pred = Predicate::lt(column_name, value);
        self.predicate = Some(self.combine_with_and(pred));
        self
    }

    pub fn le(mut self, column_name: String, value: Value) -> Self {
        let pred = Predicate::le(column_name, value);
        self.predicate = Some(self.combine_with_and(pred));
        self
    }

    pub fn gt(mut self, column_name: String, value: Value) -> Self {
        let pred = Predicate::gt(column_name, value);
        self.predicate = Some(self.combine_with_and(pred));
        self
    }

    pub fn ge(mut self, column_name: String, value: Value) -> Self {
        let pred = Predicate::ge(column_name, value);
        self.predicate = Some(self.combine_with_and(pred));
        self
    }

    pub fn is_null(mut self, column_name: String) -> Self {
        let pred = Predicate::is_null(column_name);
        self.predicate = Some(self.combine_with_and(pred));
        self
    }

    pub fn is_not_null(mut self, column_name: String) -> Self {
        let pred = Predicate::is_not_null(column_name);
        self.predicate = Some(self.combine_with_and(pred));
        self
    }

    pub fn in_list(mut self, column_name: String, values: Vec<Value>) -> Self {
        let pred = Predicate::in_list(column_name, values);
        self.predicate = Some(self.combine_with_and(pred));
        self
    }

    pub fn or(mut self, other_predicate: Predicate) -> Self {
        self.predicate = Some(match self.predicate {
            Some(existing) => Predicate::or(existing, other_predicate),
            None => other_predicate,
        });
        self
    }

    pub fn build(self) -> Predicate {
        self.predicate.unwrap_or(Predicate::True)
    }

    fn combine_with_and(&self, new_predicate: Predicate) -> Predicate {
        match &self.predicate {
            Some(existing) => Predicate::and(existing.clone(), new_predicate),
            None => new_predicate,
        }
    }
}

impl Default for PredicateBuilder {
    fn default() -> Self {
        Self::new()
    }
}