## Project Overview

Bambang DB SQLite-like embedded database in Rust with B+ tree indexing, slotted page storage, and parallel scanning capabilities. The project follows production-grade patterns while maintaining educational clarity.

## Project Structure

```
src/
├── lib.rs
├── art/
│   ├── mod.rs
├── types/
│   ├── mod.rs
│   ├── row.rs
│   ├── value.rs
│   ├── page.rs
│   ├── schema.rs
│   └── error.rs
├── storage/
│   ├── mod.rs
│   ├── buffer_pool.rs
│   ├── disk_manager.rs
│   └── btree.rs
├── planner/
│   ├── mod.rs
│   ├── logical_plan.rs
│   └── expression.rs
├── optimizer/
│   ├── mod.rs
│   ├── rules.rs
│   └── cost_model.rs
├── executor/
│   ├── mod.rs
│   ├── scan.rs
│   ├── insert.rs
│   ├── update.rs
│   ├── delete.rs
│   ├── join.rs
│   └── aggregate.rs
├── catalog/
│   ├── mod.rs
│   ├── schema_manager.rs
│   └── table_info.rs
└── main.rs
```

## Dependencies (Cargo.toml)

```toml
[package]
name = "mini-database"
version = "0.1.0"
edition = "2021"

[dependencies]
# SQL parsing
sqlparser = "0.45"
datafusion-sql = "36.0"

# Async runtime and concurrency
tokio = { version = "1.0", features = ["full"] }
rayon = "1.8"
crossbeam = "0.8"

# Serialization
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"

# Error handling
anyhow = "1.0"
thiserror = "1.0"

# Utilities
bytes = "1.5"
memmap2 = "0.9"
parking_lot = "0.12"

# Testing and benchmarking
criterion = { version = "0.5", features = ["html_reports"] }

[dev-dependencies]
tempfile = "3.8"
proptest = "1.4"
```

## Core Types Module

Type will held some type such as, add description that will make LLM agent efficiently understand and extend knowledge
if needed to folder `types`

## Planner Module

### planner/logical_plan.rs
```rust
use sqlparser::ast::*;
use crate::types::{Value, DataType};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    Scan(ScanPlan),
    Filter(FilterPlan),
    Project(ProjectPlan),
    Join(JoinPlan),
    Aggregate(AggregatePlan),
    Insert(InsertPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    CreateTable(CreateTablePlan),
    DropTable(DropTablePlan),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScanPlan {
    pub table_name: String,
    pub alias: Option<String>,
    pub projected_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterPlan {
    pub input: Box<LogicalPlan>,
    pub predicate: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectPlan {
    pub input: Box<LogicalPlan>,
    pub expressions: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinPlan {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinType,
    pub condition: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregatePlan {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<Expression>,
    pub aggregates: Vec<AggregateExpression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpression {
    pub function: AggregateFunction,
    pub expression: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertPlan {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdatePlan {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub condition: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeletePlan {
    pub table_name: String,
    pub condition: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTablePlan {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTablePlan {
    pub table_name: String,
    pub if_exists: bool,
}
```

### planner/expression.rs
```rust
use crate::types::Value;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Literal(Value),
    Column(ColumnRef),
    BinaryOp(BinaryOpExpression),
    UnaryOp(UnaryOpExpression),
    Function(FunctionExpression),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinaryOpExpression {
    pub left: Box<Expression>,
    pub operator: BinaryOperator,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    
    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    
    // Logical
    And,
    Or,
    
    // String
    Like,
    NotLike,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnaryOpExpression {
    pub operator: UnaryOperator,
    pub expression: Box<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionExpression {
    pub name: String,
    pub args: Vec<Expression>,
}

impl Expression {
    // Built-in functions
    pub fn upper(expr: Expression) -> Self {
        Expression::Function(FunctionExpression {
            name: "UPPER".to_string(),
            args: vec![expr],
        })
    }

    pub fn lower(expr: Expression) -> Self {
        Expression::Function(FunctionExpression {
            name: "LOWER".to_string(),
            args: vec![expr],
        })
    }
    
    // Helper constructors
    pub fn column(name: &str) -> Self {
        Expression::Column(ColumnRef {
            table: None,
            column: name.to_string(),
        })
    }
    
    pub fn literal(value: Value) -> Self {
        Expression::Literal(value)
    }
    
    pub fn eq(left: Expression, right: Expression) -> Self {
        Expression::BinaryOp(BinaryOpExpression {
            left: Box::new(left),
            operator: BinaryOperator::Equal,
            right: Box::new(right),
        })
    }
}
```

## Best Practices Implementation Notes

### Memory Management
- Use `Arc<T>` for shared ownership of immutable data
- Use `Rc<RefCell<T>>` for single-threaded mutable sharing
- Implement `Drop` trait for proper resource cleanup

### Error Handling

will rely on 

### Concurrency Patterns
- Use `tokio::sync::RwLock` for async read-write locks
- Use `crossbeam::channel` for thread communication
- Implement work-stealing with `rayon::ThreadPool`

### Testing Strategy

will mimick some env spec

## Performance Optimization Guidelines

1. **Buffer Pool Management**: Implement LRU eviction with efficient hash map lookups
2. **B+ Tree Optimizations**: Use copy-on-write for concurrent access
3. **Parallel Scanning**: Implement work-stealing with configurable thread pool size
4. **Memory Layout**: Align data structures to cache line boundaries
5. **I/O Optimization**: Use memory-mapped files with proper prefetching

## SQLite Compatibility Notes

- Follow SQLite's page format specification
- Implement similar type affinity rules
- Support SQLite's collation sequences
- Maintain compatibility with basic SQL syntax

This implementation provides a solid foundation for your undergraduate thesis while following production-grade patterns.