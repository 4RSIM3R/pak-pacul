# CLAUDE.md - Database Project Implementation Guide

## Project Overview

This guide covers building a SQLite-like embedded database in Rust with B+ tree indexing, slotted page storage, and parallel scanning capabilities. The project follows production-grade patterns while maintaining educational clarity.

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


### types/page.rs
```rust
use serde::{Deserialize, Serialize};
use crate::types::{PageId, RowId, Value};
use std::collections::HashMap;

pub const SLOT_DIRECTORY_ENTRY_SIZE: usize = 4; // offset (2 bytes) + length (2 bytes)

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Page {
    pub page_id: PageId,
    pub page_type: PageType,
    pub parent_page_id: Option<PageId>,
    pub next_leaf_page_id: Option<PageId>,
    pub is_dirty: bool,
    
    // Slotted page structure
    pub slot_directory: SlotDirectory,
    pub free_space_offset: u16,
    pub cell_count: u16,
    
    // Data storage
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PageType {
    InteriorIndex = 2,
    InteriorTable = 5,
    LeafIndex = 10,
    LeafTable = 13,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlotDirectory {
    pub slots: Vec<SlotEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlotEntry {
    pub offset: u16,    // Offset from beginning of page
    pub length: u16,    // Length of the cell
    pub row_id: Option<RowId>,
}

impl Page {
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        Self {
            page_id,
            page_type,
            parent_page_id: None,
            next_leaf_page_id: None,
            is_dirty: false,
            slot_directory: SlotDirectory { slots: Vec::new() },
            free_space_offset: (PAGE_SIZE - PAGE_HEADER_SIZE) as u16,
            cell_count: 0,
            data: vec![0; PAGE_SIZE],
        }
    }

    pub fn available_space(&self) -> usize {
        let slot_directory_size = self.slot_directory.slots.len() * SLOT_DIRECTORY_ENTRY_SIZE;
        let used_data_space = (PAGE_SIZE as u16 - self.free_space_offset) as usize;
        
        PAGE_SIZE - PAGE_HEADER_SIZE - slot_directory_size - used_data_space
    }

    pub fn can_fit(&self, data_size: usize) -> bool {
        self.available_space() >= data_size + SLOT_DIRECTORY_ENTRY_SIZE
    }

    pub fn insert_cell(&mut self, data: &[u8], row_id: Option<RowId>) -> Result<usize, crate::DatabaseError> {
        if !self.can_fit(data.len()) {
            return Err(crate::DatabaseError::PageFull);
        }

        // Calculate new offset for the cell (grows downward)
        let new_offset = self.free_space_offset - data.len() as u16;
        
        // Write data to page
        let start = new_offset as usize;
        let end = start + data.len();
        self.data[start..end].copy_from_slice(data);
        
        // Add slot entry
        let slot_index = self.slot_directory.slots.len();
        self.slot_directory.slots.push(SlotEntry {
            offset: new_offset,
            length: data.len() as u16,
            row_id,
        });
        
        // Update page metadata
        self.free_space_offset = new_offset;
        self.cell_count += 1;
        self.is_dirty = true;
        
        Ok(slot_index)
    }

    pub fn get_cell(&self, slot_index: usize) -> Option<&[u8]> {
        if let Some(slot) = self.slot_directory.slots.get(slot_index) {
            let start = slot.offset as usize;
            let end = start + slot.length as usize;
            Some(&self.data[start..end])
        } else {
            None
        }
    }

    pub fn delete_cell(&mut self, slot_index: usize) -> Result<(), crate::DatabaseError> {
        if slot_index >= self.slot_directory.slots.len() {
            return Err(crate::DatabaseError::InvalidSlotIndex);
        }

        // Remove slot entry
        self.slot_directory.slots.remove(slot_index);
        self.cell_count -= 1;
        self.is_dirty = true;
        
        // Note: This is a simple deletion that doesn't compact the page
        // Production implementation would include defragmentation
        
        Ok(())
    }

    pub fn defragment(&mut self) {
        // Compact the page by moving all cells to eliminate fragmentation
        let mut new_data = vec![0; PAGE_SIZE];
        let mut new_offset = PAGE_SIZE as u16;
        
        for slot in &mut self.slot_directory.slots {
            if slot.length > 0 {
                new_offset -= slot.length;
                let old_start = slot.offset as usize;
                let old_end = old_start + slot.length as usize;
                let new_start = new_offset as usize;
                let new_end = new_start + slot.length as usize;
                
                new_data[new_start..new_end].copy_from_slice(&self.data[old_start..old_end]);
                slot.offset = new_offset;
            }
        }
        
        self.data = new_data;
        self.free_space_offset = new_offset;
        self.is_dirty = true;
    }
}
```

### types/row.rs
```rust
use serde::{Deserialize, Serialize};
use crate::types::{Value, RowId};

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

    pub fn set_value(&mut self, column_index: usize, value: Value) -> Result<(), crate::DatabaseError> {
        if column_index >= self.values.len() {
            return Err(crate::DatabaseError::ColumnIndexOutOfBounds);
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
        bincode::serialize(self).unwrap_or_default()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::DatabaseError> {
        bincode::deserialize(bytes)
            .map_err(|_| crate::DatabaseError::SerializationError)
    }
}
```

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