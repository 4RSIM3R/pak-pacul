## Project Overview

Bambang DB SQLite-like embedded database in Rust with B+ tree indexing, slotted page storage, and parallel scanning capabilities. The project follows production-grade and best practices also latest api updates.

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

## Types Module

Type will held some type such as, add description that will make LLM agent efficiently understand and extend knowledge
if needed to folder `types`

## Planner Module



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