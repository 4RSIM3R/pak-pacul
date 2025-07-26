pub mod error;
pub mod page;
pub mod row;
pub mod value;

// Common type aliases
pub type PageId = u64;
pub type RowId = u64;
pub type TransactionId = u64;
pub type ColumnId = u32;

// Constants following SQLite specifications
pub const PAGE_SIZE: usize = 4096;
pub const MAX_PAGE_COUNT: u64 = 1099511627775; // 2^40 - 1 (SQLite limit)
pub const HEADER_SIZE: usize = 100; // Database header size
pub const PAGE_HEADER_SIZE: usize = 12; // Per-page header
