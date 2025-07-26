pub mod header;
pub mod storage_manager;

const BAMBANG_HEADER_SIZE: usize = 100;
const BAMBANG_MAGIC: &[u8; 16] = b"BAMBANG DB v0.1\0";
