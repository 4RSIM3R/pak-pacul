use crate::{
    storage::{BAMBANG_HEADER_SIZE, BAMBANG_MAGIC},
    types::{PAGE_SIZE, error::DatabaseError},
};

#[derive(Debug)]
pub struct BambangHeader {
    pub magic: [u8; 16],
    pub page_size: u16,
    pub file_format_write_version: u8,
    pub file_format_read_version: u8,
    pub reserved_space: u8,
    pub max_embedded_payload_fraction: u8,
    pub min_embedded_payload_fraction: u8,
    pub leaf_payload_fraction: u8,
    pub file_change_counter: u32,
    pub database_size_pages: u32,
    pub freelist_trunk_page: u32,
    pub freelist_pages_count: u32,
    pub schema_cookie: u32,
    pub schema_format_number: u32,
    pub default_page_cache_size: u32,
    pub largest_root_btree_page: u32,
    pub text_encoding: u32,
    pub user_version: u32,
    pub incremental_vacuum_mode: u32,
    pub application_id: u32,
    pub reserved: [u8; 20],
    pub version_valid_for: u32,
    pub bambang_version_number: u32,
}

impl Default for BambangHeader {
    fn default() -> Self {
        Self {
            magic: *BAMBANG_MAGIC,
            page_size: PAGE_SIZE as u16,
            file_format_write_version: 1,
            file_format_read_version: 1,
            reserved_space: 0,
            max_embedded_payload_fraction: 64,
            min_embedded_payload_fraction: 32,
            leaf_payload_fraction: 32,
            file_change_counter: 1,
            database_size_pages: 1,
            freelist_trunk_page: 0,
            freelist_pages_count: 0,
            schema_cookie: 1,
            schema_format_number: 4,
            default_page_cache_size: 0,
            largest_root_btree_page: 1,
            text_encoding: 1,
            user_version: 0,
            incremental_vacuum_mode: 0,
            application_id: 0,
            reserved: [0; 20],
            version_valid_for: 1,
            bambang_version_number: 0001000,
        }
    }
}

impl BambangHeader {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(BAMBANG_HEADER_SIZE);

        buffer.extend_from_slice(&self.magic);
        buffer.extend_from_slice(&self.page_size.to_be_bytes());
        buffer.push(self.file_format_write_version);
        buffer.push(self.file_format_read_version);
        buffer.push(self.reserved_space);
        buffer.push(self.max_embedded_payload_fraction);
        buffer.push(self.min_embedded_payload_fraction);
        buffer.push(self.leaf_payload_fraction);
        buffer.extend_from_slice(&self.file_change_counter.to_be_bytes());
        buffer.extend_from_slice(&self.database_size_pages.to_be_bytes());
        buffer.extend_from_slice(&self.freelist_trunk_page.to_be_bytes());
        buffer.extend_from_slice(&self.freelist_pages_count.to_be_bytes());
        buffer.extend_from_slice(&self.schema_cookie.to_be_bytes());
        buffer.extend_from_slice(&self.schema_format_number.to_be_bytes());
        buffer.extend_from_slice(&self.default_page_cache_size.to_be_bytes());
        buffer.extend_from_slice(&self.largest_root_btree_page.to_be_bytes());
        buffer.extend_from_slice(&self.text_encoding.to_be_bytes());
        buffer.extend_from_slice(&self.user_version.to_be_bytes());
        buffer.extend_from_slice(&self.incremental_vacuum_mode.to_be_bytes());
        buffer.extend_from_slice(&self.application_id.to_be_bytes());
        buffer.extend_from_slice(&self.reserved);
        buffer.extend_from_slice(&self.version_valid_for.to_be_bytes());
        buffer.extend_from_slice(&self.bambang_version_number.to_be_bytes());

        buffer.resize(BAMBANG_HEADER_SIZE, 0);
        buffer
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DatabaseError> {
        if bytes.len() < BAMBANG_HEADER_SIZE {
            return Err(DatabaseError::InvalidHeader {
                reason: "Header too short".to_string(),
            });
        }

        let mut offset = 0;

        let mut magic = [0u8; 16];
        magic.copy_from_slice(&bytes[offset..offset + 16]);
        if &magic != BAMBANG_MAGIC {
            return Err(DatabaseError::InvalidHeader {
                reason: "Invalid Bambang magic number".to_string(),
            });
        }
        offset += 16;

        let page_size = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
        if page_size != PAGE_SIZE as u16 {
            return Err(DatabaseError::InvalidHeader {
                reason: format!("Unsupported page size: {}", page_size),
            });
        }
        offset += 2;

        let file_format_write_version = bytes[offset];
        offset += 1;
        let file_format_read_version = bytes[offset];
        offset += 1;
        let reserved_space = bytes[offset];
        offset += 1;
        let max_embedded_payload_fraction = bytes[offset];
        offset += 1;
        let min_embedded_payload_fraction = bytes[offset];
        offset += 1;
        let leaf_payload_fraction = bytes[offset];
        offset += 1;

        let file_change_counter = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let database_size_pages = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let freelist_trunk_page = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let freelist_pages_count = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let schema_cookie = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let schema_format_number = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let default_page_cache_size = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let largest_root_btree_page = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let text_encoding = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let user_version = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let incremental_vacuum_mode = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let application_id = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let mut reserved = [0u8; 20];
        reserved.copy_from_slice(&bytes[offset..offset + 20]);
        offset += 20;

        let version_valid_for = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let bambang_version_number = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        Ok(Self {
            magic,
            page_size,
            file_format_write_version,
            file_format_read_version,
            reserved_space,
            max_embedded_payload_fraction,
            min_embedded_payload_fraction,
            leaf_payload_fraction,
            file_change_counter,
            database_size_pages,
            freelist_trunk_page,
            freelist_pages_count,
            schema_cookie,
            schema_format_number,
            default_page_cache_size,
            largest_root_btree_page,
            text_encoding,
            user_version,
            incremental_vacuum_mode,
            application_id,
            reserved,
            version_valid_for,
            bambang_version_number,
        })
    }
}
