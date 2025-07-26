use crate::types::{PAGE_HEADER_SIZE, PAGE_SIZE, PageId, RowId, error::DatabaseError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PageType {
    InteriorIndex = 2,
    InteriorTable = 5,
    LeafIndex = 10,
    LeafTable = 13,
}

impl PageType {
    pub fn from_u8(value: u8) -> Result<Self, DatabaseError> {
        match value {
            2 => Ok(PageType::InteriorIndex),
            5 => Ok(PageType::InteriorTable),
            10 => Ok(PageType::LeafIndex),
            13 => Ok(PageType::LeafTable),
            _ => Err(DatabaseError::InvalidPageType(value)),
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            PageType::InteriorIndex => 2,
            PageType::InteriorTable => 5,
            PageType::LeafIndex => 10,
            PageType::LeafTable => 13,
        }
    }
}

pub const SLOT_DIRECTORY_ENTRY_SIZE: usize = 4; // offset (2 bytes) + length (2 bytes)

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlotEntry {
    pub offset: u16, // Offset from beginning of page
    pub length: u16, // Length of the cell
    pub row_id: Option<RowId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlotDirectory {
    pub slots: Vec<SlotEntry>,
}

/*
 * Page Layout on Disk (Slotted Page Structure)
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                    PAGE HEADER (32 bytes)                       │
 * │  page_id(8) | page_type(1) | parent_id(8) | next_leaf(8) |     │
 * │  cell_count(2) | free_space_offset(2) | reserved(3)            │
 * ├─────────────────────────────────────────────────────────────────┤
 * │                  SLOT DIRECTORY                                 │
 * │  [slot0: offset(2)|len(2)] [slot1: offset(2)|len(2)] ...       │
 * ├─────────────────────────────────────────────────────────────────┤
 * │                    FREE SPACE                                   │
 * │                                                                 │
 * ├─────────────────────────────────────────────────────────────────┤
 * │                   CELL DATA                                     │
 * │  [...cell N...] [...cell 2...] [...cell 1...] [...cell 0...]   │
 * └─────────────────────────────────────────────────────────────────┘
 */

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

impl Page {
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        Self {
            page_id,
            page_type,
            parent_page_id: None,
            next_leaf_page_id: None,
            is_dirty: false,
            slot_directory: SlotDirectory { slots: Vec::new() },
            free_space_offset: PAGE_SIZE as u16,
            cell_count: 0,
            data: vec![0; PAGE_SIZE],
        }
    }

    /// Serialize the page to bytes following the documented layout
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = vec![0u8; PAGE_SIZE];
        let mut offset = 0;

        // Write PAGE HEADER (32 bytes total)
        // page_id (8 bytes)
        buffer[offset..offset + 8].copy_from_slice(&self.page_id.to_le_bytes());
        offset += 8;

        // page_type (1 byte)
        buffer[offset] = self.page_type.as_u8();
        offset += 1;

        // parent_page_id (8 bytes) - u64::MAX represents None
        let parent_id = self.parent_page_id.unwrap_or(u64::MAX);
        buffer[offset..offset + 8].copy_from_slice(&parent_id.to_le_bytes());
        offset += 8;

        // next_leaf_page_id (8 bytes) - u64::MAX represents None
        let next_leaf_id = self.next_leaf_page_id.unwrap_or(u64::MAX);
        buffer[offset..offset + 8].copy_from_slice(&next_leaf_id.to_le_bytes());
        offset += 8;

        // cell_count (2 bytes)
        buffer[offset..offset + 2].copy_from_slice(&self.cell_count.to_le_bytes());
        offset += 2;

        // free_space_offset (2 bytes)
        buffer[offset..offset + 2].copy_from_slice(&self.free_space_offset.to_le_bytes());
        offset += 2;

        // reserved space (3 bytes) - pad to PAGE_HEADER_SIZE
        // offset is now 29, we need 3 more bytes to reach 32
        offset = PAGE_HEADER_SIZE;

        // Write SLOT DIRECTORY
        for slot in &self.slot_directory.slots {
            // Each slot entry: offset(2) + length(2) = 4 bytes
            buffer[offset..offset + 2].copy_from_slice(&slot.offset.to_le_bytes());
            offset += 2;
            buffer[offset..offset + 2].copy_from_slice(&slot.length.to_le_bytes());
            offset += 2;
        }

        // Copy CELL DATA from our data buffer
        // The cell data is already properly positioned in self.data
        buffer[self.free_space_offset as usize..]
            .copy_from_slice(&self.data[self.free_space_offset as usize..]);

        buffer
    }

    /// Deserialize a page from bytes following the documented layout
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DatabaseError> {
        if bytes.len() != PAGE_SIZE {
            return Err(DatabaseError::InvalidPageSize {
                expected: PAGE_SIZE,
                actual: bytes.len(),
            });
        }

        let mut offset = 0;

        // Read PAGE HEADER
        // page_id (8 bytes)
        let page_id = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // page_type (1 byte)
        let page_type = PageType::from_u8(bytes[offset])?;
        offset += 1;

        // parent_page_id (8 bytes)
        let parent_id_raw = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        let parent_page_id = if parent_id_raw == u64::MAX {
            None
        } else {
            Some(parent_id_raw)
        };
        offset += 8;

        // next_leaf_page_id (8 bytes)
        let next_leaf_id_raw = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        let next_leaf_page_id = if next_leaf_id_raw == u64::MAX {
            None
        } else {
            Some(next_leaf_id_raw)
        };
        offset += 8;

        // cell_count (2 bytes)
        let cell_count = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
        offset += 2;

        // free_space_offset (2 bytes)
        let free_space_offset = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
        offset += 2;

        // Skip reserved space to reach end of header
        offset = PAGE_HEADER_SIZE;

        // Read SLOT DIRECTORY
        let mut slots = Vec::with_capacity(cell_count as usize);
        for _ in 0..cell_count {
            if offset + SLOT_DIRECTORY_ENTRY_SIZE > bytes.len() {
                return Err(DatabaseError::CorruptedPage {
                    page_id,
                    reason: "Slot directory extends beyond page boundary".to_string(),
                });
            }

            let slot_offset = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
            offset += 2;
            let length = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
            offset += 2;

            // Validate slot bounds
            if slot_offset as usize + length as usize > PAGE_SIZE {
                return Err(DatabaseError::CorruptedPage {
                    page_id,
                    reason: format!(
                        "Slot at offset {} with length {} exceeds page boundary",
                        slot_offset, length
                    ),
                });
            }

            slots.push(SlotEntry {
                offset: slot_offset,
                length,
                row_id: None, // row_id is not stored on disk, will be set by higher-level code
            });
        }

        // Validate free_space_offset
        if free_space_offset as usize > PAGE_SIZE {
            return Err(DatabaseError::CorruptedPage {
                page_id,
                reason: format!("Invalid free_space_offset: {}", free_space_offset),
            });
        }

        // Copy entire page data
        let mut data = vec![0u8; PAGE_SIZE];
        data.copy_from_slice(bytes);

        Ok(Page {
            page_id,
            page_type,
            parent_page_id,
            next_leaf_page_id,
            is_dirty: false, // Freshly loaded page is not dirty
            slot_directory: SlotDirectory { slots },
            free_space_offset,
            cell_count,
            data,
        })
    }

    // Existing methods remain unchanged...
    pub fn available_space(&self) -> usize {
        let slot_directory_size = self.slot_directory.slots.len() * SLOT_DIRECTORY_ENTRY_SIZE;
        let used_data_space = (PAGE_SIZE as u16 - self.free_space_offset) as usize;
        PAGE_SIZE - PAGE_HEADER_SIZE - slot_directory_size - used_data_space
    }

    pub fn can_fit(&self, data_size: usize) -> bool {
        self.available_space() >= data_size + SLOT_DIRECTORY_ENTRY_SIZE
    }

    pub fn insert_cell(
        &mut self,
        data: &[u8],
        row_id: Option<RowId>,
    ) -> Result<usize, DatabaseError> {
        if !self.can_fit(data.len()) {
            return Err(DatabaseError::PageFull {
                page_id: self.page_id,
            });
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

    pub fn delete_cell(&mut self, slot_index: usize) -> Result<(), DatabaseError> {
        if slot_index >= self.slot_directory.slots.len() {
            return Err(DatabaseError::InvalidSlotIndex {
                index: slot_index,
                max: self.slot_directory.slots.len(),
            });
        }

        // Remove slot entry
        self.slot_directory.slots.remove(slot_index);
        self.cell_count -= 1;
        self.is_dirty = true;

        self.compact_page();

        Ok(())
    }

    // Defragment the page by moving all cells to eliminate gaps
    fn compact_page(&mut self) {
        if self.slot_directory.slots.is_empty() {
            self.free_space_offset = PAGE_SIZE as u16;
            return;
        }

        // temporary buffer to hold compacted cell data
        let mut compacted_data = Vec::new();
        let mut new_offsets = Vec::new();

        // Sort slots by their current offset (highest to lowest) to maintain order
        let mut sorted_slots: Vec<(usize, &SlotEntry)> =
            self.slot_directory.slots.iter().enumerate().collect();
        sorted_slots.sort_by(|a, b| b.1.offset.cmp(&a.1.offset));

        // Copy cell data in order and calculate new offsets
        let mut current_offset = PAGE_SIZE as u16;
        for (original_index, slot) in sorted_slots {
            let cell_data = self.get_cell_by_offset(slot.offset, slot.length);
            compacted_data.extend_from_slice(cell_data);

            current_offset -= slot.length;
            new_offsets.push((original_index, current_offset));
        }

        // Update the data buffer with compacted cells
        let data_start = current_offset as usize;
        self.data[data_start..PAGE_SIZE].copy_from_slice(&compacted_data);

        // Clear the space that's now free
        self.data[PAGE_HEADER_SIZE..data_start].fill(0);

        // Update slot offsets in their original order
        for (original_index, new_offset) in new_offsets {
            self.slot_directory.slots[original_index].offset = new_offset;
        }

        // Update free space offset
        self.free_space_offset = current_offset;
    }

    fn get_cell_by_offset(&self, offset: u16, length: u16) -> &[u8] {
        let start = offset as usize;
        let end = start + length as usize;
        &self.data[start..end]
    }
}
