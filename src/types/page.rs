use std::io::Cursor;

use crate::{
    types::{
        PAGE_HEADER_SIZE, PAGE_SIZE, PageId, RowId, SLOT_DIRECTORY_ENTRY_SIZE, error::DatabaseError,
    },
    utils::hash::{calculate_page_checksum, verify_page_checksum},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PageType {
    InteriorIndex = 2,
    InteriorTable = 5,
    LeafIndex = 10,
    LeafTable = 13,
    OverflowPage = 15,
}

impl PageType {
    pub fn from_u8(value: u8) -> Result<Self, DatabaseError> {
        match value {
            2 => Ok(PageType::InteriorIndex),
            5 => Ok(PageType::InteriorTable),
            10 => Ok(PageType::LeafIndex),
            13 => Ok(PageType::LeafTable),
            15 => Ok(PageType::OverflowPage),
            _ => Err(DatabaseError::InvalidPageType(value)),
        }
    }

    pub const fn as_u8(&self) -> u8 {
        match self {
            PageType::InteriorIndex => 2,
            PageType::InteriorTable => 5,
            PageType::LeafIndex => 10,
            PageType::LeafTable => 13,
            PageType::OverflowPage => 15,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OverflowPointer {
    pub page_id: PageId,
    pub total_size: u32,
}

impl OverflowPointer {
    pub const SERIALIZED_SIZE: usize = 12; // 8 bytes page_id + 4 bytes total_size

    pub fn serialize_to_vec(&self) -> Result<Vec<u8>, DatabaseError> {
        let mut buffer = Vec::with_capacity(Self::SERIALIZED_SIZE);
        buffer.extend_from_slice(&self.page_id.to_le_bytes());
        buffer.extend_from_slice(&self.total_size.to_le_bytes());
        Ok(buffer)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlotEntry {
    pub offset: u16,
    pub length: u16,
    pub row_id: Option<RowId>,
    pub is_overflow: bool,
    pub overflow_pointer: Option<OverflowPointer>,
}

impl SlotEntry {
    pub fn new_regular(offset: u16, length: u16, row_id: Option<RowId>) -> Self {
        Self {
            offset,
            length,
            row_id,
            is_overflow: false,
            overflow_pointer: None,
        }
    }

    pub fn new_overflow(
        offset: u16,
        length: u16,
        row_id: Option<RowId>,
        pointer: OverflowPointer,
    ) -> Self {
        Self {
            offset,
            length,
            row_id,
            is_overflow: true,
            overflow_pointer: Some(pointer),
        }
    }

    /// Calculate absolute file offset for this slot's data
    pub fn absolute_file_offset(&self, page_id: PageId) -> u64 {
        (page_id * PAGE_SIZE as u64) + self.offset as u64
    }

    /// Get the data range for this slot in the file
    pub fn file_range(&self, page_id: PageId) -> (u64, u64) {
        let start = self.absolute_file_offset(page_id);
        let end = start + self.length as u64;
        (start, end)
    }

    /// Check if this slot is deleted (has zero length AND no row_id)
    pub fn is_deleted(&self) -> bool {
        self.length == 0 && self.row_id.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlotDirectory {
    pub slots: Vec<SlotEntry>,
}

impl SlotDirectory {
    pub fn new() -> Self {
        Self { slots: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PageStats {
    pub page_id: PageId,
    pub total_slots: usize,
    pub active_slots: usize,
    pub deleted_slots: usize,
    pub free_space: usize,
    pub used_space: usize,
    pub wasted_space: usize,
    pub fragmentation_ratio: f32,
    pub utilization_ratio: f32,
}

/*
 * Enhanced Page Layout on Disk (Slotted Page Structure)
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                    PAGE HEADER (36 bytes)                       │
 * │  page_id(8) | page_type(1) | parent_id(8) | next_leaf(8) |      │
 * │  cell_count(2) | free_space_offset(2) | checksum(4) |           │
 * │  reserved(3)                                                    │
 * ├─────────────────────────────────────────────────────────────────┤
 * │                  SLOT DIRECTORY                                 │
 * │  [slot0] [slot1] [slot2] ...                                    │
 * ├─────────────────────────────────────────────────────────────────┤
 * │                    FREE SPACE                                   │
 * │                                                                 │
 * ├─────────────────────────────────────────────────────────────────┤
 * │                   CELL DATA                                     │
 * │  [...cell N...] [...cell 2...] [...cell 1...] [...cell 0...]    │
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

    // Optional data - None means metadata-only mode for read-heavy workloads
    pub data: Option<Vec<u8>>,
    pub checksum: u32,
    pub overflow_pages: Vec<PageId>,
}

impl Page {
    /// Create a new empty page with full data
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let mut data = Vec::with_capacity(PAGE_SIZE);
        data.resize(PAGE_SIZE, 0);

        let mut page = Self {
            page_id,
            page_type,
            parent_page_id: None,
            next_leaf_page_id: None,
            is_dirty: false,
            slot_directory: SlotDirectory::new(),
            free_space_offset: PAGE_SIZE as u16,
            cell_count: 0,
            data: Some(data),
            checksum: 0,
            overflow_pages: Vec::new(),
        };
        page.update_checksum();
        page
    }

    /// Create metadata-only page from header + slot directory bytes
    /// Expected: header_bytes.len() >= PAGE_HEADER_SIZE + (cell_count * SLOT_DIRECTORY_ENTRY_SIZE)
    pub fn from_header_bytes(header_bytes: &[u8]) -> Result<Self, DatabaseError> {
        if header_bytes.len() < PAGE_HEADER_SIZE {
            return Err(DatabaseError::InvalidPageSize {
                expected: PAGE_HEADER_SIZE,
                actual: header_bytes.len(),
            });
        }

        // Parse header from first PAGE_HEADER_SIZE bytes
        let (
            page_id,
            page_type,
            parent_page_id,
            next_leaf_page_id,
            cell_count,
            free_space_offset,
            checksum,
        ) = Self::read_header(&header_bytes[..PAGE_HEADER_SIZE])?;

        // Calculate expected size including slot directory
        let expected_size = PAGE_HEADER_SIZE + (cell_count as usize * SLOT_DIRECTORY_ENTRY_SIZE);
        if header_bytes.len() < expected_size {
            return Err(DatabaseError::InvalidPageSize {
                expected: expected_size,
                actual: header_bytes.len(),
            });
        }

        // Parse slot directory from remaining bytes
        let slots = Self::read_slot_directory(&header_bytes[PAGE_HEADER_SIZE..expected_size], cell_count, page_id)?;

        Ok(Page {
            page_id,
            page_type,
            parent_page_id,
            next_leaf_page_id,
            is_dirty: false,
            slot_directory: SlotDirectory { slots },
            free_space_offset,
            cell_count,
            data: None, // Metadata-only mode
            checksum,
            overflow_pages: Vec::new(),
        })
    }

    /// Calculate the exact bytes needed for metadata-only parsing
    pub fn calculate_metadata_size(header_bytes: &[u8]) -> Result<usize, DatabaseError> {
        if header_bytes.len() < PAGE_HEADER_SIZE {
            return Err(DatabaseError::InvalidPageSize {
                expected: PAGE_HEADER_SIZE,
                actual: header_bytes.len(),
            });
        }

        // Extract cell_count from header
        // Based on read_header layout: page_id(8) + page_type(1) + parent_id(8) + next_leaf(8) + cell_count(2) + ...
        // So cell_count is at offset 25
        let cell_count_offset = 8 + 1 + 8 + 8; // 25
        if header_bytes.len() < cell_count_offset + 2 {
            return Err(DatabaseError::InvalidPageSize {
                expected: cell_count_offset + 2,
                actual: header_bytes.len(),
            });
        }

        let cell_count = u16::from_le_bytes([
            header_bytes[cell_count_offset],
            header_bytes[cell_count_offset + 1],
        ]);

        Ok(PAGE_HEADER_SIZE + (cell_count as usize * SLOT_DIRECTORY_ENTRY_SIZE))
    }

    /// Check if page is in metadata-only mode
    pub fn is_metadata_only(&self) -> bool {
        self.data.is_none()
    }

    /// Get memory usage of this page
    pub fn memory_footprint(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let slot_size = self.slot_directory.slots.len() * std::mem::size_of::<SlotEntry>();
        let data_size = if let Some(ref data) = self.data {
            data.capacity()
        } else {
            0
        };
        base_size + slot_size + data_size
    }

    /// Upgrade metadata-only page to full page by loading complete data
    pub fn load_full_data(&mut self, page_data: Vec<u8>) -> Result<(), DatabaseError> {
        if page_data.len() != PAGE_SIZE {
            return Err(DatabaseError::InvalidPageSize {
                expected: PAGE_SIZE,
                actual: page_data.len(),
            });
        }

        self.data = Some(page_data);
        Ok(())
    }

    // Updated checksum methods using utility functions
    pub fn update_checksum(&mut self) {
        self.checksum = calculate_page_checksum(
            self.page_id,
            &self.page_type,
            self.parent_page_id,
            self.next_leaf_page_id,
            self.cell_count,
            self.free_space_offset,
            &self.slot_directory.slots,
            self.data.as_deref(),
            self.free_space_offset as usize,
        );
    }

    pub fn verify_checksum(&self) -> bool {
        verify_page_checksum(
            self.page_id,
            &self.page_type,
            self.parent_page_id,
            self.next_leaf_page_id,
            self.cell_count,
            self.free_space_offset,
            &self.slot_directory.slots,
            self.data.as_deref(),
            self.free_space_offset as usize,
            self.checksum,
        )
    }

    pub fn needs_overflow(&self, data_size: usize) -> bool {
        data_size > (PAGE_SIZE / 4)
    }

    pub fn create_overflow_pointer(
        &mut self,
        data: &[u8],
        overflow_page_id: PageId,
    ) -> OverflowPointer {
        self.overflow_pages.push(overflow_page_id);
        OverflowPointer {
            page_id: overflow_page_id,
            total_size: data.len() as u32,
        }
    }

    pub fn insert_cell_with_overflow(
        &mut self,
        data: &[u8],
        row_id: Option<RowId>,
        overflow_page_id: Option<PageId>,
    ) -> Result<usize, DatabaseError> {
        if self.is_metadata_only() {
            return Err(DatabaseError::SerializationError {
                details: "Insertion requires full page".to_string(),
            });
        }

        let needs_overflow = overflow_page_id.is_some() || self.needs_overflow(data.len());

        if needs_overflow {
            if let Some(overflow_id) = overflow_page_id {
                let overflow_ptr = self.create_overflow_pointer(data, overflow_id);
                let overflow_data = overflow_ptr.serialize_to_vec()?;

                if !self.can_fit(overflow_data.len()) {
                    return Err(DatabaseError::PageFull {
                        page_id: self.page_id,
                    });
                }

                let new_offset = self.free_space_offset - overflow_data.len() as u16;
                let start = new_offset as usize;
                let end = start + overflow_data.len();

                if let Some(ref mut data) = self.data {
                    data[start..end].copy_from_slice(&overflow_data);
                }

                let slot_index = self.slot_directory.slots.len();
                self.slot_directory.slots.push(SlotEntry::new_overflow(
                    new_offset,
                    overflow_data.len() as u16,
                    row_id,
                    overflow_ptr,
                ));

                self.free_space_offset = new_offset;
                self.cell_count = self.slot_directory.slots.len() as u16; // FIX: Keep in sync
                self.is_dirty = true;
                self.update_checksum();

                return Ok(slot_index);
            } else {
                return Err(DatabaseError::OverflowPageRequired);
            }
        }

        self.insert_cell(data, row_id)
    }

    pub fn get_cell(&self, slot_index: usize) -> Option<&[u8]> {
        if let Some(ref data) = self.data {
            if let Some(slot) = self.slot_directory.slots.get(slot_index) {
                // FIX: Check if slot is truly deleted (length == 0 AND row_id is None)
                // Zero-length data with a valid row_id should be allowed
                if slot.length == 0 && slot.row_id.is_none() {
                    return None; // This is a deleted slot
                }
                let start = slot.offset as usize;
                let end = start + slot.length as usize;
                // FIX: Add bounds checking
                if end <= data.len() {
                    Some(&data[start..end])
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None // Metadata-only mode - use load_cell_lazy instead
        }
    }

    pub fn available_space(&self) -> usize {
        let slot_directory_size = self.slot_directory.slots.len() * SLOT_DIRECTORY_ENTRY_SIZE;
        let used_data_space = (PAGE_SIZE as u16 - self.free_space_offset) as usize;
        PAGE_SIZE.saturating_sub(PAGE_HEADER_SIZE + slot_directory_size + used_data_space)
    }

    pub fn can_fit(&self, data_size: usize) -> bool {
        self.available_space() >= data_size + SLOT_DIRECTORY_ENTRY_SIZE
    }

    pub fn insert_cell(
        &mut self,
        data: &[u8],
        row_id: Option<RowId>,
    ) -> Result<usize, DatabaseError> {
        if self.is_metadata_only() {
            return Err(DatabaseError::SerializationError {
                details: "Insertion requires full page".to_string(),
            });
        }

        if !self.can_fit(data.len()) {
            return Err(DatabaseError::PageFull {
                page_id: self.page_id,
            });
        }

        let new_offset = self.free_space_offset - data.len() as u16;

        if let Some(ref mut page_data) = self.data {
            let start = new_offset as usize;
            let end = start + data.len();
            page_data[start..end].copy_from_slice(data);
        }

        let slot_index = self.slot_directory.slots.len();
        self.slot_directory.slots.push(SlotEntry::new_regular(
            new_offset,
            data.len() as u16,
            row_id,
        ));

        self.free_space_offset = new_offset;
        self.cell_count = self.slot_directory.slots.len() as u16; // FIX: Keep in sync
        self.is_dirty = true;
        self.update_checksum();

        Ok(slot_index)
    }

    /// Delete a cell at the specified slot index
    /// This marks the slot as deleted but doesn't immediately reclaim space
    pub fn delete_cell(&mut self, slot_index: usize) -> Result<(), DatabaseError> {
        if self.is_metadata_only() {
            return Err(DatabaseError::SerializationError { 
                details: "Deletion requires full page".to_string() 
            });
        }

        if slot_index >= self.slot_directory.slots.len() {
            return Err(DatabaseError::InvalidSlotIndex {
                index: slot_index,
                max: self.slot_directory.slots.len(),
            });
        }

        // FIX: Check if slot is already deleted
        if self.slot_directory.slots[slot_index].is_deleted() {
            return Err(DatabaseError::InvalidSlotIndex {
                index: slot_index,
                max: self.slot_directory.slots.len(),
            });
        }

        // Mark the slot as deleted by setting length to 0
        // We keep the slot entry to maintain slot indices for other operations
        self.slot_directory.slots[slot_index].length = 0;
        self.slot_directory.slots[slot_index].offset = 0;
        self.slot_directory.slots[slot_index].row_id = None;
        
        // FIX: Clean up overflow information
        if self.slot_directory.slots[slot_index].is_overflow {
            if let Some(overflow_ptr) = &self.slot_directory.slots[slot_index].overflow_pointer {
                // Remove from overflow_pages list
                self.overflow_pages.retain(|&page_id| page_id != overflow_ptr.page_id);
            }
        }
        
        self.slot_directory.slots[slot_index].is_overflow = false;
        self.slot_directory.slots[slot_index].overflow_pointer = None;

        self.is_dirty = true;
        self.update_checksum();

        // Note: Removed auto-compaction to allow fragmentation testing
        // Users can call compact() manually when needed

        Ok(())
    }

    /// Update a cell's data at the specified slot index
    /// If the new data doesn't fit in the same location, the page will be compacted
    pub fn update_cell(
        &mut self,
        slot_index: usize,
        new_data: &[u8],
        row_id: Option<RowId>,
    ) -> Result<(), DatabaseError> {
        if self.is_metadata_only() {
            return Err(DatabaseError::SerializationError { 
                details: "Update requires full page".to_string() 
            });
        }

        if slot_index >= self.slot_directory.slots.len() {
            return Err(DatabaseError::InvalidSlotIndex {
                index: slot_index,
                max: self.slot_directory.slots.len(),
            });
        }

        let slot = &self.slot_directory.slots[slot_index];

        // Check if slot is already deleted
        if slot.is_deleted() {
            return Err(DatabaseError::InvalidSlotIndex {
                index: slot_index,
                max: self.slot_directory.slots.len(),
            });
        }

        let old_length = slot.length as usize;
        let new_length = new_data.len();

        // Case 1: New data fits exactly in the same space
        if new_length == old_length {
            if let Some(ref mut page_data) = self.data {
                let start = slot.offset as usize;
                let end = start + new_length;
                // FIX: Add bounds checking
                if end <= page_data.len() {
                    page_data[start..end].copy_from_slice(new_data);
                } else {
                    return Err(DatabaseError::CorruptedPage {
                        page_id: self.page_id,
                        reason: "Invalid slot offset/length".to_string(),
                    });
                }
            }

            self.slot_directory.slots[slot_index].row_id = row_id;
            self.is_dirty = true;
            self.update_checksum();
            return Ok(());
        }

        // Case 2: New data is smaller - we can update in place but will create fragmentation
        if new_length < old_length {
            if let Some(ref mut page_data) = self.data {
                let start = slot.offset as usize;
                // FIX: Add bounds checking
                if start + old_length <= page_data.len() {
                    page_data[start..start + new_length].copy_from_slice(new_data);
                    // Zero out the remaining space to avoid data leakage
                    page_data[start + new_length..start + old_length].fill(0);
                } else {
                    return Err(DatabaseError::CorruptedPage {
                        page_id: self.page_id,
                        reason: "Invalid slot offset/length".to_string(),
                    });
                }
            }

            self.slot_directory.slots[slot_index].length = new_length as u16;
            self.slot_directory.slots[slot_index].row_id = row_id;
            self.is_dirty = true;
            self.update_checksum();

            // Only compact if fragmentation is very high to avoid changing offsets unnecessarily
            if self.get_fragmentation_ratio() > 0.5 {
                self.compact()?;
            }
            return Ok(());
        }

        // Case 3: New data is larger - need to relocate or compact
        // First, try to see if we have enough free space after compaction
        let current_free_space = self.available_space();
        let space_gained_from_deletion = old_length;
        let net_space_needed = new_length.saturating_sub(old_length);

        if current_free_space + space_gained_from_deletion < new_length {
            return Err(DatabaseError::PageFull {
                page_id: self.page_id,
            });
        }

        // Delete the old cell and compact to reclaim space
        self.slot_directory.slots[slot_index].length = 0;
        self.slot_directory.slots[slot_index].offset = 0;
        self.compact()?;

        // Now insert the new data in the freed space
        let new_offset = self.free_space_offset - new_data.len() as u16;

        if let Some(ref mut page_data) = self.data {
            let start = new_offset as usize;
            let end = start + new_data.len();
            page_data[start..end].copy_from_slice(new_data);
        }

        // Update the slot entry
        self.slot_directory.slots[slot_index] =
            SlotEntry::new_regular(new_offset, new_data.len() as u16, row_id);

        self.free_space_offset = new_offset;
        self.is_dirty = true;
        self.update_checksum();

        Ok(())
    }

    /// Compact the page to eliminate fragmentation
    /// This moves all active cells to the end of the page, removing gaps
    pub fn compact(&mut self) -> Result<(), DatabaseError> {
        if self.is_metadata_only() {
            return Err(DatabaseError::SerializationError { 
                details: "Compaction requires full page".to_string() 
            });
        }

        let Some(ref mut page_data) = self.data else {
            return Err(DatabaseError::SerializationError { 
                details: "Compaction requires full page".to_string() 
            });
        };

        // Collect all active (non-deleted) cells with their data
        let mut active_cells: Vec<(usize, Vec<u8>, SlotEntry)> = Vec::new();

        for (slot_index, slot) in self.slot_directory.slots.iter().enumerate() {
            if !slot.is_deleted() {
                let start = slot.offset as usize;
                let end = start + slot.length as usize;
                // FIX: Add bounds checking
                if end <= page_data.len() {
                    let cell_data = page_data[start..end].to_vec();
                    active_cells.push((slot_index, cell_data, slot.clone()));
                }
            }
        }

        // FIX: Don't sort by offset - maintain logical order for better cache locality
        // active_cells.sort_by_key(|(_, _, slot)| slot.offset);

        // Clear the data area that will be rewritten
        let data_start = self.free_space_offset as usize;
        if data_start < page_data.len() {
            page_data[data_start..].fill(0);
        }

        // Rewrite cells from the end of the page backwards
        let mut new_free_space_offset = PAGE_SIZE as u16;

        for (slot_index, cell_data, mut slot_entry) in active_cells.into_iter().rev() {
            let cell_size = cell_data.len();
            new_free_space_offset -= cell_size as u16;

            let start = new_free_space_offset as usize;
            let end = start + cell_size;

            // Copy the cell data to its new location
            if end <= page_data.len() {
                page_data[start..end].copy_from_slice(&cell_data);
            }

            // Update the slot entry with new offset
            slot_entry.offset = new_free_space_offset;
            self.slot_directory.slots[slot_index] = slot_entry;
        }

        // FIX: Don't remove deleted slots - just keep them marked as deleted
        // This maintains slot index stability
        // self.slot_directory.slots.retain(|slot| !slot.is_deleted());
        // self.cell_count = self.slot_directory.slots.len() as u16;

        self.free_space_offset = new_free_space_offset;
        self.is_dirty = true;
        self.update_checksum();

        Ok(())
    }

    /// Check if a slot is deleted (has zero length)
    pub fn is_slot_deleted(&self, slot_index: usize) -> bool {
        self.slot_directory
            .slots
            .get(slot_index)
            .map(|slot| slot.is_deleted())
            .unwrap_or(true) // Return true for out-of-bounds indices
    }

    /// Get the number of active (non-deleted) cells
    pub fn active_cell_count(&self) -> usize {
        self.slot_directory
            .slots
            .iter()
            .filter(|slot| !slot.is_deleted())
            .count()
    }

    /// Get statistics about the page
    pub fn get_page_stats(&self) -> PageStats {
        let total_slots = self.slot_directory.slots.len();
        let active_slots = self.active_cell_count();
        let deleted_slots = total_slots - active_slots;

        let active_cell_data_size: usize = self
            .slot_directory
            .slots
            .iter()
            .filter(|slot| !slot.is_deleted())
            .map(|slot| slot.length as usize)
            .sum();

        // Calculate wasted space: space used by deleted cells
        let deleted_cell_data_size: usize = self
            .slot_directory
            .slots
            .iter()
            .filter(|slot| slot.is_deleted())
            .map(|slot| {
                // For deleted slots, we need to estimate the space they previously occupied
                // Since we zero out the length on deletion, we'll use a heuristic
                // In a real implementation, we'd track this better
                if slot.offset > 0 {
                    // Estimate based on typical cell size or use a minimum
                    100 // Assume deleted cells were around 100 bytes
                } else {
                    0
                }
            })
            .sum();

        let total_used_space = (PAGE_SIZE as u16 - self.free_space_offset) as usize;
        let wasted_space = total_used_space.saturating_sub(active_cell_data_size);

        PageStats {
            page_id: self.page_id,
            total_slots,
            active_slots,
            deleted_slots,
            free_space: self.available_space(),
            used_space: PAGE_SIZE - self.available_space(),
            wasted_space,
            fragmentation_ratio: self.get_fragmentation_ratio(),
            utilization_ratio: self.get_utilization_ratio(),
        }
    }

    pub fn get_fragmentation_ratio(&self) -> f32 {
        if self.slot_directory.slots.is_empty() {
            return 0.0;
        }

        let active_data_size: usize = self
            .slot_directory
            .slots
            .iter()
            .filter(|slot| !slot.is_deleted())
            .map(|slot| slot.length as usize)
            .sum();

        let used_space = PAGE_SIZE - self.free_space_offset as usize;

        if used_space == 0 || active_data_size == 0 {
            0.0
        } else {
            // FIX: Calculate fragmentation as wasted space ratio
            let wasted_space = used_space - active_data_size;
            wasted_space as f32 / used_space as f32
        }
    }

    pub fn get_utilization_ratio(&self) -> f32 {
        let used_space = PAGE_SIZE - self.free_space_offset as usize;
        let available_space = PAGE_SIZE - PAGE_HEADER_SIZE;
        used_space as f32 / available_space as f32
    }

    // Helper methods
    fn read_header(
        bytes: &[u8],
    ) -> Result<
        (
            PageId,
            PageType,
            Option<PageId>,
            Option<PageId>,
            u16,
            u16,
            u32,
        ),
        DatabaseError,
    > {
        let mut offset = 0;

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

        let page_type = PageType::from_u8(bytes[offset])?;
        offset += 1;

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

        let cell_count = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
        offset += 2;

        let free_space_offset = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
        offset += 2;

        let checksum = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        Ok((
            page_id,
            page_type,
            parent_page_id,
            next_leaf_page_id,
            cell_count,
            free_space_offset,
            checksum,
        ))
    }

    fn read_slot_directory(
        bytes: &[u8],
        cell_count: u16,
        page_id: PageId,
    ) -> Result<Vec<SlotEntry>, DatabaseError> {
        let mut slots = Vec::with_capacity(cell_count as usize);
        let mut offset = 0;

        for _ in 0..cell_count {
            if offset + SLOT_DIRECTORY_ENTRY_SIZE > bytes.len() {
                return Err(DatabaseError::CorruptedPage {
                    page_id,
                    reason: "Slot directory extends beyond buffer".to_string(),
                });
            }

            let slot_offset = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
            offset += 2;
            let length = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
            offset += 2;

            // FIX: Only validate non-deleted slots
            if length > 0 && slot_offset as usize + length as usize > PAGE_SIZE {
                return Err(DatabaseError::CorruptedPage {
                    page_id,
                    reason: format!(
                        "Slot at offset {} with length {} exceeds page boundary",
                        slot_offset, length
                    ),
                });
            }

            // Detect overflow slots based on size
            let is_overflow = length as usize == OverflowPointer::SERIALIZED_SIZE;

            slots.push(SlotEntry {
                offset: slot_offset,
                length,
                row_id: None,
                is_overflow,
                overflow_pointer: None, // Will be parsed when data is loaded
            });
        }

        Ok(slots)
    }

    // Keep existing from_bytes for backward compatibility
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DatabaseError> {
        if bytes.len() != PAGE_SIZE {
            return Err(DatabaseError::InvalidPageSize {
                expected: PAGE_SIZE,
                actual: bytes.len(),
            });
        }

        let (
            page_id,
            page_type,
            parent_page_id,
            next_leaf_page_id,
            cell_count,
            free_space_offset,
            stored_checksum,
        ) = Self::read_header(&bytes[..PAGE_HEADER_SIZE])?;

        if free_space_offset as usize > PAGE_SIZE {
            return Err(DatabaseError::CorruptedPage {
                page_id,
                reason: format!("Invalid free_space_offset: {}", free_space_offset),
            });
        }

        let slots = Self::read_slot_directory(&bytes[PAGE_HEADER_SIZE..], cell_count, page_id)?;

        let mut data = Vec::with_capacity(PAGE_SIZE);
        data.extend_from_slice(bytes);

        let page = Page {
            page_id,
            page_type,
            parent_page_id,
            next_leaf_page_id,
            is_dirty: false,
            slot_directory: SlotDirectory { slots },
            free_space_offset,
            cell_count,
            data: Some(data),
            checksum: stored_checksum,
            overflow_pages: Vec::new(),
        };

        if !page.verify_checksum() {
            return Err(DatabaseError::CorruptedPage {
                page_id,
                reason: "Checksum verification failed".to_string(),
            });
        }

        Ok(page)
    }

    /// Serialize the page to bytes (only works in full data mode)
    pub fn to_bytes(&self) -> Result<Vec<u8>, DatabaseError> {
        if self.is_metadata_only() {
            return Err(DatabaseError::SerializationError { 
                details: "Serialization requires full page".to_string() 
            });
        }

        let mut buffer = Vec::with_capacity(PAGE_SIZE);
        buffer.resize(PAGE_SIZE, 0);

        let mut cursor = Cursor::new(&mut buffer);
        self.write_header(&mut cursor);

        // Write SLOT DIRECTORY
        let mut offset = PAGE_HEADER_SIZE;
        for slot in &self.slot_directory.slots {
            buffer[offset..offset + 2].copy_from_slice(&slot.offset.to_le_bytes());
            offset += 2;
            buffer[offset..offset + 2].copy_from_slice(&slot.length.to_le_bytes());
            offset += 2;
        }

        // Copy CELL DATA
        if let Some(ref data) = self.data {
            let data_start = self.free_space_offset as usize;
            if data_start < PAGE_SIZE && data_start < data.len() {
                let copy_len = std::cmp::min(PAGE_SIZE - data_start, data.len() - data_start);
                buffer[data_start..data_start + copy_len].copy_from_slice(&data[data_start..data_start + copy_len]);
            }
        }

        Ok(buffer)
    }

    fn write_header(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let buffer = cursor.get_mut();
        let mut offset = 0;

        buffer[offset..offset + 8].copy_from_slice(&self.page_id.to_le_bytes());
        offset += 8;

        buffer[offset] = self.page_type.as_u8();
        offset += 1;

        let parent_id = self.parent_page_id.unwrap_or(u64::MAX);
        buffer[offset..offset + 8].copy_from_slice(&parent_id.to_le_bytes());
        offset += 8;

        let next_leaf_id = self.next_leaf_page_id.unwrap_or(u64::MAX);
        buffer[offset..offset + 8].copy_from_slice(&next_leaf_id.to_le_bytes());
        offset += 8;

        buffer[offset..offset + 2].copy_from_slice(&self.cell_count.to_le_bytes());
        offset += 2;

        buffer[offset..offset + 2].copy_from_slice(&self.free_space_offset.to_le_bytes());
        offset += 2;

        buffer[offset..offset + 4].copy_from_slice(&self.checksum.to_le_bytes());
    }
}