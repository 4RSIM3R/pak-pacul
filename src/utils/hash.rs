use crc32fast::Hasher;

use crate::types::{page::{PageType, SlotEntry}, PageId};

pub fn calculate_page_checksum(
    page_id: PageId,
    page_type: &PageType,
    parent_page_id: Option<PageId>,
    next_leaf_page_id: Option<PageId>,
    cell_count: u16,
    free_space_offset: u16,
    slots: &[SlotEntry],
    data: Option<&[u8]>,
    data_start_offset: usize,
) -> u32 {
    let mut hasher = Hasher::new();

    hasher.update(&page_id.to_le_bytes());
    hasher.update(&[page_type.as_u8()]);
    hasher.update(&parent_page_id.unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(&next_leaf_page_id.unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(&cell_count.to_le_bytes());
    hasher.update(&free_space_offset.to_le_bytes());

    for slot in slots {
        hasher.update(&slot.offset.to_le_bytes());
        hasher.update(&slot.length.to_le_bytes());
        hasher.update(&[if slot.is_overflow { 1 } else { 0 }]);
    }

    // Only hash data if we have it loaded
    if let Some(data_slice) = data {
        hasher.update(&data_slice[data_start_offset..]);
    }

    hasher.finalize()
}

pub fn verify_page_checksum(
    page_id: PageId,
    page_type: &PageType,
    parent_page_id: Option<PageId>,
    next_leaf_page_id: Option<PageId>,
    cell_count: u16,
    free_space_offset: u16,
    slots: &[SlotEntry],
    data: Option<&[u8]>,
    data_start_offset: usize,
    expected_checksum: u32,
) -> bool {
    let calculated = calculate_page_checksum(
        page_id,
        page_type,
        parent_page_id,
        next_leaf_page_id,
        cell_count,
        free_space_offset,
        slots,
        data,
        data_start_offset,
    );
    calculated == expected_checksum
}
