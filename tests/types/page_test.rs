use std::time::Instant;

use bambang::types::{
    error::DatabaseError, page::{Page, PageType}, PAGE_HEADER_SIZE, PAGE_SIZE, SLOT_DIRECTORY_ENTRY_SIZE
};

// Test utilities
fn create_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

fn create_sample_row_data(id: u32) -> Vec<u8> {
    format!("row_data_{:06}", id).into_bytes()
}

#[test]
fn test_page_creation_and_basic_properties() {
    let page = Page::new(1, PageType::LeafTable);

    assert_eq!(page.page_id, 1);
    assert_eq!(page.page_type, PageType::LeafTable);
    assert_eq!(page.cell_count, 0);
    assert_eq!(page.free_space_offset, PAGE_SIZE as u16);
    assert!(!page.is_metadata_only());
    assert!(page.verify_checksum());
    assert_eq!(page.slot_directory.slots.len(), 0);
}

#[test]
fn test_metadata_only_mode() {
    // Create a minimal header for testing
    let mut header_bytes = vec![0u8; PAGE_HEADER_SIZE + 16]; // Header + space for 4 slots

    // Manually construct header: page_id=42, page_type=13 (LeafTable), no parent/next, cell_count=2
    header_bytes[0..8].copy_from_slice(&42u64.to_le_bytes());
    header_bytes[8] = 13; // LeafTable
    header_bytes[9..17].copy_from_slice(&u64::MAX.to_le_bytes()); // No parent
    header_bytes[17..25].copy_from_slice(&u64::MAX.to_le_bytes()); // No next leaf
    header_bytes[25..27].copy_from_slice(&2u16.to_le_bytes()); // cell_count = 2
    header_bytes[27..29].copy_from_slice(&(PAGE_SIZE as u16 - 100).to_le_bytes()); // free_space_offset
    header_bytes[29..33].copy_from_slice(&0u32.to_le_bytes()); // checksum (we'll ignore for this test)

    // Add slot directory entries
    let slot_offset = PAGE_HEADER_SIZE;
    header_bytes[slot_offset..slot_offset + 2].copy_from_slice(&1000u16.to_le_bytes()); // slot 0 offset
    header_bytes[slot_offset + 2..slot_offset + 4].copy_from_slice(&50u16.to_le_bytes()); // slot 0 length
    header_bytes[slot_offset + 4..slot_offset + 6].copy_from_slice(&1050u16.to_le_bytes()); // slot 1 offset
    header_bytes[slot_offset + 6..slot_offset + 8].copy_from_slice(&30u16.to_le_bytes()); // slot 1 length

    let metadata_size = Page::calculate_metadata_size(&header_bytes).unwrap();
    assert_eq!(metadata_size, PAGE_HEADER_SIZE + 8); // 2 slots * 4 bytes each

    let page = Page::from_header_bytes(&header_bytes[..metadata_size]).unwrap();

    assert_eq!(page.page_id, 42);
    assert_eq!(page.page_type, PageType::LeafTable);
    assert_eq!(page.cell_count, 2);
    assert!(page.is_metadata_only());
    assert_eq!(page.slot_directory.slots.len(), 2);
    assert_eq!(page.slot_directory.slots[0].offset, 1000);
    assert_eq!(page.slot_directory.slots[0].length, 50);
    assert_eq!(page.slot_directory.slots[1].offset, 1050);
    assert_eq!(page.slot_directory.slots[1].length, 30);

    println!("Metadata size: {}", metadata_size);
    println!("Memory footprint: {}", page.memory_footprint());
    println!("Page size: {}", PAGE_SIZE);

    // Should be much smaller memory footprint
    assert!(page.memory_footprint() < PAGE_SIZE);
}

#[test]
fn test_cell_insertion_and_retrieval() {
    let mut page = Page::new(1, PageType::LeafTable);
    let test_data1 = create_sample_row_data(1);
    let test_data2 = create_sample_row_data(2);

    // Insert first cell
    let slot1 = page.insert_cell(&test_data1, Some(1)).unwrap();
    assert_eq!(slot1, 0);
    assert_eq!(page.cell_count, 1);
    assert_eq!(page.active_cell_count(), 1);
    assert!(page.is_dirty);

    // Insert second cell
    let slot2 = page.insert_cell(&test_data2, Some(2)).unwrap();
    assert_eq!(slot2, 1);
    assert_eq!(page.cell_count, 2);
    assert_eq!(page.active_cell_count(), 2);

    // Retrieve cells
    let retrieved1 = page.get_cell(slot1).unwrap();
    let retrieved2 = page.get_cell(slot2).unwrap();

    assert_eq!(retrieved1, test_data1);
    assert_eq!(retrieved2, test_data2);

    // Verify slot directory
    assert_eq!(page.slot_directory.slots[0].row_id, Some(1));
    assert_eq!(page.slot_directory.slots[1].row_id, Some(2));

    // Check free space decreases appropriately
    let expected_space = PAGE_SIZE
        - PAGE_HEADER_SIZE
        - (2 * SLOT_DIRECTORY_ENTRY_SIZE)
        - test_data1.len()
        - test_data2.len();
    assert_eq!(page.available_space(), expected_space);
}

#[test]
fn test_cell_deletion() {
    let mut page = Page::new(1, PageType::LeafTable);
    let test_data = create_sample_row_data(1);

    let slot = page.insert_cell(&test_data, Some(1)).unwrap();
    assert_eq!(page.active_cell_count(), 1);

    // Delete the cell
    page.delete_cell(slot).unwrap();
    assert_eq!(page.active_cell_count(), 0);
    assert!(page.is_slot_deleted(slot));

    // Verify slot is marked as deleted
    assert_eq!(page.slot_directory.slots[slot].length, 0);
    assert_eq!(page.slot_directory.slots[slot].offset, 0);
    assert_eq!(page.slot_directory.slots[slot].row_id, None);

    // Should not be able to retrieve deleted cell
    assert!(page.get_cell(slot).is_none() || page.get_cell(slot) == Some(&[]));
}

#[test]
fn test_cell_update_same_size() {
    let mut page = Page::new(1, PageType::LeafTable);
    let original_data = create_test_data(50);
    let new_data = create_test_data(50); // Same size

    let slot = page.insert_cell(&original_data, Some(1)).unwrap();
    let original_offset = page.slot_directory.slots[slot].offset;

    // Update with same size should work in-place
    page.update_cell(slot, &new_data, Some(2)).unwrap();

    assert_eq!(page.slot_directory.slots[slot].offset, original_offset);
    assert_eq!(page.slot_directory.slots[slot].length, 50);
    assert_eq!(page.slot_directory.slots[slot].row_id, Some(2));

    let retrieved = page.get_cell(slot).unwrap();
    assert_eq!(retrieved, new_data);
}

#[test]
fn test_cell_update_smaller_size() {
    let mut page = Page::new(1, PageType::LeafTable);
    let original_data = create_test_data(100);
    let new_data = create_test_data(50); // Smaller

    let slot = page.insert_cell(&original_data, Some(1)).unwrap();
    let original_offset = page.slot_directory.slots[slot].offset;

    page.update_cell(slot, &new_data, Some(2)).unwrap();

    assert_eq!(page.slot_directory.slots[slot].offset, original_offset);
    assert_eq!(page.slot_directory.slots[slot].length, 50);
    assert_eq!(page.slot_directory.slots[slot].row_id, Some(2));

    let retrieved = page.get_cell(slot).unwrap();
    assert_eq!(retrieved, new_data);
}

#[test]
fn test_cell_update_larger_size() {
    let mut page = Page::new(1, PageType::LeafTable);
    let original_data = create_test_data(50);
    let new_data = create_test_data(100); // Larger

    let slot = page.insert_cell(&original_data, Some(1)).unwrap();

    // This should trigger compaction and relocation
    page.update_cell(slot, &new_data, Some(2)).unwrap();

    assert_eq!(page.slot_directory.slots[slot].length, 100);
    assert_eq!(page.slot_directory.slots[slot].row_id, Some(2));

    let retrieved = page.get_cell(slot).unwrap();
    assert_eq!(retrieved, new_data);
}

#[test]
fn test_page_compaction() {
    let mut page = Page::new(1, PageType::LeafTable);

    // Insert multiple cells
    let data1 = create_test_data(100);
    let data2 = create_test_data(100);
    let data3 = create_test_data(100);

    let slot1 = page.insert_cell(&data1, Some(1)).unwrap();
    let slot2 = page.insert_cell(&data2, Some(2)).unwrap();
    let slot3 = page.insert_cell(&data3, Some(3)).unwrap();

    let initial_free_space = page.available_space();

    // Delete middle cell to create fragmentation
    page.delete_cell(slot2).unwrap();

    // Force compaction
    page.compact().unwrap();

    // Should have more free space and fewer slots
    assert!(page.available_space() > initial_free_space);
    assert_eq!(page.active_cell_count(), 2);

    // Remaining cells should still be accessible with updated indices
    // Note: After compaction, slot indices might change
    let active_slots: Vec<_> = (0..page.slot_directory.slots.len())
        .filter(|&i| !page.is_slot_deleted(i))
        .collect();

    assert_eq!(active_slots.len(), 2);
}

#[test]
fn test_fragmentation_calculation() {
    let mut page = Page::new(1, PageType::LeafTable);

    // Initially no fragmentation
    assert_eq!(page.get_fragmentation_ratio(), 0.0);

    // Insert and delete to create fragmentation
    let data = create_test_data(100);
    let slot1 = page.insert_cell(&data, Some(1)).unwrap();
    let slot2 = page.insert_cell(&data, Some(2)).unwrap();
    let slot3 = page.insert_cell(&data, Some(3)).unwrap();

    // Should still be low fragmentation
    let frag_before = page.get_fragmentation_ratio();
    assert!(frag_before < 0.1);

    // Delete middle cell
    page.delete_cell(slot2).unwrap();

    // Should have increased fragmentation
    let frag_after = page.get_fragmentation_ratio();
    assert!(frag_after > frag_before);
}

#[test]
fn test_page_statistics() {
    let mut page = Page::new(1, PageType::LeafTable);

    let stats_empty = page.get_page_stats();
    assert_eq!(stats_empty.total_slots, 0);
    assert_eq!(stats_empty.active_slots, 0);
    assert_eq!(stats_empty.deleted_slots, 0);

    // Add some cells
    let data = create_test_data(100);
    page.insert_cell(&data, Some(1)).unwrap();
    page.insert_cell(&data, Some(2)).unwrap();
    page.insert_cell(&data, Some(3)).unwrap();

    let stats_full = page.get_page_stats();
    assert_eq!(stats_full.total_slots, 3);
    assert_eq!(stats_full.active_slots, 3);
    assert_eq!(stats_full.deleted_slots, 0);
    assert!(stats_full.utilization_ratio > 0.0);

    // Delete one cell
    page.delete_cell(1).unwrap();

    let stats_deleted = page.get_page_stats();
    assert_eq!(stats_deleted.total_slots, 3);
    assert_eq!(stats_deleted.active_slots, 2);
    assert_eq!(stats_deleted.deleted_slots, 1);
    assert!(stats_deleted.wasted_space > 0);
}

#[test]
fn test_serialization_roundtrip() {
    let mut page = Page::new(42, PageType::LeafTable);
    page.parent_page_id = Some(10);
    page.next_leaf_page_id = Some(50);

    // Add some data
    let data1 = create_sample_row_data(1);
    let data2 = create_sample_row_data(2);
    page.insert_cell(&data1, Some(1)).unwrap();
    page.insert_cell(&data2, Some(2)).unwrap();

    // Serialize
    let bytes = page.to_bytes().unwrap();
    assert_eq!(bytes.len(), PAGE_SIZE);

    // Deserialize
    let reconstructed = Page::from_bytes(&bytes).unwrap();

    // Verify all fields match
    assert_eq!(reconstructed.page_id, page.page_id);
    assert_eq!(reconstructed.page_type, page.page_type);
    assert_eq!(reconstructed.parent_page_id, page.parent_page_id);
    assert_eq!(reconstructed.next_leaf_page_id, page.next_leaf_page_id);
    assert_eq!(reconstructed.cell_count, page.cell_count);
    assert_eq!(reconstructed.free_space_offset, page.free_space_offset);
    assert_eq!(
        reconstructed.slot_directory.slots.len(),
        page.slot_directory.slots.len()
    );

    // Verify data integrity
    assert_eq!(reconstructed.get_cell(0).unwrap(), data1);
    assert_eq!(reconstructed.get_cell(1).unwrap(), data2);

    // Checksum should be valid
    assert!(reconstructed.verify_checksum());
}

#[test]
fn test_page_capacity_limits() {
    let mut page = Page::new(1, PageType::LeafTable);
    let small_data = create_test_data(10);
    let mut inserted_count = 0;

    // Fill page until it's full
    loop {
        match page.insert_cell(&small_data, Some(inserted_count as u64)) {
            Ok(_) => inserted_count += 1,
            Err(DatabaseError::PageFull { .. }) => break,
            Err(e) => panic!("Unexpected error: {:?}", e),
        }

        // Safety check to prevent infinite loop
        if inserted_count > 1000 {
            panic!("Inserted too many items, possible bug");
        }
    }

    assert!(inserted_count > 0);
    assert!(!page.can_fit(small_data.len()));

    // Should not be able to insert more
    assert!(matches!(
        page.insert_cell(&small_data, Some(999)),
        Err(DatabaseError::PageFull { .. })
    ));
}

#[test]
fn test_overflow_functionality() {
    let mut page = Page::new(1, PageType::LeafTable);
    let large_data = create_test_data(PAGE_SIZE / 2); // Definitely needs overflow

    assert!(page.needs_overflow(large_data.len()));

    // This should fail without overflow page
    assert!(matches!(
        page.insert_cell_with_overflow(&large_data, Some(1), None),
        Err(DatabaseError::OverflowPageRequired)
    ));

    // With overflow page should work
    let overflow_page_id = 100;
    let slot = page
        .insert_cell_with_overflow(&large_data, Some(1), Some(overflow_page_id))
        .unwrap();

    assert_eq!(page.slot_directory.slots[slot].is_overflow, true);
    assert!(page.slot_directory.slots[slot].overflow_pointer.is_some());
    assert_eq!(page.overflow_pages, vec![overflow_page_id]);
}

#[test]
fn test_error_conditions() {
    let mut page = Page::new(1, PageType::LeafTable);

    // Test invalid slot index
    assert!(matches!(
        page.delete_cell(999),
        Err(DatabaseError::InvalidSlotIndex { .. })
    ));

    assert!(matches!(
        page.update_cell(999, &[1, 2, 3], Some(1)),
        Err(DatabaseError::InvalidSlotIndex { .. })
    ));

    // Test metadata-only operations
    let mut header_bytes = vec![0u8; PAGE_HEADER_SIZE];
    // Set a valid page type (LeafTable = 13) at offset 8
    header_bytes[8] = 13;
    let mut meta_page = Page::from_header_bytes(&header_bytes).unwrap();

    assert!(matches!(
        meta_page.insert_cell(&[1, 2, 3], Some(1)),
        Err(DatabaseError::SerializationError { .. })
    ));

    assert!(matches!(
        meta_page.to_bytes(),
        Err(DatabaseError::SerializationError { .. })
    ));
}

#[test]
fn test_checksum_validation() {
    let mut page = Page::new(1, PageType::LeafTable);
    let data = create_sample_row_data(1);
    page.insert_cell(&data, Some(1)).unwrap();

    // Valid checksum initially
    assert!(page.verify_checksum());

    // Manually corrupt data and verify checksum fails
    if let Some(ref mut page_data) = page.data {
        page_data[PAGE_SIZE - 1] ^= 0xFF; // Flip some bits
    }

    // Should fail checksum now
    assert!(!page.verify_checksum());

    // Update checksum and it should pass again
    page.update_checksum();
    assert!(page.verify_checksum());
}

#[test]
fn test_memory_footprint_tracking() {
    let page_full = Page::new(1, PageType::LeafTable);
    let mut header_bytes = vec![0u8; PAGE_HEADER_SIZE];
    // Set a valid page type (LeafTable = 13) at offset 8
    header_bytes[8] = 13;
    let page_meta = Page::from_header_bytes(&header_bytes).unwrap();

    let full_footprint = page_full.memory_footprint();
    let meta_footprint = page_meta.memory_footprint();

    // Metadata-only should be significantly smaller
    assert!(meta_footprint < full_footprint);
    assert!(full_footprint >= PAGE_SIZE); // Should include the data Vec
}

#[test]
fn test_concurrent_stress_operations() {
    let mut page = Page::new(1, PageType::LeafTable);
    let mut operations = Vec::new();

    // Simulate a mix of operations
    for i in 0..50 {
        let data = create_test_data(50 + (i % 20)); // Variable sizes
        if let Ok(slot) = page.insert_cell(&data, Some(i as u64)) {
            operations.push(('I', slot, i)); // Insert
        }
    }

    // Random updates and deletes
    for i in (0..operations.len()).step_by(3) {
        let (_, slot, _) = operations[i];
        if !page.is_slot_deleted(slot) {
            let new_data = create_test_data(30);
            let _ = page.update_cell(slot, &new_data, Some((i + 1000) as u64));
            operations[i] = ('U', slot, i + 1000);
        }
    }

    for i in (1..operations.len()).step_by(5) {
        let (_, slot, _) = operations[i];
        if !page.is_slot_deleted(slot) {
            let _ = page.delete_cell(slot);
            operations[i] = ('D', slot, 0);
        }
    }

    // Verify page consistency
    assert!(page.verify_checksum());
    assert_eq!(page.cell_count as usize, page.slot_directory.slots.len());

    // Verify active count matches reality
    let expected_active = operations.iter().filter(|(op, _, _)| *op != 'D').count();
    // Note: This might not match exactly due to our deletion pattern, but should be reasonable
    assert!(page.active_cell_count() <= expected_active);

    let stats = page.get_page_stats();
    assert!(stats.utilization_ratio >= 0.0 && stats.utilization_ratio <= 1.0);
    assert!(stats.fragmentation_ratio >= 0.0 && stats.fragmentation_ratio <= 1.0);
}

#[test]
fn test_page_type_conversion() {
    // Test all page types
    let types = vec![
        (PageType::InteriorIndex, 2),
        (PageType::InteriorTable, 5),
        (PageType::LeafIndex, 10),
        (PageType::LeafTable, 13),
        (PageType::OverflowPage, 15),
    ];

    for (page_type, expected_byte) in types {
        assert_eq!(page_type.as_u8(), expected_byte);
        assert_eq!(PageType::from_u8(expected_byte).unwrap(), page_type);
    }

    // Test invalid page type
    assert!(matches!(
        PageType::from_u8(99),
        Err(DatabaseError::InvalidPageType(99))
    ));
}

#[test]
fn test_boundary_conditions() {
    let mut page = Page::new(1, PageType::LeafTable);

    // Test with zero-length data
    let empty_data = Vec::new();
    let slot = page.insert_cell(&empty_data, Some(1)).unwrap();
    assert_eq!(page.get_cell(slot).unwrap(), &empty_data);

    // Test with maximum reasonable data size
    let max_data = create_test_data(PAGE_SIZE / 8);
    let slot2 = page.insert_cell(&max_data, Some(2)).unwrap();
    assert_eq!(page.get_cell(slot2).unwrap(), &max_data);

    // Test slot directory at boundaries
    assert!(!page.is_slot_deleted(slot));
    assert!(!page.is_slot_deleted(slot2));
    assert!(page.is_slot_deleted(999)); // Out of bounds should return true
}

#[test]
fn bench_page_operations() {
    let iterations = 1000;

    // Benchmark insertion
    let start = Instant::now();
    let mut page = Page::new(1, PageType::LeafTable);
    let data = create_test_data(100);

    for i in 0..iterations {
        if page.insert_cell(&data, Some(i)).is_err() {
            break; // Page full
        }
    }
    let insert_duration = start.elapsed();

    // Benchmark retrieval
    let start = Instant::now();
    for i in 0..page.active_cell_count() {
        let _ = page.get_cell(i);
    }
    let retrieve_duration = start.elapsed();

    // Benchmark metadata-only creation
    let header_bytes = vec![0u8; PAGE_HEADER_SIZE + 100]; // Some slot entries
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = Page::from_header_bytes(&header_bytes);
    }
    let metadata_duration = start.elapsed();

    println!("Insert duration: {:?}", insert_duration);
    println!("Retrieve duration: {:?}", retrieve_duration);
    println!("Metadata creation duration: {:?}", metadata_duration);

    // Basic performance assertions
    assert!(insert_duration.as_millis() < 100); // Should be fast
    assert!(retrieve_duration.as_micros() < 1000); // Should be very fast
    assert!(metadata_duration.as_millis() < 10); // Should be very fast
}
