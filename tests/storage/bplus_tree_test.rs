use bambang::{
    storage::bplus_tree::BPlusTree,
    types::{
        PAGE_SIZE,
        page::{Page, PageType},
        row::Row,
        value::Value,
    },
};
use std::io::Write;
use tempfile::NamedTempFile;

fn create_test_db_file() -> NamedTempFile {
    let mut temp_file = NamedTempFile::new().unwrap();
    let root_page = Page::new(1, PageType::LeafTable);
    let page_bytes = root_page.to_bytes().unwrap();
    temp_file.write_all(&page_bytes).unwrap();
    temp_file.flush().unwrap();
    temp_file
}

fn create_test_row(key: i64, name: &str) -> Row {
    Row::new(vec![Value::Integer(key), Value::Text(name.to_string())])
}

#[test]
fn test_bplus_tree_creation() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let btree = BPlusTree::new(file, 1).unwrap();
    assert_eq!(btree.root_page_id, 1);
    assert_eq!(btree.order, 4);
    assert_eq!(btree.next_page_id, 2);
    assert!(btree.page_cache.is_empty());
}

#[test]
fn test_single_row_insert() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    let test_row = create_test_row(1, "Alice");
    let result = btree.insert(test_row, None).unwrap();
    assert!(result.is_none());
    let root_page = btree.load_page(1, None).unwrap();
    assert_eq!(root_page.cell_count, 1);
    assert_eq!(root_page.page_type, PageType::LeafTable);
}

#[test]
fn test_multiple_row_insert_no_split() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    let rows = vec![
        create_test_row(1, "Alice"),
        create_test_row(2, "Bob"),
        create_test_row(3, "Charlie"),
    ];
    for row in rows {
        let result = btree.insert(row, None).unwrap();
        assert!(result.is_none());
    }
    let root_page = btree.load_page(1, None).unwrap();
    assert_eq!(root_page.cell_count, 3);
    assert_eq!(root_page.page_type, PageType::LeafTable);
}

#[test]
fn test_row_insert_with_leaf_split() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    let large_name = "A".repeat(1000);
    let mut rows = Vec::new();
    for i in 1..=10 {
        rows.push(create_test_row(i, &format!("{}{}", large_name, i)));
    }
    let mut split_occurred = false;
    for row in rows {
        let result = btree.insert(row, None).unwrap();
        if result.is_some() {
            split_occurred = true;
            assert!(btree.root_page_id > 1);
            break;
        }
    }
    assert!(split_occurred);
    let new_root = btree.load_page(btree.root_page_id, None).unwrap();
    assert_eq!(new_root.page_type, PageType::InteriorTable);
    assert!(new_root.cell_count >= 2);
}

#[test]
fn test_ordered_insertion() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    for i in 1..=5 {
        let row = create_test_row(i, &format!("User{}", i));
        btree.insert(row, None).unwrap();
    }
    let root_page = btree.load_page(btree.root_page_id, None).unwrap();
    assert!(root_page.cell_count > 0);
}

#[test]
fn test_reverse_ordered_insertion() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    for i in (1..=5).rev() {
        let row = create_test_row(i, &format!("User{}", i));
        btree.insert(row, None).unwrap();
    }
    let root_page = btree.load_page(btree.root_page_id, None).unwrap();
    assert!(root_page.cell_count > 0);
}

#[test]
fn test_random_insertion() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    let keys = vec![3, 1, 4, 2, 5];
    for key in keys {
        let row = create_test_row(key, &format!("User{}", key));
        btree.insert(row, None).unwrap();
    }
    let root_page = btree.load_page(btree.root_page_id, None).unwrap();
    assert!(root_page.cell_count > 0);
}

#[test]
fn test_duplicate_key_insertion() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    let row1 = create_test_row(1, "Alice");
    let row2 = create_test_row(1, "Bob");
    btree.insert(row1, None).unwrap();
    btree.insert(row2, None).unwrap();
    let root_page = btree.load_page(btree.root_page_id, None).unwrap();
    assert_eq!(root_page.cell_count, 2);
}

#[test]
fn test_large_data_insertion_with_overflow() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    let large_data = "X".repeat(PAGE_SIZE / 2);
    let large_row = create_test_row(1, &large_data);
    let result = btree.insert(large_row, None).unwrap();
    let root_page = btree.load_page(btree.root_page_id, None).unwrap();
    assert!(root_page.cell_count > 0);
}

#[test]
fn test_interior_page_creation() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    let large_name = "Data".repeat(500);
    let mut interior_created = false;
    for i in 1..=20 {
        let row = create_test_row(i, &format!("{}{}", large_name, i));
        if btree.insert(row, None).unwrap().is_some() {
            interior_created = true;
        }
    }
    assert!(interior_created);
    let root_page = btree.load_page(btree.root_page_id, None).unwrap();
    assert_eq!(root_page.page_type, PageType::InteriorTable);
}

#[test]
fn test_page_allocation() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    let initial_next_page = btree.next_page_id;
    let large_data = "X".repeat(1000);
    for i in 1..=10 {
        let row = create_test_row(i, &format!("{}{}", large_data, i));
        btree.insert(row, None).unwrap();
    }
    assert!(btree.next_page_id > initial_next_page);
}

#[test]
fn test_cell_data_integrity() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    let test_data = vec![
        (1, "Alice"),
        (2, "Bob with special chars: !@#$%^&*()"),
        (3, "Charlie\nwith\nnewlines"),
        (4, "Dave with unicode: 🦀🔥⚡"),
    ];
    for (key, name) in &test_data {
        let row = create_test_row(*key, name);
        btree.insert(row, None).unwrap();
    }
    let root_page_id = btree.root_page_id;
    let root_page = btree.load_page(root_page_id, None).unwrap();
    assert_eq!(root_page.cell_count, test_data.len() as u16);
    let mut cell_data_vec = Vec::new();
    for i in 0..root_page.slot_directory.slots.len() {
        if let Some(cell_data) = root_page.get_cell(i) {
            cell_data_vec.push(cell_data.to_vec());
        }
    }
    for cell_data in cell_data_vec {
        let extracted_key = btree.extract_key_from_cell(&cell_data).unwrap();
        assert!(matches!(extracted_key, Value::Integer(_)));
    }
}

#[test]
fn test_split_result_structure() {
    let temp_file = create_test_db_file();
    let file = temp_file.reopen().unwrap();
    let mut btree = BPlusTree::new(file, 1).unwrap();
    let large_data = "X".repeat(800);
    let mut split_result = None;
    for i in 1..=15 {
        let row = create_test_row(i, &format!("{}{}", large_data, i));
        if let Some(result) = btree.insert(row, None).unwrap() {
            split_result = Some(result);
            break;
        }
    }
    assert!(split_result.is_some());
    assert!(btree.root_page_id > 1);
}
