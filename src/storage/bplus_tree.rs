use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

use crate::types::{
    PAGE_SIZE, PageId,
    error::DatabaseError,
    page::{Page, PageType},
    row::Row,
    value::Value,
};

#[derive(Debug, Clone)]
pub struct Cell {
    pub data: Vec<u8>,
    pub overflow_page_id: Option<PageId>,
}

#[derive(Debug)]
pub struct SplitResult {
    pub left_page: Page,
    pub right_page: Page,
    pub separator_key: Value,
}

pub struct BPlusTree {
    pub root_page_id: PageId,
    pub file: File,
    pub page_cache: HashMap<PageId, Page>,
    pub next_page_id: PageId,
    pub order: usize,
}

impl BPlusTree {
    pub fn new(file: File, root_page_id: PageId) -> Result<Self, DatabaseError> {
        Self::new_with_extras(file, root_page_id, None)
    }

    pub fn new_with_extras(file: File, root_page_id: PageId, extras: Option<u64>) -> Result<Self, DatabaseError> {
        let file_size = file.metadata()?.len();
        let data_size = if let Some(extras) = extras {
            file_size.saturating_sub(extras)
        } else {
            file_size
        };
        let next_page_id = ((data_size / PAGE_SIZE as u64) + 1) as PageId;
        Ok(Self {
            root_page_id,
            file,
            page_cache: HashMap::new(),
            next_page_id,
            order: 4,
        })
    }

    pub fn load_page(
        &mut self,
        page_id: PageId,
        extras: Option<u64>,
    ) -> Result<&Page, DatabaseError> {
        // Add bounds checking for page_id
        if page_id == 0 {
            return Err(DatabaseError::CorruptedPage {
                page_id,
                reason: "Invalid page ID: 0".to_string(),
            });
        }
        
        let offset = if let Some(extras) = extras {
            extras as u64 + (page_id - 1) * PAGE_SIZE as u64
        } else {
            (page_id - 1) * PAGE_SIZE as u64
        };
        
        // Add bounds checking for file offset
        let file_size = self.file.metadata()?.len();
        if offset + PAGE_SIZE as u64 > file_size {
            return Err(DatabaseError::CorruptedPage {
                page_id,
                reason: format!("Page offset {} exceeds file size {}", offset, file_size),
            });
        }
        
        if !self.page_cache.contains_key(&page_id) {
            let mut buffer = vec![0u8; PAGE_SIZE];
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.read_exact(&mut buffer)?;
            let page = Page::from_bytes(&buffer)?;
            self.page_cache.insert(page_id, page);
        }
        Ok(self.page_cache.get(&page_id).unwrap())
    }

    fn write_page(&mut self, page_id: PageId, page: Page, extras: Option<u64>) -> Result<(), DatabaseError> {
        // Add bounds checking for page_id
        if page_id == 0 {
            return Err(DatabaseError::CorruptedPage {
                page_id,
                reason: "Invalid page ID: 0".to_string(),
            });
        }
        
        let page_bytes = page.to_bytes()?;
        let offset = if let Some(extras) = extras {
            extras as u64 + (page_id - 1) * PAGE_SIZE as u64
        } else {
            (page_id - 1) * PAGE_SIZE as u64
        };
        
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page_bytes)?;
        // Don't flush here - let batch operations handle flushing
        // Don't add to cache when writing - only cache when pages are requested
        Ok(())
    }

    fn allocate_page(&mut self, page_type: PageType, extras: Option<u64>) -> Result<PageId, DatabaseError> {
        let new_page_id = self.next_page_id;
        self.next_page_id += 1;
        let new_page = Page::new(new_page_id, page_type);
        self.write_page(new_page_id, new_page, extras)?;
        Ok(new_page_id)
    }

    pub fn insert(
        &mut self,
        row: Row,
        extras: Option<u64>,
    ) -> Result<Option<PageId>, DatabaseError> {
        let key = row.values[0].clone();
        let row_bytes = row.to_bytes();
        
        // Validate row data before insertion
        if row_bytes.is_empty() {
            return Err(DatabaseError::CorruptedDatabase {
                reason: "Empty row data".to_string(),
            });
        }
        
        let split_result = self.insert_recursive(
            self.root_page_id,
            key,
            Cell {
                data: row_bytes,
                overflow_page_id: None,
            },
            extras,
        )?;
        
        if let Some(split) = split_result {
            let new_root_id = self.allocate_page(PageType::InteriorTable, extras)?;
            let mut new_root = Page::new(new_root_id, PageType::InteriorTable);
            let left_entry_data =
                self.create_interior_entry(&split.separator_key, split.left_page.page_id)?;
            let right_entry_data =
                self.create_interior_entry(&Value::Null, split.right_page.page_id)?;
            new_root.insert_cell(&left_entry_data, None)?;
            new_root.insert_cell(&right_entry_data, None)?;
            
            // Batch write all pages to reduce I/O overhead
            self.write_pages_batch(&[
                (new_root_id, new_root.clone()),
                (split.left_page.page_id, split.left_page.clone()),
                (split.right_page.page_id, split.right_page.clone()),
            ], extras)?;
            
            // CRITICAL FIX: Update cache with all modified pages
            self.page_cache.insert(new_root_id, new_root);
            self.page_cache.insert(split.left_page.page_id, split.left_page);
            self.page_cache.insert(split.right_page.page_id, split.right_page);
            
            self.root_page_id = new_root_id;
            return Ok(Some(new_root_id));
        }
        
        Ok(None)
    }

    fn create_interior_entry(
        &self,
        key: &Value,
        page_id: PageId,
    ) -> Result<Vec<u8>, DatabaseError> {
        let mut entry_data = Vec::new();
        entry_data.extend_from_slice(&page_id.to_le_bytes());
        let key_bytes = key.to_bytes();
        entry_data.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        entry_data.extend_from_slice(&key_bytes);
        Ok(entry_data)
    }

    fn insert_recursive(
        &mut self,
        page_id: PageId,
        key: Value,
        cell: Cell,
        extras: Option<u64>,
    ) -> Result<Option<SplitResult>, DatabaseError> {
        let page = self.load_page(page_id, extras)?.clone();
        
        match page.page_type {
            PageType::LeafTable => {
                let mut updated_page = page;
                if updated_page.can_fit(cell.data.len()) {
                    if let Some(overflow_page_id) = cell.overflow_page_id {
                        updated_page.insert_cell_with_overflow(
                            &cell.data,
                            None,
                            Some(overflow_page_id),
                        )?;
                        self.write_page(page_id, updated_page.clone(), extras)?;
                        // CRITICAL FIX: Update cache
                        self.page_cache.insert(page_id, updated_page);
                    } else {
                        if updated_page.needs_overflow(cell.data.len()) {
                            let overflow_id = self.allocate_overflow_page(&cell.data, extras)?;
                            updated_page.insert_cell_with_overflow(
                                &cell.data,
                                None,
                                Some(overflow_id),
                            )?;
                            self.write_page(page_id, updated_page.clone(), extras)?;
                            // CRITICAL FIX: Update cache
                            self.page_cache.insert(page_id, updated_page);
                        } else {
                            // Use optimized insertion for regular cells
                            self.insert_with_reduced_writes(page_id, updated_page, &cell.data, extras)?;
                        }
                    }
                    Ok(None)
                } else {
                    let split_result = self.split_leaf_page(updated_page, key, cell, extras)?;
                    Ok(Some(split_result))
                }
            }
            PageType::InteriorTable => {
                let child_page_id = self.find_child_page(&page, &key)?;
                let split_result = self.insert_recursive(child_page_id, key, cell, extras)?;
                if let Some(split) = split_result {
                    let new_entry_data =
                        self.create_interior_entry(&split.separator_key, split.right_page.page_id)?;
                    let mut updated_page = page;
                    if !updated_page.can_fit(new_entry_data.len()) {
                        let interior_split =
                            self.split_interior_page(updated_page, new_entry_data, extras)?;
                        Ok(Some(interior_split))
                    } else {
                        updated_page.insert_cell(&new_entry_data, None)?;
                        
                        // Batch write all pages to reduce I/O overhead
                        self.write_pages_batch(&[
                            (page_id, updated_page),
                            (split.left_page.page_id, split.left_page),
                            (split.right_page.page_id, split.right_page),
                        ], extras)?;
                        
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Err(DatabaseError::CorruptedDatabase {
                reason: "Invalid page type for B+ tree operation".to_string(),
            }),
        }
    }

    fn split_leaf_page(
        &mut self,
        mut full_page: Page,
        key: Value,
        cell: Cell,
        extras: Option<u64>,
    ) -> Result<SplitResult, DatabaseError> {
        let new_page_id = self.allocate_page(PageType::LeafTable, extras)?;
        let mut right_page = Page::new(new_page_id, PageType::LeafTable);
        let mut all_cells = Vec::new();
        
        // Collect all existing cells from the full page
        for i in 0..full_page.slot_directory.slots.len() {
            if let Some(cell_data) = full_page.get_cell(i) {
                if !cell_data.is_empty() {  // Skip empty cells
                    match self.extract_key_from_cell(cell_data) {
                        Ok(extracted_key) => {
                            all_cells.push((extracted_key, cell_data.to_vec()));
                        }
                        Err(_) => {
                            // Skip corrupted cells but don't fail the entire operation
                            continue;
                        }
                    }
                }
            }
        }
        
        // Add the new cell
        all_cells.push((key, cell.data));
        
        // Sort all cells by key
        all_cells.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // Ensure we have at least one cell to split
        if all_cells.is_empty() {
            return Err(DatabaseError::CorruptedDatabase {
                reason: "No cells to split".to_string(),
            });
        }
        
        let split_point = all_cells.len() / 2;
        
        // Clear the left page and rebuild it
        full_page.slot_directory.slots.clear();
        full_page.free_space_offset = PAGE_SIZE as u16;
        full_page.cell_count = 0;
        
        // Insert cells into left page
        for (_, cell_data) in &all_cells[..split_point] {
            if let Err(e) = full_page.insert_cell(cell_data, None) {
                return Err(DatabaseError::CorruptedDatabase {
                    reason: format!("Failed to insert cell into left page: {}", e),
                });
            }
        }
        
        // Insert cells into right page
        for (_, cell_data) in &all_cells[split_point..] {
            if let Err(e) = right_page.insert_cell(cell_data, None) {
                return Err(DatabaseError::CorruptedDatabase {
                    reason: format!("Failed to insert cell into right page: {}", e),
                });
            }
        }
        
        // Update leaf page linkage
        right_page.next_leaf_page_id = full_page.next_leaf_page_id;
        full_page.next_leaf_page_id = Some(new_page_id);
        
        let separator_key = all_cells[split_point].0.clone();
        Ok(SplitResult {
            left_page: full_page,
            right_page,
            separator_key,
        })
    }

    pub fn extract_key_from_cell(&self, cell_data: &[u8]) -> Result<Value, DatabaseError> {
        let row = Row::from_bytes(cell_data)?;
        Ok(row.values[0].clone())
    }

    fn split_interior_page(
        &mut self,
        mut full_page: Page,
        new_entry_data: Vec<u8>,
        extras: Option<u64>,
    ) -> Result<SplitResult, DatabaseError> {
        let new_page_id = self.allocate_page(PageType::InteriorTable, extras)?;
        let mut right_page = Page::new(new_page_id, PageType::InteriorTable);
        let mut all_entries = Vec::new();
        for i in 0..full_page.slot_directory.slots.len() {
            if let Some(entry_data) = full_page.get_cell(i) {
                let key = self.extract_key_from_interior_entry(entry_data)?;
                all_entries.push((key, entry_data.to_vec()));
            }
        }
        let new_key = self.extract_key_from_interior_entry(&new_entry_data)?;
        all_entries.push((new_key, new_entry_data));
        all_entries.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let split_point = all_entries.len() / 2;
        let separator_key = all_entries[split_point].0.clone();
        full_page.slot_directory.slots.clear();
        full_page.free_space_offset = PAGE_SIZE as u16;
        full_page.cell_count = 0;
        for (_, entry_data) in &all_entries[..split_point] {
            full_page.insert_cell(entry_data, None)?;
        }
        for (_, entry_data) in &all_entries[split_point + 1..] {
            right_page.insert_cell(entry_data, None)?;
        }
        Ok(SplitResult {
            left_page: full_page,
            right_page,
            separator_key,
        })
    }

    fn extract_key_from_interior_entry(&self, entry_data: &[u8]) -> Result<Value, DatabaseError> {
        if entry_data.len() < 12 {
            return Err(DatabaseError::CorruptedPage {
                page_id: 0,
                reason: "Interior entry too short".to_string(),
            });
        }
        let key_length =
            u32::from_le_bytes([entry_data[8], entry_data[9], entry_data[10], entry_data[11]])
                as usize;
        if entry_data.len() < 12 + key_length {
            return Err(DatabaseError::CorruptedPage {
                page_id: 0,
                reason: "Interior entry key data incomplete".to_string(),
            });
        }
        let key_bytes = &entry_data[12..12 + key_length];
        Value::from_bytes(key_bytes)
    }

    fn allocate_overflow_page(&mut self, data: &[u8], extras: Option<u64>) -> Result<PageId, DatabaseError> {
        let overflow_page_id = self.allocate_page(PageType::OverflowPage, extras)?;
        let mut overflow_page = Page::new(overflow_page_id, PageType::OverflowPage);
        let available_space = overflow_page.available_space();
        if data.len() <= available_space {
            overflow_page.insert_cell(data, None)?;
            self.write_page(overflow_page_id, overflow_page, extras)?;
            Ok(overflow_page_id)
        } else {
            Err(DatabaseError::CorruptedDatabase {
                reason: "Data too large for overflow page".to_string(),
            })
        }
    }

    fn find_child_page(&self, interior_page: &Page, key: &Value) -> Result<PageId, DatabaseError> {
        let mut child_page_id = None;
        for i in 0..interior_page.slot_directory.slots.len() {
            if let Some(entry_data) = interior_page.get_cell(i) {
                let (entry_page_id, entry_key) = self.parse_interior_entry(entry_data)?;
                if i < interior_page.slot_directory.slots.len() - 1 {
                    if key <= &entry_key {
                        child_page_id = Some(entry_page_id);
                        break;
                    }
                } else {
                    child_page_id = Some(entry_page_id);
                }
            }
        }
        child_page_id.ok_or(DatabaseError::CorruptedPage {
            page_id: interior_page.page_id,
            reason: "No valid child page found".to_string(),
        })
    }

    fn parse_interior_entry(&self, entry_data: &[u8]) -> Result<(PageId, Value), DatabaseError> {
        if entry_data.len() < 12 {
            return Err(DatabaseError::CorruptedPage {
                page_id: 0,
                reason: "Interior entry too short".to_string(),
            });
        }
        let page_id = u64::from_le_bytes([
            entry_data[0],
            entry_data[1],
            entry_data[2],
            entry_data[3],
            entry_data[4],
            entry_data[5],
            entry_data[6],
            entry_data[7],
        ]);
        let key_length =
            u32::from_le_bytes([entry_data[8], entry_data[9], entry_data[10], entry_data[11]])
                as usize;
        if entry_data.len() < 12 + key_length {
            return Err(DatabaseError::CorruptedPage {
                page_id: 0,
                reason: "Interior entry key data incomplete".to_string(),
            });
        }
        let key_bytes = &entry_data[12..12 + key_length];
        let key = Value::from_bytes(key_bytes)?;
        Ok((page_id, key))
    }

    /// Optimized insertion that reduces write overhead by batching operations
    fn insert_with_reduced_writes(
        &mut self,
        page_id: PageId,
        mut page: Page,
        cell_data: &[u8],
        extras: Option<u64>,
    ) -> Result<(), DatabaseError> {
        // Insert the cell into the page structure
        page.insert_cell(cell_data, None)?;
        
        // Write the entire page and flush immediately for single page operations
        self.write_page(page_id, page.clone(), extras)?;
        self.file.flush()?;
        
        // CRITICAL FIX: Update cache with modified page
        self.page_cache.insert(page_id, page);
        
        Ok(())
    }
    
    /// Batch write multiple pages to reduce I/O overhead
    fn write_pages_batch(
        &mut self,
        pages: &[(PageId, Page)],
        extras: Option<u64>,
    ) -> Result<(), DatabaseError> {
        for (page_id, page) in pages {
            self.write_page(*page_id, page.clone(), extras)?;
            // CRITICAL FIX: Update cache for each page
            self.page_cache.insert(*page_id, page.clone());
        }
        // Single flush for all writes
        self.file.flush()?;
        Ok(())
    }
}
