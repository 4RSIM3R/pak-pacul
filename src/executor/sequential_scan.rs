use std::{
    collections::VecDeque,
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use crate::{
    executor::scan::Scanner,
    storage::storage_manager::StorageManager,
    types::{
        PAGE_SIZE, PageId,
        error::DatabaseError,
        page::{Page, PageType},
        row::Row,
    },
};

pub struct SequentialScanner {
    file: File,
    root_page_id: PageId,
    current_page_id: Option<PageId>,
    current_slot_index: usize,
    batch_size: usize,
    read_ahead_pages: VecDeque<Page>,
    table_name: String,
    extras: Option<u64>,
    is_exhausted: bool,
}

impl SequentialScanner {
    pub fn new(
        storage_manager: &StorageManager,
        table_name: String,
        batch_size: Option<usize>,
    ) -> Result<Self, DatabaseError> {
        let root_page_id = storage_manager
            .table_roots
            .get(&table_name)
            .copied()
            .ok_or_else(|| DatabaseError::TableNotFound {
                name: table_name.clone(),
            })?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(&storage_manager.db_info.path)?;
        let extras = Some(crate::storage::BAMBANG_HEADER_SIZE as u64);
        Ok(Self {
            file,
            root_page_id,
            current_page_id: None,
            current_slot_index: 0,
            batch_size: batch_size.unwrap_or(32),
            read_ahead_pages: VecDeque::new(),
            table_name,
            extras,
            is_exhausted: false,
        })
    }

    fn page_offset(&self, page_id: PageId) -> u64 {
        let header_offset = self
            .extras
            .unwrap_or(crate::storage::BAMBANG_HEADER_SIZE as u64);
        header_offset + (page_id - 1) * PAGE_SIZE as u64
    }

    fn find_first_leaf(&mut self) -> Result<PageId, DatabaseError> {
        let mut current_page_id = self.root_page_id;
        loop {
            let page = self.load_page_metadata(current_page_id)?;
            match page.page_type {
                PageType::LeafTable => {
                    return Ok(current_page_id);
                }
                PageType::InteriorTable => {
                    if let Some(first_slot) = page.slot_directory.slots.first() {
                        let child_page_id =
                            self.read_child_page_id_from_slot(current_page_id, first_slot)?;
                        current_page_id = child_page_id;
                    } else {
                        return Err(DatabaseError::CorruptedPage {
                            page_id: current_page_id,
                            reason: "Interior page has no children".to_string(),
                        });
                    }
                }
                _ => {
                    return Err(DatabaseError::CorruptedPage {
                        page_id: current_page_id,
                        reason: "Invalid page type in B+ tree".to_string(),
                    });
                }
            }
        }
    }

    fn load_page_metadata(&mut self, page_id: PageId) -> Result<Page, DatabaseError> {
        let offset = self.page_offset(page_id);
        let mut header_buffer = vec![0u8; crate::types::PAGE_HEADER_SIZE];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut header_buffer)?;
        let metadata_size = Page::calculate_metadata_size(&header_buffer)?;
        let mut metadata_buffer = vec![0u8; metadata_size];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut metadata_buffer)?;
        Page::from_header_bytes(&metadata_buffer)
    }

    fn read_child_page_id_from_slot(
        &mut self,
        page_id: PageId,
        slot: &crate::types::page::SlotEntry,
    ) -> Result<PageId, DatabaseError> {
        let page_offset = self.page_offset(page_id);
        let slot_offset = page_offset + slot.offset as u64;
        let mut page_id_buffer = [0u8; 8];
        self.file.seek(SeekFrom::Start(slot_offset))?;
        self.file.read_exact(&mut page_id_buffer)?;
        Ok(u64::from_le_bytes(page_id_buffer))
    }

    fn read_row_from_slot(
        &mut self,
        page_id: PageId,
        slot: &crate::types::page::SlotEntry,
    ) -> Result<Row, DatabaseError> {
        if slot.is_deleted() {
            return Err(DatabaseError::CorruptedPage {
                page_id,
                reason: "Attempting to read deleted slot".to_string(),
            });
        }
        let page_offset = self.page_offset(page_id);
        let slot_offset = page_offset + slot.offset as u64;
        let data_length = slot.length as usize;
        let mut row_buffer = vec![0u8; data_length];
        self.file.seek(SeekFrom::Start(slot_offset))?;
        self.file.read_exact(&mut row_buffer)?;
        Row::from_bytes(&row_buffer)
    }

    fn prefetch_next_page(&mut self, current_page: &Page) -> Result<(), DatabaseError> {
        if let Some(next_page_id) = current_page.next_leaf_page_id {
            if self.read_ahead_pages.len() < 2 {
                let next_page = self.load_page_metadata(next_page_id)?;
                self.read_ahead_pages.push_back(next_page);
            }
        }
        Ok(())
    }

    fn get_next_page(&mut self) -> Result<Option<(PageId, Page)>, DatabaseError> {
        // First, try to use prefetched pages
        if let Some(page) = self.read_ahead_pages.pop_front() {
            if let Some(page_id) = self.current_page_id {
                let current_page = self.load_page_metadata(page_id)?;
                if let Some(next_id) = current_page.next_leaf_page_id {
                    return Ok(Some((next_id, page)));
                }
            }
        }

        // If no prefetched pages, load the next page directly
        if let Some(current_id) = self.current_page_id {
            let current_page = self.load_page_metadata(current_id)?;
            if let Some(next_id) = current_page.next_leaf_page_id {
                let next_page = self.load_page_metadata(next_id)?;
                return Ok(Some((next_id, next_page)));
            }
        }

        Ok(None)
    }
    
}

impl Scanner for SequentialScanner {
    fn scan(&mut self) -> Result<Option<Row>, DatabaseError> {
        if self.is_exhausted {
            return Ok(None);
        }
        if self.current_page_id.is_none() {
            let first_leaf_id = self.find_first_leaf()?;
            self.current_page_id = Some(first_leaf_id);
            self.current_slot_index = 0;
        }
        loop {
            if let Some(page_id) = self.current_page_id {
                let page = self.load_page_metadata(page_id)?;
                if self.current_slot_index < page.slot_directory.slots.len() {
                    let slot = &page.slot_directory.slots[self.current_slot_index];
                    if slot.is_deleted() {
                        self.current_slot_index += 1;
                        continue;
                    }
                    let row = self.read_row_from_slot(page_id, slot)?;
                    self.current_slot_index += 1;
                    // Prefetch next page when we're near the end of current page
                    if self.current_slot_index >= page.slot_directory.slots.len().saturating_sub(2)
                    {
                        let _ = self.prefetch_next_page(&page);
                    }
                    return Ok(Some(row));
                } else {
                    if let Some((next_page_id, _)) = self.get_next_page()? {
                        self.current_page_id = Some(next_page_id);
                        self.current_slot_index = 0;
                    } else {
                        self.is_exhausted = true;
                        return Ok(None);
                    }
                }
            } else {
                self.is_exhausted = true;
                return Ok(None);
            }
        }
    }
    fn scan_batch(&mut self, batch_size: usize) -> Result<Vec<Row>, DatabaseError> {
        let mut rows = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match self.scan()? {
                Some(row) => rows.push(row),
                None => break,
            }
        }
        Ok(rows)
    }
    fn reset(&mut self) -> Result<(), DatabaseError> {
        self.current_page_id = None;
        self.current_slot_index = 0;
        self.read_ahead_pages.clear();
        self.is_exhausted = false;
        Ok(())
    }
}
