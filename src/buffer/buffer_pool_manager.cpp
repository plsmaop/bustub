//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  std::lock_guard<std::mutex> latch(latch_);

  Page *page = nullptr;
  frame_id_t frame_id = 0;
  auto pgt_iter = page_table_.find(page_id);
  if (pgt_iter != page_table_.end()) {
    frame_id = pgt_iter->second;
    page = &pages_[frame_id];
    replacer_->Pin(frame_id);
    page->pin_count_++;

    return page;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&frame_id)) {
      return page;
    }
  }

  page = &pages_[frame_id];

  page_table_.erase(page->GetPageId());
  page_table_.emplace(page_id, frame_id);
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }

  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  memset(page->data_, 0, PAGE_SIZE);
  disk_manager_->ReadPage(page_id, page->data_);
  replacer_->Pin(frame_id);

  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> latch(latch_);
  auto pgtbl_iter = page_table_.find(page_id);
  if (pgtbl_iter == page_table_.end()) {
    return false;
  }

  auto p = &pages_[pgtbl_iter->second];
  if (p->pin_count_ <= 0) {
    return false;
  }

  p->is_dirty_ |= is_dirty;
  p->pin_count_--;

  if (p->pin_count_ == 0) {
    replacer_->Unpin(pgtbl_iter->second);
  }

  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> latch(latch_);
  auto pgtbl_iter = page_table_.find(page_id);
  if (page_id == INVALID_PAGE_ID || pgtbl_iter == page_table_.end()) {
    return false;
  }

  auto p = &pages_[pgtbl_iter->second];
  disk_manager_->WritePage(page_id, p->GetData());
  p->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> latch(latch_);
  Page *page = nullptr;
  frame_id_t frame_id = 0;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&frame_id)) {
      return page;
    }
  }

  page = &pages_[frame_id];

  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }

  *page_id = disk_manager_->AllocatePage();
  page_table_.erase(page->GetPageId());
  page_table_.emplace(*page_id, frame_id);
  page->page_id_ = *page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  memset(page->data_, 0, PAGE_SIZE);

  replacer_->Pin(frame_id);
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> latch(latch_);

  disk_manager_->DeallocatePage(page_id);

  auto pgt_iter = page_table_.find(page_id);
  if (pgt_iter == page_table_.end()) {
    return true;
  }

  auto p = &pages_[pgt_iter->second];
  // p->WLatch();
  if (p->pin_count_ > 0) {
    // p->WUnlatch();
    return false;
  }

  free_list_.emplace_back(pgt_iter->second);
  replacer_->Pin(pgt_iter->second);
  page_table_.erase(page_id);

  p->pin_count_ = 0;
  p->page_id_ = INVALID_PAGE_ID;
  p->is_dirty_ = false;
  memset(p->data_, 0, PAGE_SIZE);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> latch(latch_);
  for (auto &[page_id, frame_id] : page_table_) {
    auto p = &pages_[frame_id];
    if (p->IsDirty()) {
      disk_manager_->WritePage(page_id, p->GetData());
      p->is_dirty_ = false;
    }
  }
}

}  // namespace bustub
