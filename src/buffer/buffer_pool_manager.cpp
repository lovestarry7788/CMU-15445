//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {

  /*
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");
  */

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  // 1. 获取空闲的 frame
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {  // 能找到剔除的页面
    auto page = pages_ + frame_id;
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    page->ResetMemory();
    page_table_->Remove(page->page_id_);
  } else {
    return nullptr;
  }

  // 2. 获取 page_id
  *page_id = AllocatePage();
  auto page = pages_ + frame_id;
  page->page_id_ = *page_id;
  page->pin_count_++;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(*page_id, frame_id);

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;
  bool can_find = page_table_->Find(page_id, frame_id);
  // 1. 找到页面，直接返回
  if (can_find) {
    auto page = pages_ + frame_id;
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  // 2. 获取空闲的 frame
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    auto page = pages_ + frame_id;
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    page->ResetMemory();
    page_table_->Remove(page->page_id_);
  } else {
    return nullptr;
  }

  // 3. 获取 page_id
  auto page = pages_ + frame_id;
  page->page_id_ = page_id;
  page->pin_count_++;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  disk_manager_->ReadPage(page_id, page->data_);
  page_table_->Insert(page_id, frame_id);

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  bool can_find = page_table_->Find(page_id, frame_id);
  if (can_find) {
    auto page = pages_ + frame_id;
    if (page->pin_count_ <= 0) {
      return false;
    }
    page->pin_count_--;
    if (page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
      page->is_dirty_ |= is_dirty;
    }
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  bool can_find = page_table_->Find(page_id, frame_id);
  if (can_find) {
    auto page = pages_ + frame_id;
    disk_manager_->WritePage(page_id, page->data_);
    page->is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  bool can_find = page_table_->Find(page_id, frame_id);
  if (can_find) {
    return true;
  }
  auto page = pages_ + frame_id;
  if (page->pin_count_ != 0) {
    return false;
  }
  if (page->IsDirty()) {
    page->is_dirty_ = false;
    disk_manager_->WritePage(page_id, page->GetData());
  }
  page_table_->Remove(pages_[frame_id].page_id_);
  free_list_.emplace_back(frame_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  DeallocatePage(page_id);
  return true;
}

/**
 * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
 * @return the id of the allocated page
 */
auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
