//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

// 和其它可剔除的页面进行比较（需要多加一个标记）
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  bool evict_success{false};
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (entries_[(*it)].evictable_) {
      (*frame_id) = (*it);
      list_.erase(it);
      evict_success = true;
      break;
    }
  }

  if (!evict_success) {
    for (auto it = klist_.begin(); it != klist_.end(); ++it) {
      if (entries_[(*it)].evictable_) {
        (*frame_id) = (*it);
        klist_.erase(it);
        evict_success = true;
        break;
      }
    }
  }

  if (evict_success) {
    entries_.erase(entries_.find(*frame_id));
    --curr_size_;
    return true;
  }

  return false;
}

// 更新记录
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t count = ++entries_[frame_id].count_;
  // 1. list_ FIFO，已经进了的不用再进
  // 2. klist_ LRU，
  if (count == 1) {
    curr_size_++;
    list_.emplace_back(frame_id);
    entries_[frame_id].pos_ = (--list_.end());
  } else if (count == k_) {
    list_.erase(entries_[frame_id].pos_);
    klist_.emplace_back(frame_id);
    entries_[frame_id].pos_ = (--klist_.end());
  } else if (count > k_) {
    klist_.erase(entries_[frame_id].pos_);
    klist_.emplace_back(frame_id);
    entries_[frame_id].pos_ = (--klist_.end());
  }
}

// 标记某一个 Frame 是否可以被驱逐
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (entries_.find(frame_id) == entries_.end()) {
    return;
  }

  if (entries_[frame_id].evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!entries_[frame_id].evictable_ && set_evictable) {
    curr_size_++;
  }

  entries_[frame_id].evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (entries_.find(frame_id) == entries_.end()) {
    return;
  }

  if (!entries_[frame_id].evictable_) {
    throw std::logic_error(std::string("Can't remove a inevictable frame ") + std::to_string(frame_id));
  }

  if (entries_[frame_id].count_ < k_) {
    list_.erase(entries_[frame_id].pos_);
  } else {
    klist_.erase(entries_[frame_id].pos_);
  }

  curr_size_--;
  entries_.erase(entries_.find(frame_id));
}

// 可以被驱逐的数量
auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
