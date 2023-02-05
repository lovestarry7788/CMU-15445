//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  V val;
  if (!(dir_[index]->Find(key, val))) {
    if (dir_[index]->IsFull()) {
      int local_depth = dir_[index]->GetDepth();
      if (local_depth == global_depth_) {
        dir_.reserve(dir_.size() << 1);
        global_depth_++;
        std::copy_n(dir_.begin(), dir_.size(), std::back_inserter(dir_));  // 复制直至最后一个 iterator
      }
      num_buckets_++;
      auto bucket0 = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
      auto bucket1 = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
      int local_mask = 1 << local_depth;
      for (const auto &[k, v] : dir_[index]->GetItems()) {
        if (std::hash<K>()(k) & local_mask) {
          bucket1->Insert(k, v);
        } else {
          bucket0->Insert(k, v);
        }
      }
      for (size_t i = (std::hash<K>()(key) & (local_mask - 1)); i < dir_.size(); i += local_mask) {
        if ((i & local_mask) != 0U) {
          dir_[i] = bucket1;
        } else {
          dir_[i] = bucket0;
        }
      }
      index = IndexOf(key);
    }
    dir_[index]->Insert(key, value);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (const auto &[k, v] : list_) {
    if (key == k) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto iter = list_.begin(); iter != list_.end(); ++iter) {
    if ((*iter).first == key) {
      list_.erase(iter);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto &[k, v] : list_) {
    if (key == k) {
      v = value;
      return true;
    }
  }
  if (!IsFull()) {
    list_.emplace_back(key, value);
    // push_back 可以直接传 {key, value}
    // emplace_back 需要传 key,value ，使用可变参数和完美转换。
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
