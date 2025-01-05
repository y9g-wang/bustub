//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (int & bucket_page_id : bucket_page_ids_) {
    bucket_page_id = INVALID_PAGE_ID;
  }

  for (uint8_t &local_depth : local_depths_) {
    local_depth = 0;
  }
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  return (1 << global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1 << local_depths_[bucket_idx]) - 1;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= Size()) {
    throw Exception("bucket_idx is outside the bucket range");
  }

  // if global_depth_ is 0, bucket_idx is its own split image
  if (global_depth_ == 0) {
    return 0;
  }
  return bucket_idx ^ (1 << (global_depth_-1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ == max_depth_) {
    throw Exception("global depth equals to max depth");
  }
 uint32_t initial_size = Size();
  global_depth_++;

  uint8_t new_local_depths[HTABLE_DIRECTORY_ARRAY_SIZE];
  page_id_t new_bucket_page_ids[HTABLE_DIRECTORY_ARRAY_SIZE];

  for (int & bucket_page_id : new_bucket_page_ids) {
    bucket_page_id = INVALID_PAGE_ID;
  }

  for (uint8_t &local_depth : new_local_depths) {
    local_depth = 0;
  }

  for (uint32_t i = 0; i < initial_size; i++) {
    new_local_depths[i] = local_depths_[i];
    new_local_depths[GetSplitImageIndex(i)] = local_depths_[i];

    new_bucket_page_ids[i] = bucket_page_ids_[i];
    new_bucket_page_ids[GetSplitImageIndex(i)] = bucket_page_ids_[i];
  }

  std::copy(new_local_depths, new_local_depths+HTABLE_DIRECTORY_ARRAY_SIZE, local_depths_);
  std::copy(new_bucket_page_ids, new_bucket_page_ids+HTABLE_DIRECTORY_ARRAY_SIZE, bucket_page_ids_);
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ == 0) {
    throw Exception("global depth equals to zero");
  }
  uint32_t initial_size = Size();
  global_depth_--;
  for (uint32_t i = Size(); i < initial_size; i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  // number of entries in directory
  return 1 << global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub
