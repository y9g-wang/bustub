//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  page_id_t new_header_page_id;
  BasicPageGuard page_guard = bpm_->NewPageGuarded(&new_header_page_id);
  WritePageGuard write_page_guard = page_guard.UpgradeWrite();
  auto *header_page = write_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
  header_page_id_ = new_header_page_id;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint64_t hashed_key = hash_fn_.GetHash(key);
  ReadPageGuard header_page_guard = bpm_->FetchPageRead(header_page_id_);
  const auto *header_page = header_page_guard.As<ExtendibleHTableHeaderPage>();

  uint32_t directory_index = header_page->HashToDirectoryIndex(hashed_key);
  if (!header_page->IsInit(directory_index)) {
    return false;
  }

  ReadPageGuard directory_page_guard = bpm_->FetchPageRead(header_page->GetDirectoryPageId(directory_index));
  const auto *directory_page = directory_page_guard.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_index = directory_page->HashToBucketIndex(hashed_key);

  if (directory_page->GetBucketPageId(bucket_index) == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard bucket_page_guard = bpm_->FetchPageRead(directory_page->GetBucketPageId(bucket_index));
  const auto *bucket_page = bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  V value;
  bool is_found = bucket_page->Lookup(key, value, cmp_);
  if (is_found) {
    result->emplace_back(value);
  }
  return is_found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hashed_key = hash_fn_.GetHash(key);
  WritePageGuard header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_index = header_page->HashToDirectoryIndex(hashed_key);

  ExtendibleHTableDirectoryPage *directory_page;
  WritePageGuard directory_page_guard;
  if (!header_page->IsInit(directory_index)) {
    page_id_t directory_page_id;
    directory_page_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
    header_page->SetDirectoryPageId(directory_index, directory_page_id);
    directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
    directory_page->Init(directory_max_depth_);
  } else {
    directory_page_guard = bpm_->FetchPageWrite(header_page->GetDirectoryPageId(directory_index));
    directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  }

  uint32_t bucket_index = directory_page->HashToBucketIndex(hashed_key);
  WritePageGuard bucket_page_guard;
  ExtendibleHTableBucketPage<K, V, KC> *bucket_page;
  if (directory_page->GetBucketPageId(bucket_index) == INVALID_PAGE_ID) {
    page_id_t bucket_page_id;
    bucket_page_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
    bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);
    directory_page->SetBucketPageId(bucket_index, bucket_page_id);
  } else {
    bucket_page_guard = bpm_->FetchPageWrite(directory_page->GetBucketPageId(bucket_index));
    bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }

  // split bucket
  while (bucket_page->IsFull()) {
    if (directory_page->GetLocalDepth(bucket_index) == directory_page->GetGlobalDepth()) {
      // grow directory
      // assume we won't need to increase global depth beyond max depth
      directory_page->IncrGlobalDepth();
    }

    // create new bucket for split image
    bucket_index = directory_page->HashToBucketIndex(hashed_key);
    uint32_t new_bucket_index = directory_page->GetSplitImageIndex(bucket_index);
    page_id_t new_bucket_page_id;
    WritePageGuard new_bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
    auto *new_bucket_page = new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);

    // redistribute the elements between the 2 buckets
    uint32_t i = 0;
    while (i < bucket_page->Size()) {
      K entry_key = bucket_page->KeyAt(i);
      uint32_t hashed_entry_key = hash_fn_.GetHash(entry_key);
      if (directory_page->HashToBucketIndex(hashed_entry_key) == bucket_index) {
        i++;
        continue;
      }
      bool result = new_bucket_page->Insert(bucket_page->KeyAt(i), bucket_page->ValueAt(i), cmp_);
      if (!result) {
        std::cout << "splitting bucket, failed to insert entry into new bucket" << std::endl;
      }
      bucket_page->RemoveAt(i);
    }

    directory_page->SetBucketPageId(new_bucket_index, new_bucket_page_id);
    directory_page->IncrLocalDepth(bucket_index);
    directory_page->SetLocalDepth(new_bucket_index, directory_page->GetLocalDepth(bucket_index));

    new_bucket_page_guard.Drop();
  }

  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint64_t hashed_key = hash_fn_.GetHash(key);
  WritePageGuard header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();

  uint32_t directory_index = header_page->HashToDirectoryIndex(hashed_key);
  if (!header_page->IsInit(directory_index)) {
    return false;
  }
  WritePageGuard directory_page_guard = bpm_->FetchPageWrite(header_page->GetDirectoryPageId(directory_index));
  auto *directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();

  uint32_t bucket_index = directory_page->HashToBucketIndex(hashed_key);
  if (directory_page->GetBucketPageId(bucket_index) == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(directory_page->GetBucketPageId(bucket_index));
  auto *bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bool is_removed = bucket_page->Remove(key, cmp_);

  // recursively merge buckets and shrink directory
  while (bucket_page->IsEmpty() && directory_page->GetGlobalDepth() > 0) {

    uint32_t split_image_bucket_index = directory_page->GetSplitImageIndex(bucket_index);
    page_id_t split_image_page_id = directory_page->GetBucketPageId(split_image_bucket_index);
    WritePageGuard split_image_page_guard = bpm_->FetchPageWrite(split_image_page_id);
    auto *split_image_bucket_page = split_image_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    if (!split_image_bucket_page->IsEmpty() ||
      directory_page->GetLocalDepth(bucket_index) != directory_page->GetLocalDepth(split_image_bucket_index)) {
      break;
    }

    directory_page->SetBucketPageId(split_image_bucket_index, directory_page->GetBucketPageId(bucket_index));
    directory_page->DecrLocalDepth(bucket_index);
    directory_page->DecrLocalDepth(split_image_bucket_index);

    if (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
    bucket_page_guard.Drop();
    split_image_page_guard.Drop();

    bucket_index = directory_page->HashToBucketIndex(hashed_key);
    bucket_page_guard = bpm_->FetchPageWrite(directory_page->GetBucketPageId(bucket_index));
    bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }

  return is_removed;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
