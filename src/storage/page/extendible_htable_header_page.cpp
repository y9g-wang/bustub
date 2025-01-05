//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  for (int & directory_page_id : directory_page_ids_) {
    directory_page_id = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  uint32_t num_bits_shifts = sizeof(hash) * CHAR_BIT - max_depth_;
  while (num_bits_shifts) {
    hash /= 2;
    num_bits_shifts--;
  }
  return hash;
}

auto ExtendibleHTableHeaderPage::IsInit(uint32_t directory_idx) const -> bool {
  return directory_page_ids_[directory_idx] != INVALID_PAGE_ID;
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t {
  return 1 << max_depth_;
}

}  // namespace bustub
