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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t const num_frames, size_t const k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (node_store_.empty()) {
    return false;
  }

  // using O(N) algorithm to find the target frame. Can probably be optimized using a priority queue or balanced binary
  // search tree e.g. red black tree e.g. map data structure in c++.
  std::shared_ptr<LRUKNode> target;
  for (std::pair<frame_id_t, std::shared_ptr<LRUKNode>> node: node_store_) {
    if (!node.second->isEvictable()) {
      continue;
    }

    if (target == nullptr || node.second->hasLargerBackwardKDistance(*target, current_timestamp_)) {
      target = node.second;
    }
  }

  if (target == nullptr) {
    return false;
  }

  *frame_id = target->getFrameID();
  node_store_.erase(target->getFrameID());
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(const frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (node_store_.count(frame_id) == 0) {
    node_store_[frame_id] = std::make_shared<LRUKNode>(LRUKNode(k_, frame_id));
  }
  std::shared_ptr<LRUKNode> target = node_store_[frame_id];
  target->recordAccess(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(const frame_id_t frame_id, const bool set_evictable) {
  if (node_store_.count(frame_id) == 0) {
    return;
  }

  std::shared_ptr<LRUKNode> target = node_store_[frame_id];
  if (!target->isEvictable() && set_evictable) {
    curr_size_++;
  } else if (target->isEvictable() && !set_evictable) {
    curr_size_--;
  }
  target->setEvictable(set_evictable);
}

void LRUKReplacer::Remove(const frame_id_t frame_id) {
  if (node_store_.count(frame_id) == 0) {
    return;
  }

  std::shared_ptr<LRUKNode> target = node_store_[frame_id];
  if (!target->isEvictable()) {
    throw Exception("target frame is not evictable");
  }

  curr_size_--;
  node_store_.erase(target->getFrameID());
}

auto LRUKReplacer::Size() const -> size_t {
  return curr_size_;
}

}  // namespace bustub
