//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <backward-cpp/backward.hpp>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
public:
 LRUKNode() = default;

 LRUKNode(const size_t k, const frame_id_t fid) : k_(k), fid_(fid) {}

 // Compares LRUKNode via their backward k-distance.
 [[nodiscard]] bool hasLargerBackwardKDistance (LRUKNode const& rhs, size_t current_timestamp) const {
  if (getBackwardKDistance(current_timestamp) != rhs.getBackwardKDistance(current_timestamp)) {
    return getBackwardKDistance(current_timestamp) > rhs.getBackwardKDistance(current_timestamp);
  }

  // Only possible when the backward k-distance of both frames are +inf.
  // The frame with the smaller least-recent access is considered as having
  // the larger backward k-distance.
  return history_.front() < rhs.history_.front();
 }

 [[nodiscard]] frame_id_t getFrameID() const {
  return fid_;
 }

 void setFrameID(frame_id_t const frame_id) {
  fid_ = frame_id;
 }

 void recordAccess(size_t const currrent_time) {
  history_.push_back(currrent_time);

  while (history_.size() > k_) {
   history_.pop_front();
  }
 }

 void setEvictable(const bool isEvictable) {
  is_evictable_ = isEvictable;
 }

 [[nodiscard]] bool isEvictable() const {
  return is_evictable_;
 }

 // getBackwardKDistance returns the backward k-distance, if it is available.
 // Otherwise, it returns +inf.
 [[nodiscard]] size_t getBackwardKDistance(size_t const current_timestamp) const {
  if (history_.size() == k_) {
   return current_timestamp - history_.front();
  }
  return std::numeric_limits<int>::max();
 }


 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */

  // history_ stores the timestamp of the past accesses.
  std::list<size_t> history_;

  // k_ stores the maximum number of past accesses.
  size_t k_{};

  // fid_ stores the frame id.
  // Default value is 0.
  frame_id_t fid_{0};

  // is_evictable_ stores whether the frame is evictable.
  // Default value is false.
  bool is_evictable_{false};
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() {

  };

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() const -> size_t;

 private:

  // node_store stores the access history of the frames that are available in memory.
  std::unordered_map<frame_id_t, std::shared_ptr<LRUKNode>> node_store_;

  // current_timestamp_ stores the current logical timestamp.
  // It is incremented after every operation.
  // This helps to uniquely indentify each frame access.
  size_t current_timestamp_{0};

  // curr_size_ stores the current number of evictable frames.
  size_t curr_size_{0};

  // replacer_size_ stores the maximum number of frames that needs to be stored.
  // It should not be modified after initialization.
  [[maybe_unused]] const size_t replacer_size_;

  // k_ stores the maximum number of past accesses that needs to be recorded for each frame.
  size_t k_;

  // latch_ stores the mutex that ensure that is no race condition.
  std::mutex latch_;
};

}  // namespace bustub
