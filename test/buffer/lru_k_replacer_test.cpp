/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <common/exception.h>

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, No_Evictable_Frame_Test) {
  LRUKReplacer lru_replacer(3, 2);

  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);

  int value;
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());
}
TEST(LRUKReplacerTest, Evict_Frame_Success_Test) {
  LRUKReplacer lru_replacer(3, 2);

  // Scenario: add three elements to the replacer. We have [1,2,3]. All are evictable and have +inf backward k-distance.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);

  int value;
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(1, value);
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(2, value);
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(3, value);
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());

  // Scenario: add three elements to the replacer. We have [1,2,3]. All are evictable.
  // Some have +inf backward k-distance.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.RecordAccess(1); // [2,3,1]
  lru_replacer.RecordAccess(3); // [2,1,3]

  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(2, value);
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(1, value);
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(3, value);
}
TEST(LRUKReplacerTest, Remove_Frame_Success_Test) {
  LRUKReplacer lru_replacer(3, 2);

  // Scenario: add three elements to the replacer. We have [1,2]. Frame 3 is non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);


  int value;
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(1, value);

  ASSERT_EQ(1, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(2, value);

  ASSERT_EQ(0, lru_replacer.Size());
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());

  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Remove(3);
  ASSERT_EQ(0, lru_replacer.Size());
}
TEST(LRUKReplacerTest, Remove_Nonevictable_Frame_Test) {
  LRUKReplacer lru_replacer(3, 2);

  // Scenario: add one elements to the replacer. Frame 1 is non-evictable.
  lru_replacer.RecordAccess(1);

  // Frame 1 cannot be evicted.
  int value;
  ASSERT_EQ(0, lru_replacer.Size());
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());

  // Frame 1 cannot be removed.
  bool exceptionThrown = false;
  try {
    lru_replacer.Remove(1);
  } catch (Exception& e) {
    exceptionThrown = true;
  }
  ASSERT_EQ(true, exceptionThrown);
}
TEST(LRUKReplacerTest, Evict_7_2_Test) {
  LRUKReplacer lru_replacer(7, 2);

  // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);
  ASSERT_EQ(5, lru_replacer.Size());

  // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
  // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
  lru_replacer.RecordAccess(1);

  // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
  // first based on LRU.
  int value;
  lru_replacer.Evict(&value);
  ASSERT_EQ(2, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);
  ASSERT_EQ(2, lru_replacer.Size());

  // Scenario: Now replacer has frames [5,1].
  // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
  lru_replacer.RecordAccess(3); // [5,3,1]
  lru_replacer.RecordAccess(4); // [5,3,4,1]
  lru_replacer.RecordAccess(5); // [3,4,1,5]
  lru_replacer.RecordAccess(4); // [3,1,5,4]
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());

  // Scenario: continue looking for victims. We expect 3 to be evicted next.
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(4, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(6, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Now we have [1,5,4]. Continue looking for victims.
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, lru_replacer.Size());

  // Update access history for 1. Now we have [4,1]. Next victim is 4.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(value, 4);

  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(value, 1);
  ASSERT_EQ(0, lru_replacer.Size());

  // This operation should not modify size
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());
}

}  // namespace bustub
