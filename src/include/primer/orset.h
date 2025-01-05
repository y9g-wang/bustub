#pragma once

#include <string>
#include <unordered_set>
#include <vector>

namespace bustub {

template <typename T>
struct pair_hash {
 inline size_t operator()(const std::pair<T, uid_t> &v) const {
  std::hash<T> hasherT1;
  std::hash<uid_t> hasherUID;
  return hasherT1(v.first) ^ hasherUID(v.second);
 }
};

/** @brief Unique ID type. */
using uid_t = int64_t;

/** @brief The observed remove set datatype. */
template <typename T>
class ORSet {
 public:
  ORSet() = default;

  /**
   * @brief Checks if an element is in the set.
   *
   * @param elem the element to check
   * @return true if the element is in the set, and false otherwise.
   */
  auto Contains(const T &elem) const -> bool;

  /**
   * @brief Adds an element to the set.
   *
   * @param elem the element to add
   * @param uid unique token associated with the add operation.
   */
  void Add(const T &elem, uid_t uid);

  /**
   * @brief Removes an element from the set if it exists.
   *
   * @param elem the element to remove.
   */
  void Remove(const T &elem);

  /**
   * @brief Merge changes from another ORSet.
   *
   * @param other another ORSet
   */
  void Merge(const ORSet<T> &other);

  /**
   * @brief Gets all the elements in the set.
   *
   * @return all the elements in the set.
   */
  auto Elements() const -> std::vector<T>;

  /**
   * @brief Gets a string representation of the set.
   *
   * @return a string representation of the set.
   */
  auto ToString() const -> std::string;

 private:
  // TODO(student): Add your private memeber variables to represent ORSet.
  std::unordered_set<std::pair<T, uid_t>, pair_hash<T>> elements;
  std::unordered_set<std::pair<T, uid_t>, pair_hash<T>> tombstone;
};

}  // namespace bustub
