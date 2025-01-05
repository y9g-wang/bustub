#include "primer/orset.h"
#include <algorithm>
#include <string>
#include <vector>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  // TODO(student): Implement this
  for (std::pair<T, uid_t> p: this->elements) {
    if (p.first == elem) {
        return true;
    }
  }
  return false;
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // TODO(student): Implement this
  std::pair<T, uid_t> p {elem, uid};
  this->elements.insert(p);
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this

  std::vector<std::pair<T, uid_t>> to_delete;
  for (const std::pair<T, uid_t>& p: this->elements) {
    if (p.first == elem) {
      this->tombstone.insert(p);
      to_delete.push_back(p);
    }
  }

  for (const std::pair<T, uid_t>& p: to_delete) {
      this->elements.erase(p);
  }
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this
  // remove elements in tombstone from other.elements -> s1
  // remove other.tombstone from elements -> s2
  // elements = merge s1 & s2
  // tombstone = merge tombstone & other.tombstone

  std::unordered_set<std::pair<T, uid_t>, pair_hash<T>> s1 = other.elements;
  for (std::pair<T, uid_t> p: this->tombstone) {
    s1.erase(p);
  }

  for (const std::pair<T, uid_t>& p: other.tombstone) {
    this->elements.erase(p);
  }

  for (const std::pair<T, uid_t>& p: s1) {
    this->elements.insert(p);
  }

  for (const std::pair<T, uid_t>& p: other.tombstone) {
    this->tombstone.insert(p);
  }

}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this
  std::unordered_set<T> deduped;
  for (const std::pair<T, uid_t>& p: this->elements) {
    deduped.insert(p.first);
  }

  std::vector<T> res;
  for (const T& ele: deduped) {
    res.push_back(ele);
  }

  return res;
  // dedup then store in vector
}

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;



}  // namespace bustub
