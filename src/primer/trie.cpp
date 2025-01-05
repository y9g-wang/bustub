#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  auto node = this->root_;
  if (this->root_ == nullptr) {
    return nullptr;
  }
  try {
    for (auto c: key) {
      node = node->children_.at(c);
    }
  } catch (...) {
    return nullptr;
  }
  auto nodeWithValue = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (nodeWithValue == NULL) {
    return nullptr;
  }
  return nodeWithValue->value_.get();

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  std::shared_ptr<TrieNode> node = std::make_shared<TrieNode>();
  if (this->root_ != nullptr) {
    node = std::shared_ptr<TrieNode>(this->root_->Clone());
  }
  auto trie = Trie(node);

  if (key.length() == 0) {
      std::shared_ptr<TrieNodeWithValue<T>> newNode = std::make_shared<TrieNodeWithValue<T>>(
        node -> children_, std::make_shared<T>(std::move(value)));
      return Trie(newNode);
  }

  for (std::size_t i = 0; i < key.length(); i++) {
    char c = key[i];
    if (node->children_[c] == nullptr) {
      if (i == key.length() - 1) {
        auto newNode = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
        node ->children_[c] = newNode;
        node = newNode;
      } else {
        auto newNode = std::make_shared<TrieNode>();
        node ->children_[c] = newNode;
        node = newNode;
      }
    } else {
      if (i == key.length() -1) {
        std::shared_ptr<TrieNodeWithValue<T>> newNode = std::make_shared<TrieNodeWithValue<T>>(
          node -> children_[c]->children_, std::make_shared<T>(std::move(value)));
        node -> children_[c] = newNode;
        node = newNode;
      } else {
        // moving a temporary object prevents copy elision
        auto newNode = std::shared_ptr<TrieNode>(node->children_[c]->Clone());
        node ->children_[c] = newNode;
        node = newNode;
      }
    }
  }
  return trie;
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  std::shared_ptr<TrieNode> node = std::make_shared<TrieNode>();
  if (this->root_ != nullptr) {
    node = std::shared_ptr<TrieNode>(this->root_->Clone());

    if (key.empty()) {
      node = std::make_shared<TrieNode>(this->root_->children_);
      return Trie(node);
    }
  }
  auto trie = Trie(node);



  std::vector<std::shared_ptr<TrieNode>> v {node};
  for (std::size_t i = 0; i < key.size(); i++) {
    char c = key[i];
    if (node->children_[c] == nullptr) {
      return trie;
    }
    if (i == key.size()-1) {
      if (node->children_[c]->children_.empty()) {
        node->children_.erase(c);
        for (size_t j = i; j >= 0; j--) {
          char cc = key[j];
          auto n = v.back();
          if (node->children_.empty() && !node->is_value_node_) {
            n->children_.erase(cc);
          } else {
            return trie;
          }
          node = n;
          v.pop_back();
          if (j == 0) {
            if (!trie.root_->is_value_node_ && trie.root_->children_.empty()) {
              trie.root_ = nullptr;
              return trie;
            }
            break;
          }
        }
      } else {
        auto newNode = std::make_shared<TrieNode>(node->children_[c]->children_);
        node ->children_[c] = newNode;
        node = newNode;
      }
    } else {
      auto newNode = std::shared_ptr<TrieNode>(node->children_[c]->Clone());
      node ->children_[c] = newNode;
      node = newNode;
    }
    v.emplace_back(node);
  }

  return trie;
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
