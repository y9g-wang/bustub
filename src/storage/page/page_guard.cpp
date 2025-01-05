#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "fmt/chrono.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_  = that.page_;
}

void BasicPageGuard::Drop() {
  if (!bpm_) {
    return;
  }
  bool is_unpinned = bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  if (!is_unpinned) {
    std::cout << "dropping basic page guard, did not evict page" << std::endl;
  }
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ |= that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  Drop();
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard temp (bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return temp;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard temp  = bpm_->FetchPageWrite(page_->GetPageId());
  return temp;
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {
  guard_ = BasicPageGuard(bpm,page);
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (!guard_.page_) {
    return;
  }
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {
  guard_ = BasicPageGuard(bpm, page);
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {

  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (!guard_.page_) {
    return;
  }
  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  Drop();
}  // NOLINT

}  // namespace bustub
