//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();

  frame_id_t frame_id;
  // no free frame_id means that bpm don't have space to read page from disk to memory
  if (!GetFreeFrame(&frame_id)) {
    latch_.unlock();
    return nullptr;
  }

  Page *free_page = GetPageByPageID(INVALID_PAGE_ID);
  if (!free_page) {
    throw Exception("free list is not empty, but unable to find free page");
  }

  page_id_t new_page_id = AllocatePage();

  // update page_table_
  page_table_[new_page_id] = frame_id;

  // update free page
  free_page->page_id_ = new_page_id;
  free_page->is_dirty_ = false;
  free_page->pin_count_ = 1;

  // update replacer_
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  *page_id = new_page_id;

  latch_.unlock();
  return free_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();

  Page *page = GetPageByPageID(page_id);
  if (page) {
    // pin_count is not incremented during page access,
    // which may cause pin_count to be lower than expected.
    // Will not add this code yet to focus on higher priority tasks
    //
    page->pin_count_++;
    frame_id_t frame_id = page_table_[page_id];
    replacer_->RecordAccess(frame_id);
    latch_.unlock();
    return page;
  }

  // failed to find page in bpm, so try to read it from disk
  // try to find a free frame_id
  frame_id_t frame_id;
  // no free frame_id means that bpm don't have space to read page from disk to memory
  if (!GetFreeFrame(&frame_id)) {
    latch_.unlock();
    return nullptr;
  }
  // managed to find unused frame_id and cleaned up

  Page *free_page = GetPageByPageID(INVALID_PAGE_ID);
  if (!free_page) {
    throw Exception("free list is not empty, but unable to find free page");
  }

  // managed to find an unused page to store data from page_id
  // update page_table_
  page_table_[page_id] = frame_id;

  // read data from page_id into memory into free_page
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule(DiskRequest{false, free_page->GetData(), page_id, std::move(promise)});
  if (!future.get()) {
    throw Exception("failed to read from disk");
  }
  free_page->page_id_ = page_id;
  free_page->is_dirty_ = false;
  free_page->pin_count_ = 1;

  // update replacer_
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  latch_.unlock();
  return free_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  Page *page = GetPageByPageID(page_id);
  if (!page || page->GetPinCount() == 0) {
    latch_.unlock();
    return false;
  }

  page->pin_count_--;
  page->is_dirty_ |= is_dirty;

  if (!page->GetPinCount()) {
    MustEvictPage(page);
    latch_.unlock();
    return true;
  }

  latch_.unlock();
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  Page *page = GetPageByPageID(page_id);
  if (!page) {
    latch_.unlock();
    return false;
  }
  page->pin_count_ = 0;
  MustEvictPage(page);

  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[static_cast<int>(i)].page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    pages_[static_cast<int>(i)].pin_count_ = 0;
    MustEvictPage(&pages_[static_cast<int>(i)]);
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();

  Page *page = GetPageByPageID(page_id);
  if (!page) {
    latch_.unlock();
    return true;
  }
  if (page->GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }
  MustEvictPage(page);
  DeallocatePage(page_id);

  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // FetchPage may return nullptr
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  // FetchPage may return nullptr
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  // FetchPage may return nullptr
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  // NewPage may return nullptr
  return {this, NewPage(page_id)};
}

}  // namespace bustub
