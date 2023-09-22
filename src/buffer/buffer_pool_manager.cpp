#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 * 1.     Search the page table for the requested page (P).
 * 1.1    If P exists, pin it and return it immediately.
 * 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
 *        Note that pages are always found from the free list first.
 * 2.     If R is dirty, write it back to the disk.
 * 3.     Delete R from the page table and insert P.
 * 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
//  LOG(INFO) << "BufferPoolManager::FetchPage() called." << std::endl;
  auto i = page_table_.find(page_id);
  if (i != page_table_.end()) {
    replacer_->Pin(i->first);
    pages_[i->first].pin_count_++;
//    LOG(INFO) << "BufferPoolManager::FetchPage() succeeded: " << "Page " << page_id << " found in frame " << i->second << "." << std::endl;
    return pages_ + i->second;
  }
  else {
    std::thread t;
    frame_id_t frame_id = TryToFindFreePage();
    if (frame_id == INVALID_FRAME_ID) {
//      LOG(ERROR) << "BufferPoolManager::FetchPage() crashed!" << std::endl;
      return nullptr;
    }
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    t = std::thread(&DiskManager::ReadPage, disk_manager_, page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].page_id_ = page_id;
    page_table_[page_id] = frame_id;
//    LOG(INFO) << "BufferPoolManager::FetchPage() succeeded: " << "Page " << page_id << " moved into frame " << frame_id << "." << std::endl;
    t.join();
    return pages_ + frame_id;
  }
}

/**
 * TODO: Student Implement
 * 0.   Make sure you call AllocatePage!
 * 1.   If all the pages in the buffer pool are pinned, return nullptr.
 * 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
 * 3.   Update P's metadata, zero out memory and add P to the page table.
 * 4.   Set the page ID output parameter. Return a pointer to P.
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
//  LOG(INFO) << "BufferPoolManager::NewPage() called."<< std::endl;
  if (replacer_->Size() + free_list_.size() > 0) {
    frame_id_t frame_id = TryToFindFreePage();
    if (frame_id == INVALID_FRAME_ID) {
//      LOG(ERROR) << "BufferPoolManager::NewPage() crashed!" << std::endl;
      return nullptr;
    }
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    page_id = AllocatePage();
    std::thread t(memset, pages_[frame_id].data_, 0, PAGE_SIZE);
    (pages_ + frame_id) -> pin_count_++;
    (pages_ + frame_id) -> is_dirty_ = true;
    (pages_ + frame_id) -> page_id_ = page_id;
    page_table_[page_id] = frame_id;
    replacer_->Unpin(frame_id);
    replacer_->Pin(frame_id);
//    LOG(INFO) << "BufferPoolManager::NewPage() succeeded: " << "FrameID: " << frame_id << ", PageID: " << page_id << std::endl;
    t.join();
    return pages_ + frame_id;
  }
  else {
//    LOG(INFO) << "BufferPoolManager::NewPage() failed: Buffer is completely filled!" << std::endl;
    return nullptr;
  }
}

/**
 * TODO: Student Implement
 * 0.   Make sure you call DeallocatePage!
 * 1.   Search the page table for the requested page (P).
 * 1.   If P does not exist, return true.
 * 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
 * 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
//  LOG(INFO) << "BufferPoolManager::DeletePage() called." << std::endl;
  auto i = page_table_.find(page_id);
  if (i != page_table_.end()) {
    if (pages_[i->second].pin_count_) {
//      LOG(INFO) << "BufferPoolManager::DeletePage() failed: " << "Page " << page_id << " is pinned." << std::endl;
      return false;
    }
    else {
      frame_id_t frame_id;
      if (replacer_->Victim(&frame_id)) {
        if (pages_[i->second].is_dirty_) {
          disk_manager_->WritePage(page_id, pages_[i->second].data_);
        }
        memcpy(pages_ + i->second, pages_ + frame_id, sizeof(Page));
        free_list_.push_back(frame_id);
        page_table_.erase(page_id);
//        LOG(INFO) << "BufferPoolManager::DeletePage() succeeded: " << std::endl;
      }
      else {
//        LOG(ERROR) << "BufferPoolManager::DeletePage() crashed!" << std::endl;
        return false;
      }
    }
  }
  else {
//    LOG(INFO) << "BufferPoolManager::DeletePage() succeeded: " << "Page " << page_id << " is not in the buffer." << std::endl;
    return true;
  }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
//  LOG(INFO) << "BufferPoolManager::UnpinPage() called." << std::endl;
  auto i = page_table_.find(page_id);
  if (i != page_table_.end()) {
    pages_[i->second].is_dirty_ = is_dirty;
    pages_[i->second].pin_count_--;
    replacer_->Unpin(i->second);
//    LOG(INFO) << "BufferPoolManager::UnpinPage() succeeded: " << "PageID: " << page_id << ", FrameID: " << i->second << std::endl;
    return true;
  }
  else {
//    LOG(INFO) << "BufferPoolManager::UnpinPage() failed: Page " << page_id << " not found." << std::endl;
    return false;
  }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto i = page_table_.find(page_id);
  if (i != page_table_.end()) {
    disk_manager_->WritePage(i->first, pages_[i->second].data_);
    pages_[i->first].is_dirty_ = false;
//    LOG(INFO) << "BufferPoolManager::FlushPage() succeeded: " << "PageID: " << page_id << ", FrameID: " << i->second << std::endl;
    return true;
  }
  else {
//    LOG(INFO) << "BufferPoolManager::FlushPage() failed: Page " << page_id << " not found." << std::endl;
    return false;
  }
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
//      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}

/**
* TODO: Student Implement
 */
frame_id_t BufferPoolManager::TryToFindFreePage() {
  frame_id_t frame_id;
  if (free_list_.empty()) {
    if (!replacer_->Victim(&frame_id)) {
      return INVALID_FRAME_ID;
    }
    else {
      page_table_.erase(pages_[frame_id].page_id_);
    }
  }
  else {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  return frame_id;
}