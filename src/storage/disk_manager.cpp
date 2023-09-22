#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  uint32_t bitmap_offset;
  uint16_t extent_offset;
  page_id_t logical_page_id;
  char bitmap_data_[PAGE_SIZE];
  BitmapPage<PAGE_SIZE>* bitmap_page_;
  auto meta_page_ = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
//  LOG(INFO) << "DiskManager::AllocatePage() called" << std::endl;
  ReadPhysicalPage(1 + next_free_extent_ * (BITMAP_SIZE + 1), bitmap_data_);
  bitmap_page_ = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_data_);
  if (bitmap_page_->AllocatePage(bitmap_offset)) {
    std::thread t(&DiskManager::WritePhysicalPage, this, 1 + next_free_extent_ * (BITMAP_SIZE + 1), bitmap_data_);
    logical_page_id = bitmap_offset + next_free_extent_ * BITMAP_SIZE;
    extent_offset = next_free_extent_;
    if (meta_page_->extent_used_page_[next_free_extent_] == 0) {
      meta_page_->num_extents_++;
    }
    meta_page_->extent_used_page_[next_free_extent_]++;
    meta_page_->num_allocated_pages_++;
    while (meta_page_->extent_used_page_[next_free_extent_] == BITMAP_SIZE) {
      next_free_extent_++;
      next_free_extent_ %= EXTENT_SIZE;
    }
//    LOG(INFO) << "DiskManager::AllocatePage() succeeded: " << "ExtentSize: " << EXTENT_SIZE
//              << ", ExtentOffset: " << extent_offset << ", BitmapOffset: " << bitmap_offset
//              << ", LogicPageID: " << logical_page_id << ", ExtentUsed: " << meta_page_->num_extents_
//              << ", PageAllocated: " << meta_page_->num_allocated_pages_ << ", NextFreeExtent: " << next_free_extent_ << std::endl;
    t.join();
    return logical_page_id;
  }
  else {
//    LOG(INFO) << "DiskManager::AllocatePage() failed." << std::endl;
    return INVALID_PAGE_ID;
  }
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  uint32_t bitmap_offset = logical_page_id % BITMAP_SIZE, extent_offset = logical_page_id / BITMAP_SIZE;
  char bitmap_data_[PAGE_SIZE];
  BitmapPage<PAGE_SIZE>* bitmap_page_;
  auto meta_page_ = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
//  LOG(INFO) << "DiskManager::DeAllocatePage() called." << std::endl;
  ReadPhysicalPage(1 + extent_offset * (BITMAP_SIZE + 1), bitmap_data_);
  bitmap_page_ = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_data_);
  if (bitmap_page_->DeAllocatePage(bitmap_offset)) {
    std::thread t(&DiskManager::WritePhysicalPage, this, 1 + extent_offset * (BITMAP_SIZE + 1), bitmap_data_);
    next_free_extent_ = extent_offset;
    meta_page_->extent_used_page_[extent_offset]--;
    meta_page_->num_allocated_pages_--;
    if (meta_page_->extent_used_page_[extent_offset] == 0) {
      meta_page_->num_extents_--;
    }
//    LOG(INFO) << "DiskManager::DeAllocatePage() succeeded: " << "ExtentSize: " << EXTENT_SIZE
//              << ", ExtentOffset: " << extent_offset << ", BitmapOffset: " << bitmap_offset
//              << ", LogicPageID: " << logical_page_id << ", ExtentUsed: " << meta_page_->num_extents_
//              << ", PageAllocated: " << meta_page_->num_allocated_pages_ << ", NextFreeExtent: " << next_free_extent_ << std::endl;
    WritePhysicalPage(1 + extent_offset * (BITMAP_SIZE + 1), bitmap_data_);
    t.join();
  }
  else {
//    LOG(WARNING) << "DiskManager::DeAllocatePage() failed." << std::endl;
  }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  uint32_t bitmap_offset = logical_page_id % BITMAP_SIZE, extent_offset = logical_page_id / BITMAP_SIZE;
  char bitmap_data_[PAGE_SIZE];
  BitmapPage<PAGE_SIZE>* bitmap_page_;
  DiskFileMetaPage* meta_page_ = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
  ReadPhysicalPage(1 + extent_offset * (BITMAP_SIZE + 1), bitmap_data_);
  bitmap_page_ = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_data_);
  return bitmap_page_->IsPageFree(bitmap_offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  uint32_t bitmap_offset = logical_page_id % BITMAP_SIZE, extent_offset = logical_page_id / BITMAP_SIZE;
  return 1 + extent_offset * (BITMAP_SIZE + 1) + bitmap_offset + 1;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}