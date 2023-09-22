#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ < MAX_CHARS * 8){
    // Set corresponding bit to 1
    uint32_t byte_index = this->next_free_page_ / 8;
    uint8_t bit_index = this->next_free_page_ % 8;
    unsigned char *byte = this->bytes + byte_index;
    unsigned char mask = '\01' << bit_index;
    *byte |= mask;
    if (IsPageFree(next_free_page_)) {
      // case 1: bit set failed
//      LOG(ERROR) << "BitmapPage::AllocatePage() crashed!" << std::endl;
      return false;
    } else {
      // case 2: bit set successfully, update status variables
      page_offset = next_free_page_;
      this->page_allocated_++;
      UpdateNextFreePage();
//      LOG(INFO) << "BitmapPage::AllocatePage() succeeded: " << "PageSize: " << PageSize << ", page_offset: "
//                << page_offset << ", page_allocated: " << page_allocated_ << ", next_free_page_: " << next_free_page_ << std::endl;
      return true;
    }
  } else {
    // case 3: bitmap page is full, no operation
//    LOG(INFO) << "BitmapPage::AllocatePage() failed: Bitmap Page is full." << std::endl;
    return false;
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (!(this->IsPageFree(page_offset))) {
    // set corresponding bit to 0
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    unsigned char* byte = this->bytes + byte_index;
    unsigned char mask = '\01' << bit_index;
    *byte &= ~mask;
    if (this->IsPageFree(page_offset)) {
      // case 1: bit set successfully, update status variables
      this->next_free_page_ = page_offset;
      this->page_allocated_--;
//      LOG(INFO) << "BitmapPage::DeAllocatePage() succeeded: " << "PageSize: " << PageSize
//                << ", page_offset: " << page_offset << ", page_allocated: " << page_allocated_ << std::endl;
      return true;
    } else {
      // case 2: bit set failed
//      LOG(ERROR) << "BitmapPage::AllocatePage() crashed!" << std::endl;
      return false;
    }
  } else {
    // case 3: page is already free, no operation
//    LOG(INFO) << "BitmapPage::DeAllocatePage() failed: Page " << page_offset << " is already free." << std::endl;
    return false;
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  unsigned char byte = this->bytes[byte_index];
  unsigned char mask = '\01' << bit_index;
  return ! (byte & mask);
}

/**
 * Student added function
 * @param this: whose `next_free_page` would be updated to proper value by this function
 */
template <size_t PageSize>
void BitmapPage<PageSize>::UpdateNextFreePage() {
  uint32_t byte_index;
  if (page_allocated_ == MAX_CHARS * 8) return;
  while (!IsPageFree(next_free_page_)) {
    next_free_page_++;
    if (next_free_page_ % 8 == 0) {
      byte_index = next_free_page_ / 8 % MAX_CHARS;
      while (bytes[byte_index] == '\377') {
        byte_index++;
        byte_index %= MAX_CHARS;
      }
      next_free_page_ = byte_index * 8;
      while (!IsPageFree(next_free_page_)) {
        next_free_page_++;
      }
      break;
    }
  }

}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;