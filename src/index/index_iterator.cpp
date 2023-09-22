#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return page->GetItem(item_index);
//  ASSERT(false, "Not implemented yet.");
}

IndexIterator &IndexIterator::operator++() {
//  static int rx = 0;
//  ++rx; LOG(INFO) << "rx = " << rx ;
  if(++item_index == page->GetSize() && page->GetNextPageId() != INVALID_PAGE_ID) {
//    LOG(INFO) << "IndexIterator : move to right page";
    auto * next_page = reinterpret_cast<::LeafPage *>
        (buffer_pool_manager->FetchPage(page->GetNextPageId())->GetData());
    current_page_id = page->GetNextPageId();
//    LOG(INFO) << "before : ";
    buffer_pool_manager->UnpinPage(page->GetPageId(), false);
    page = next_page;
//    LOG(INFO) << "after : ";
    item_index = 0;
  } if(item_index == page->GetSize()) {
    // End() = default;
    buffer_pool_manager->UnpinPage(current_page_id, false);
    current_page_id = INVALID_PAGE_ID;
    page = nullptr;
    item_index = 0;
    *this = IndexIterator();
  }
  return *this;
//  ASSERT(false, "Not implemented yet.");
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}