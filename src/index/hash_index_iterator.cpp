//
// Created by PaperCloud on 2023/6/7.
//
#include "index/hash_index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

HashIndexIterator::HashIndexIterator() = default;

HashIndexIterator::HashIndexIterator(map<int, page_id_t> *mp, int key_v, page_id_t page_id, BufferPoolManager *bpm, int index) :
  current_page_id(page_id), mp_(mp), current_key_v(key_v), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<HashPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

HashIndexIterator::~HashIndexIterator() {
  if(current_page_id != INVALID_PAGE_ID) {
    buffer_pool_manager->UnpinPage(current_page_id, false);
  }
}

std::pair<GenericKey *, RowId> HashIndexIterator::operator*() {
  return page->GetItem(item_index);
}

HashIndexIterator &HashIndexIterator::operator++() {
  if(++item_index == page->GetSize() && page->GetNextPageId() != INVALID_PAGE_ID) {
    auto * next_page = reinterpret_cast<HashPage *>
        (buffer_pool_manager->FetchPage(page->GetNextPageId())->GetData());
    current_page_id = page->GetNextPageId();
    buffer_pool_manager->UnpinPage(page->GetPageId(), false);
    page = next_page;
    item_index = 0;
  } if(item_index == page->GetSize()) {
    buffer_pool_manager->UnpinPage(current_page_id, false);
    auto next = mp_->upper_bound(current_key_v);
    if(next != mp_->end()) {
      current_page_id = next->second;
      current_key_v = next->first;
      page = reinterpret_cast<HashPage *>
             (buffer_pool_manager->FetchPage(current_page_id)->GetData());
      item_index = 0;
      ASSERT(item_index < page->GetSize(), "empty page exists");
    } else {
      LOG(ERROR) << "HashIndexIterator::operator++ : out of range!";
      return *this=HashIndexIterator();
    }
  }
  return *this;
}

bool HashIndexIterator::operator==(const HashIndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool HashIndexIterator::operator!=(const HashIndexIterator &itr) const { return !(*this == itr); }
