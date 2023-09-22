//
// Created by PaperCloud on 2023/6/7.
//
#include "index/hash_table.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
#include "page/hash_page.h"

HashTable::HashTable(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &comparator,
          int max_tuple_size)
    : index_id_(index_id), buffer_pool_manager_(buffer_pool_manager),
      processor_(comparator), max_tuple_size_(max_tuple_size){}

bool HashTable::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  int key_v = get_hash_value(key);
  page_id_t page_id;
  HashPage * page = nullptr;
  if(!page_id_.count(key_v)) {
    page = reinterpret_cast<HashPage *>(buffer_pool_manager_->NewPage(page_id)->GetData());
    if(page == nullptr) {
      LOG(ERROR) << "out of memory";
      return false;
    }
    page_id_[key_v] = page_id;
    int max_size = (PAGE_SIZE - HASH_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(value));
    page->Init(page_id, INVALID_PAGE_ID, processor_.GetKeySize(), max_size);
  }
  else {
    page_id = page_id_[key_v];
    page = reinterpret_cast<HashPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  }
  if(page->Insert(key, value, buffer_pool_manager_)) {
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  }
  else {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
}

bool HashTable::Remove(const GenericKey *key, Transaction *transaction) {
  int key_v = get_hash_value(key);
  if(!page_id_.count(key_v)) {
    return false;
  }
  int page_id = page_id_[key_v];
  auto * page = reinterpret_cast<HashPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  if(page->RemoveAndDeleteRecord(key, processor_, buffer_pool_manager_)) {
    if(page->GetSize() == 0) {
      if(page->GetNextPageId() != INVALID_PAGE_ID) {
        page_id_[key_v] = page->GetNextPageId();
        buffer_pool_manager_->DeletePage(page->GetPageId());
      } else {
        page_id_.erase(key_v);
        buffer_pool_manager_->DeletePage(page->GetPageId());
      }
    }
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  }
  else {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
}

bool HashTable::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  int key_v = get_hash_value(key);
  if(!page_id_.count(key_v)) {
    return false;
  }
  int page_id = page_id_[key_v];
  auto * page = reinterpret_cast<HashPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  RowId res;
  bool Find = page->Lookup(key, res, processor_, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(page_id, false);
  if(Find) result.push_back(res);
  return Find;
}

HashIndexIterator HashTable::Begin() {
  if(page_id_.empty()) {
    return HashIndexIterator();
  }
  auto begin = page_id_.begin();
  return HashIndexIterator(&page_id_, begin->first, begin->second, buffer_pool_manager_, 0);
}

HashIndexIterator HashTable::End() {
  return HashIndexIterator();
}

void _Destroy(page_id_t page_id, BufferPoolManager * bpm) {
  auto * page = reinterpret_cast<HashPage *>
      (bpm->FetchPage(page_id)->GetData());
  if(page->GetNextPageId() != INVALID_PAGE_ID) {
    _Destroy(page->GetNextPageId(), bpm);
  } page->SetNextPageId(INVALID_PAGE_ID);
  bpm->DeletePage(page->GetPageId());
  bpm->UnpinPage(page->GetPageId(), false);
}

void HashTable::Destroy() {
  for(auto &[key, val] : page_id_) {
    _Destroy(val, buffer_pool_manager_);
  }
  page_id_.clear();
}

int HashTable::get_hash_value(const GenericKey *key) {
  static constexpr int Mod = 1000003, Base = 23;
  long long x = *(reinterpret_cast<const long long *> (key));
  int ret = 0;
  while(x) ret = ((1ll * ret * Base) % Mod + x % 10) % Mod, x /= 10;
  return ret;
}

bool HashTable::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}