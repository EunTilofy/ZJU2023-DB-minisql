//
// Created by PaperCloud on 2023/6/7.
//
#include "page/hash_page.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()

void HashPage::Init(page_id_t page_id, page_id_t pre_id, int key_size,
          int max_size) {
  page_id_ = page_id;
  pre_page_id_ = pre_id;
  key_size_ = key_size;
  max_size_ = max_size;
  size_ = 0;
  next_page_id_ = INVALID_PAGE_ID;
}

// helper methods
int HashPage::GetSize() const{
  return size_;
}

void HashPage::SetSize(int size) {
  size_ = size;
}

int HashPage::GetPageId() const {
  return page_id_;
}

void HashPage::SetPageId(page_id_t page_id) {
  page_id_ = page_id;
}

page_id_t HashPage::GetPrePageId() const {
    return pre_page_id_;
}

void HashPage::SetPrePageId(page_id_t pre_page_id) {
    pre_page_id_ = pre_page_id;
}

page_id_t HashPage::GetNextPageId() const{
  return next_page_id_;
}

int HashPage::GetMaxSize() const {
  return max_size_;
}

void HashPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

int HashPage::GetKeySize() const {
  return key_size_;
}

GenericKey *HashPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void HashPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId HashPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void HashPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

int HashPage::KeyIndex(const GenericKey *key, const KeyManager &comparator) {
  for(int i = 0; i < GetSize(); ++i) {
    if(comparator.CompareKeys(key, KeyAt(i)) == 0) return i;
  }
  return -1;
}

void *HashPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void HashPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

std::pair<GenericKey *, RowId> HashPage::GetItem(int index) {
  return make_pair(KeyAt(index), ValueAt(index));
}

bool HashPage::Insert(GenericKey *key, const RowId &value, BufferPoolManager *buffer_pool_manager) {
  if(GetSize() < GetMaxSize()) {
    SetKeyAt(GetSize(), key);
    SetValueAt(GetSize(), value);
    SetSize(GetSize() + 1);
  } else {
    HashPage * page= this;
    bool current_page = true;
    while(page->GetSize() == page->GetMaxSize() && page->GetNextPageId() != INVALID_PAGE_ID) {
      if(!current_page) {
        buffer_pool_manager->UnpinPage(page->GetPageId(), false);
      } else current_page = false;
//      int x = page->GetNextPageId();
      page = reinterpret_cast<HashPage *>
          (buffer_pool_manager->FetchPage(page->GetNextPageId())->GetData());
    }
    if(page->GetSize() == page->GetMaxSize()) {
      page_id_t new_page_id;
      auto * new_page = reinterpret_cast<HashPage *>
          (buffer_pool_manager->NewPage(new_page_id)->GetData());
      new_page->Init(new_page_id, page->GetPageId(), page->GetKeySize(), page->GetMaxSize());
      new_page->SetKeyAt(0, key);
      new_page->SetValueAt(0, value);
      new_page->SetSize(1);
      page->SetNextPageId(new_page_id);
      buffer_pool_manager->UnpinPage(new_page_id, true);
      if(!current_page) buffer_pool_manager->UnpinPage(page->GetPageId(), true);
    } else {
      page->SetKeyAt(page->GetSize(), key);
      page->SetValueAt(page->GetSize(), value);
      page->SetSize(page->GetSize() + 1);
      if(!current_page) buffer_pool_manager->UnpinPage(page->GetPageId(), true);
    }
  }
  return true;
}

bool HashPage::Lookup(const GenericKey *key, RowId &value,
                      const KeyManager &comparator, BufferPoolManager *buffer_pool_manager){
  for(int i = 0; i < GetSize(); ++i) {
    if(comparator.CompareKeys(key, KeyAt(i)) == 0) {
      value = ValueAt(i);
      return true;
    }
  }
  if(GetNextPageId() != INVALID_PAGE_ID) {
    auto * next_page = reinterpret_cast<HashPage *>
          (buffer_pool_manager->FetchPage(GetNextPageId())->GetData());
    bool Find = next_page->Lookup(key, value, comparator, buffer_pool_manager);
    buffer_pool_manager->UnpinPage(next_page->GetPageId(), false);
    return Find;
  } else return false;
}

bool HashPage::RemoveAndDeleteRecord(const GenericKey *key,
                                     const KeyManager &comparator, BufferPoolManager *buffer_pool_manager){
  bool _delete = false;
  for(int i = 0; i < GetSize(); ++i) {
    if(comparator.CompareKeys(key, KeyAt(i)) == 0) {
      PairCopy(PairPtrAt(i), PairPtrAt(i + 1), GetSize() - i - 1);
      SetSize(GetSize() - 1);
      _delete = true;
      break;
    }
  }
  if(GetNextPageId() != INVALID_PAGE_ID) {
    auto * next_page = reinterpret_cast<HashPage *>
        (buffer_pool_manager->FetchPage(GetNextPageId())->GetData());
    _delete = next_page->RemoveAndDeleteRecord(key, comparator, buffer_pool_manager);
    if(next_page->GetSize() == 0) {
      SetNextPageId(next_page->GetNextPageId());
      buffer_pool_manager->DeletePage(next_page->GetPageId());
      buffer_pool_manager->UnpinPage(next_page->GetPageId(), true);
    }
    buffer_pool_manager->UnpinPage(next_page->GetPageId(), false);
  }
  return _delete;
}