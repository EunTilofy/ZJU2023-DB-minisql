//
// Created by PaperCloud on 2023/6/7.
//

#ifndef MINISQL_HASH_PAGE_H
#define MINISQL_HASH_PAGE_H

#include "page/page.h"
#include <utility>
#include <vector>

#include "index/generic_key.h"
#include "page/b_plus_tree_page.h"

#define HASH_PAGE_HEADER_SIZE 24

class HashPage{
 public:
  void Init(page_id_t page_id, page_id_t pre_id = INVALID_PAGE_ID, int key_size = UNDEFINED_SIZE,
            int max_size = UNDEFINED_SIZE);

  // helper methods
  [[nodiscard]] int GetSize() const;

  [[nodiscard]] int GetKeySize() const;

  [[nodiscard]] int GetMaxSize() const;

  void SetSize(int size);

  int GetPageId() const;

  void SetPageId(page_id_t page_id);

  [[nodiscard]] page_id_t GetPrePageId() const;

  void SetPrePageId(page_id_t pre_page_id);

  [[nodiscard]] page_id_t GetNextPageId() const;

  void SetNextPageId(page_id_t next_page_id);

  GenericKey *KeyAt(int index);

  void SetKeyAt(int index, GenericKey *key);

  [[nodiscard]] RowId ValueAt(int index) const;

  void SetValueAt(int index, RowId value);

  int KeyIndex(const GenericKey *key, const KeyManager &comparator);

  void *PairPtrAt(int index);

  void PairCopy(void *dest, void *src, int pair_num);

  std::pair<GenericKey *, RowId> GetItem(int index);

  // insert and delete methods
  bool Insert(GenericKey *key, const RowId &value, BufferPoolManager *buffer_pool_manager);

  bool Lookup(const GenericKey *key, RowId &value, const KeyManager &comparator, BufferPoolManager *buffer_pool_manager);

  bool RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &comparator, BufferPoolManager *buffer_pool_manager);

 private:
  [[maybe_unused]] int key_size_; // 4
  [[maybe_unused]] int size_; // 4
  [[maybe_unused]] int max_size_; // 4
  [[maybe_unused]] page_id_t page_id_; // 4
  [[maybe_unused]] page_id_t pre_page_id_{INVALID_PAGE_ID}; // 4
  [[maybe_unused]] page_id_t next_page_id_{INVALID_PAGE_ID}; // 4
  char data_[PAGE_SIZE - HASH_PAGE_HEADER_SIZE];
};

#endif  // MINISQL_HASH_PAGE_H
