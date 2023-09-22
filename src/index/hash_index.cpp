//
// Created by PaperCloud on 2023/6/7.
//
#include <algorithm>
#include "index/hash_index.h"

#include "index/generic_key.h"
#include "utils/tree_file_mgr.h"

HashIndex::HashIndex(index_id_t index_id, IndexSchema *key_schema, size_t key_size, BufferPoolManager *buffer_pool_manager_)
  :   Index(index_id, key_schema),
      processor_(key_schema_, key_size),
      container_(index_id, buffer_pool_manager_, processor_){}

dberr_t HashIndex::InsertEntry(const Row &key, RowId row_id, Transaction *txn) {
  GenericKey *index_key = processor_.InitKey();
  processor_.SerializeFromKey(index_key, key, key_schema_);

  bool status = container_.Insert(index_key, row_id, txn);
  delete index_key;

  if(!status) {
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t HashIndex::RemoveEntry(const Row &key, RowId row_id, Transaction *txn) {
  GenericKey *index_key = processor_.InitKey();
  processor_.SerializeFromKey(index_key, key, key_schema_);

  container_.Remove(index_key, txn);
  delete index_key;
  return DB_SUCCESS;
}

dberr_t HashIndex::ScanKey(const Row &key, vector<RowId> &result, Transaction *txn, string compare_operator) {
  GenericKey *index_key = processor_.InitKey();
  processor_.SerializeFromKey(index_key, key, key_schema_);
  if(compare_operator == "=") {
    container_.GetValue(index_key, result, txn);
    if(result.empty()) return DB_KEY_NOT_FOUND;
    else return DB_SUCCESS;
  }
  else return DB_SUCCESS;
}

dberr_t HashIndex::Destroy() {
  container_.Destroy();
  return DB_SUCCESS;
}

HashIndexIterator HashIndex::GetBeginIterator() {
  return container_.Begin();
}

HashIndexIterator HashIndex::GetEndIterator() {
  return container_.End();
}