//
// Created by PaperCloud on 2023/6/7.
//

#ifndef MINISQL_HASH_INDEX_H
#define MINISQL_HASH_INDEX_H

#include "index/generic_key.h"
#include "index/index.h"
#include "index/hash_table.h"

class HashIndex : public Index {
 public:
  HashIndex(index_id_t index_id, IndexSchema *key_schema, size_t key_size, BufferPoolManager *bufferPoolManager);

  dberr_t InsertEntry(const Row &key, RowId row_id, Transaction *txn) override;

  dberr_t RemoveEntry(const Row &key, RowId row_id, Transaction *txn) override;

  dberr_t ScanKey(const Row &key, std::vector<RowId> &result, Transaction *txn, string compare_operator = "=") override;

  dberr_t Destroy() override;

  HashIndexIterator GetBeginIterator();

  HashIndexIterator GetEndIterator();

 protected:
  // comparator for key
  KeyManager processor_;
  // container
  HashTable container_;
};
#endif  // MINISQL_HASH_INDEX_H
