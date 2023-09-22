//
// Created by PaperCloud on 2023/6/7.
//

#ifndef MINISQL_HASH_TABLE_H
#define MINISQL_HASH_TABLE_H

#include <queue>
#include <string>
#include <vector>
#include <map>

#include "index/hash_index_iterator.h"
#include "transaction/transaction.h"

class HashTable {
 public:
  HashTable(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &comparator,
            int max_tuple_size = UNDEFINED_SIZE);

  bool Insert(GenericKey *key, const RowId &value, Transaction *transaction = nullptr);

  bool Remove(const GenericKey *key, Transaction *transaction = nullptr);

  bool GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction = nullptr);

  HashIndexIterator Begin();

  HashIndexIterator End();

  void Destroy();

  static int get_hash_value(const GenericKey* key);

  bool Check();

 private:
  map<int, page_id_t> page_id_;
  index_id_t index_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyManager processor_;
  int max_tuple_size_;
};

#endif  // MINISQL_HASH_TABLE_H
