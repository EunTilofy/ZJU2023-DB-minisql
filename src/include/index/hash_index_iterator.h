//
// Created by PaperCloud on 2023/6/7.
//

#ifndef MINISQL_HASH_INDEX_ITERATOR_H
#define MINISQL_HASH_INDEX_ITERATOR_H

#include "page/hash_page.h"
#include <map>

class HashIndexIterator {
 public:
  explicit HashIndexIterator();

  explicit HashIndexIterator(std::map<int, page_id_t> *mp, int key_v, page_id_t page_id, BufferPoolManager *bpm, int index = 0);

  ~HashIndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  std::pair<GenericKey *, RowId> operator*();

  /** Move to the next key/value pair.*/
  HashIndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const HashIndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const HashIndexIterator &itr) const;

 private:
  page_id_t current_page_id{INVALID_PAGE_ID};
  std::map<int, page_id_t> *mp_{nullptr};
  int current_key_v{-1};
  HashPage *page{nullptr};
  int item_index{0};
  BufferPoolManager *buffer_pool_manager{nullptr};
};


#endif  // MINISQL_HASH_INDEX_ITERATOR_H
