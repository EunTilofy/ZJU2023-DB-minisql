#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();

  explicit TableIterator(Row *row, TableHeap *this_heap, RowId &rid) : row_(row), this_heap_(this_heap), rid(rid){}

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const {return rid == itr.rid;}

  inline bool operator!=(const TableIterator &itr) const {return !(rid == itr.rid);}

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept{
    if (itr.row_) {
      row_ = new Row(*itr.row_);
    }
    else row_ = nullptr;
    this_heap_ = itr.this_heap_;
    rid = itr.rid;
    return *this;
  }

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  Row *row_{nullptr};
  TableHeap *this_heap_{nullptr};
  RowId rid{INVALID_PAGE_ID, 0};
  // add your own private member variables here
};

#endif  // MINISQL_TABLE_ITERATOR_H
