#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator() {

}

TableIterator::TableIterator(const TableIterator &other) {
  RowId rowid(-1,0);
  if (other.row_){
    row_ = new Row(*other.row_);
    rowid = row_->GetRowId();
  }
  else row_ = nullptr;
  this_heap_ = other.this_heap_;
  rid.Set(rowid.GetPageId(), rowid.GetSlotNum());
}

TableIterator::~TableIterator() {
  if(row_ != nullptr) delete row_;
}


const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // 首先，要获取当前元组所在的磁盘页
  //  ASSERT(false, "not implement yet");
  ASSERT(row_ != nullptr, "[ ERROR ] - cannot do ++ operation on a null iterator");
  page_id_t page_id = rid.GetPageId();
  ASSERT(page_id != INVALID_PAGE_ID, "[ ERROR ] - cannot do ++ operation on end iterator");
  TablePage *page = reinterpret_cast<TablePage *>(this_heap_->buffer_pool_manager_->FetchPage(page_id));
  ASSERT(page_id == page->GetPageId(), "[ ERROR ] - page_id == page->GetPageId() should be true");
  RowId nextid;
  // 搜索下一个可用的页面
  if (page->GetNextTupleRid(rid, &nextid)) {
    row_->GetFields().clear();
    rid.Set(nextid.GetPageId(), nextid.GetSlotNum());
    row_->SetRowId(rid);
    this_heap_->GetTuple(row_, nullptr);
    row_->SetRowId(rid);
    this_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
    return *this;
  }
  page_id_t next_page_id = INVALID_PAGE_ID;
  while ((next_page_id = page->GetNextPageId()) != INVALID_PAGE_ID) {
    TablePage *next_page = reinterpret_cast<TablePage *>(this_heap_->buffer_pool_manager_->FetchPage(next_page_id));
    this_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = next_page;
    if (page->GetFirstTupleRid(&nextid)) {
      row_->GetFields().clear();
      rid = nextid;
      row_->SetRowId(nextid);
      this_heap_->GetTuple(row_, nullptr);
      row_->SetRowId(rid);
      this_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return *this;
    }
  }
  // 到这里，说明咩有元组了
  rid.Set(INVALID_PAGE_ID, 0);
  //  row_ = nullptr;
  this_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  Row* row_next = new Row(*(this->row_));
  TableHeap* this_heap_next = this->this_heap_;
  RowId rid_next = this->rid;
  ++(*this);
  return TableIterator(row_next, this_heap_next, rid_next);
}
