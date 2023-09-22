#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn){
  if(total_page > 5)
  {
    auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id));
    if(last_page == nullptr)
    {
      buffer_pool_manager_->UnpinPage(last_page_id, false);
      return false;
    }
    if(last_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)){
      buffer_pool_manager_->UnpinPage(last_page_id, true);
    }else{
      page_id_t next_page_id;
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if(new_page != nullptr && next_page_id != INVALID_PAGE_ID)
      {
        new_page->Init(next_page_id, last_page_id, log_manager_, txn);
        last_page->SetNextPageId(next_page_id);
        new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        buffer_pool_manager_->UnpinPage(last_page_id, false);
        buffer_pool_manager_->UnpinPage(next_page_id, true);
        last_page_id = next_page_id;
        total_page++;
      }
      else
      {
        buffer_pool_manager_->UnpinPage(next_page_id, true);
        buffer_pool_manager_->UnpinPage(last_page_id, false);
        return false;
      }
    }
    return true;
  }
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));
  bool is_not_valid = buffer_pool_manager_->IsPageFree(GetFirstPageId());
  if(is_not_valid) {
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id_));
  }
  if(page == nullptr)
  {
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return false;
  }
  page_id_t page_id = GetFirstPageId();
  while(1)
  {
    if(page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))
    {
      return buffer_pool_manager_->UnpinPage(page_id, true);
    }
    auto next_page_id = page->GetNextPageId();
    if(next_page_id == INVALID_PAGE_ID)
    {
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if(new_page == nullptr || next_page_id == INVALID_PAGE_ID)
      {
        buffer_pool_manager_->UnpinPage(page_id, false);
        return false;
      }
      else{
        new_page->Init(next_page_id, page_id, log_manager_, txn);
        page->SetNextPageId(next_page_id);
        buffer_pool_manager_->UnpinPage(page_id, false);
        page = new_page;
        page_id = next_page_id;
      }
    }
    else{
      buffer_pool_manager_->UnpinPage(page_id, false);
      page_id = next_page_id;
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    }
  }
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  auto page_id = rid.GetPageId();
  if(page == nullptr)
  {
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return false;
  }
  Row old_row;
  old_row.SetRowId(rid);
  int msg = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if(msg == 1)
  {
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  }
  else if(msg == -3)
  {
    ApplyDelete(rid, txn);
    InsertTuple(row, txn);
    buffer_pool_manager_->UnpinPage(page_id, true);
    //Log(INFO) << "Table_Heap::UpdateTuple() succeed: " << "page_id: " << page_id;
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  auto page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if(page == nullptr)
  {
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return;
  }
  else {
    page->ApplyDelete(rid, txn, log_manager_);
    buffer_pool_manager_->UnpinPage(page_id, true);
  }
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  RowId rowid = row->GetRowId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rowid.GetPageId()));
  auto page_id = rowid.GetPageId();
  if(page == nullptr){
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
  if(page->GetTuple(row, schema_, txn, lock_manager_))
  {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page_id, false);
  return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  page_id_t page_id = first_page_id_;
  RowId result_rid;
  while(1)
  {
    if(page_id == INVALID_PAGE_ID)
    {
      return End();
    }
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if(page->GetFirstTupleRid(&result_rid))
    {
      buffer_pool_manager_->UnpinPage(page_id, false);
      break;
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = page->GetNextPageId();
  }
  if(page_id != INVALID_PAGE_ID)
  {
    Row* result_row = new Row(result_rid);
    GetTuple(result_row, txn);
    return TableIterator(result_row, this, result_rid);
  }
  return End();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  RowId null;
  return TableIterator(nullptr, this, null);
}
