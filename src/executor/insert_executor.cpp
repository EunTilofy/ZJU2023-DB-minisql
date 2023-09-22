//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  TableInfo* table;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table);
  vector<IndexInfo*> indexes;
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(), indexes);
  if (child_executor_->Next(row, rid)) {
    for (auto index: indexes) {
      if (!index->GetIndexName().compare("PRIMARY_KEY_") && index->GetIndexKeySchema()->GetColumnCount() == 1) {
        uint32_t col_idx = index->GetIndexKeySchema()->GetColumn(0)->GetTableInd();
        Field *keyVal = row->GetField(col_idx);
        std::vector<Field> keyData {Field(*keyVal)};
        Row key = Row(keyData);
        vector <RowId> res;
        index->GetIndex()->ScanKey(key, res, nullptr, "=");
        if (!res.empty()) {
          throw string("PK corruption!");
        }
      }
      if (index->GetIndexName().find("__Unique") == 0 && index->GetIndexKeySchema()->GetColumnCount() == 1) {
        uint32_t col_idx = index->GetIndexKeySchema()->GetColumn(0)->GetTableInd();
        Field* fieldVal = row->GetField(col_idx);
        std::vector<Field> uniqueData {Field(*fieldVal)};
        Row unique = Row(uniqueData);
        vector <RowId> res;
        index->GetIndex()->ScanKey(unique, res, nullptr, "=");
        if (!res.empty()) {
          throw string("Uniqueness constraint corruption!");
        }
      }
    }
    table->GetTableHeap()->InsertTuple(*row, nullptr);
    for (auto index: indexes) {
      uint32_t col_idx = index->GetIndexKeySchema()->GetColumn(0)->GetTableInd();
      Field *keyVal = row->GetField(col_idx);
      std::vector<Field> keyData {Field(*keyVal)};
      Row key = Row(keyData);
      index->GetIndex()->InsertEntry(key, row->GetRowId(), nullptr);
    }
    *rid = row->GetRowId();
    return true;
  }
  return false;
}