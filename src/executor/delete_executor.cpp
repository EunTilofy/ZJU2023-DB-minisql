//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  string table_name = plan_->GetTableName();
  TableInfo* table;
  exec_ctx_->GetCatalog()->GetTable(table_name, table);
  tableHeap_ = table->GetTableHeap();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if (child_executor_->Next(row, rid)) {
    tableHeap_->MarkDelete(*rid, nullptr);
    vector<IndexInfo *> indexes;
    exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(), indexes);
    for (auto index : indexes) {
      auto key_schema = index->GetIndexKeySchema();
      vector<size_t> key_map;
      for (auto i : key_schema->GetColumns()) {
        key_map.emplace_back(i->GetTableInd());
      }
      vector<Field> key_fields;
      for (auto i : key_map) {
        key_fields.emplace_back(Field(*row->GetField(i)));
      }
      Row key(key_fields);
      index->GetIndex()->RemoveEntry(key_fields, *rid, nullptr);
    }
    return true;
  }
  else return false;
}