//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
  child_executor_->Init();
  string table_name = plan_->GetTableName();
  TableInfo* table;
  exec_ctx_->GetCatalog()->GetTable(table_name, table);
  tableHeap_ = table->GetTableHeap();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if (child_executor_->Next(row, rid)) {
    auto upd_attr = plan_->GetUpdateAttr();
    vector<Field> fields;
    for (size_t i = 0; i < row->GetFieldCount(); ++i) {
      if (upd_attr.count(i)) {
        fields.emplace_back(Field(upd_attr[i]->Evaluate(row)));
      }
      else {
        fields.emplace_back(Field(*row->GetField(i)));
      }
    }
    Row newRow(fields);
    if (tableHeap_->UpdateTuple(newRow, *rid, nullptr)) {
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
        vector<Field> new_key_fields;
        for (auto i: key_map) {
          new_key_fields.emplace_back(Field(*newRow.GetField(i)));
        }
        Row key(key_fields);
        index->GetIndex()->RemoveEntry(key_fields, *rid, nullptr);
        Row newKey(new_key_fields);
        index->GetIndex()->InsertEntry(new_key_fields, *rid, nullptr);
      }
      *row = newRow;
      return true;
    }
    return false;
  }
  else return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  return Row();
}