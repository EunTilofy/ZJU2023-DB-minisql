//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
  TableInfo* targetTable;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), targetTable);
  tableHeap_ = targetTable->GetTableHeap();
  original_schema_ = targetTable->GetSchema();
  it_ = tableHeap_->Begin(nullptr);
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  while (it_ != tableHeap_->End()) {
    if (plan_->GetPredicate() == nullptr || Field(kTypeInt, 1).CompareEquals(plan_->GetPredicate()->Evaluate(&*it_))) {
      vector<Field> fields;
      for (auto column : original_schema_->GetColumns()) {
        for (auto target : plan_->OutputSchema()->GetColumns()) {
          if (!target->GetName().compare(column->GetName())) {
            fields.push_back(*it_->GetField(column->GetTableInd()));
          }
        }
      }
      *rid = RowId(it_->GetRowId());
      *row = Row(fields);
      row->SetRowId(*rid);
      ++it_;
      return true;
    }
    else ++it_;
  }
  return false;
}
