#include "executor/executors/index_scan_executor.h"
#include <algorithm>

// Student added function
pair<string, Field> FindIndexVal(AbstractExpression* node, uint32_t col_idx) {
  ComparisonExpression* comparisonNode = nullptr;
  string comparisonType;
  ColumnValueExpression* col = nullptr;
  ConstantValueExpression* cons = nullptr;
  switch(node->GetType()) {
    case ExpressionType::LogicExpression:
      for (const auto& child : node->GetChildren()) {
        pair<string, Field> field = FindIndexVal(&*(child), col_idx);
        if (!field.second.IsNull()) {
          return field;
        }
      }
      break;
    case ExpressionType::ComparisonExpression:
      comparisonNode = dynamic_cast<ComparisonExpression*>(&*node);
      comparisonType = comparisonNode->GetComparisonType();
      if (node->GetChildAt(0)->GetType() == ExpressionType::ColumnExpression && node->GetChildAt(0)) {
        col = dynamic_cast<ColumnValueExpression*>(&*node->GetChildAt(0));
        if (col->GetColIdx() == col_idx) {
          cons = dynamic_cast<ConstantValueExpression*>(&*node->GetChildAt(1));
          return pair<string, Field>(comparisonType, Field(cons->val_));
        }
        else {
          return pair<string, Field>("", Field(TypeId::kTypeInvalid));
        }
      }
      break;
    default:
      return pair<string, Field>("", Field(TypeId::kTypeInvalid));
  }
}

bool RowIdComp(RowId a, RowId b) {
  return a.GetPageId() > b.GetPageId() || (a.GetPageId() == b.GetPageId() && a.GetSlotNum() > b.GetSlotNum());
}

/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  TableInfo* targetTable;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), targetTable);
  original_schema_ = targetTable->GetSchema();
  pair<string, Field>* res;
  unordered_map<IndexInfo*, pair<string, Field*>> index_val;
  for (auto index: plan_->indexes_) {
    auto col_idx = index->GetIndexKeySchema()->GetColumn(0)->GetTableInd();
    AbstractExpression* node = &*(plan_->filter_predicate_);
    res = new pair<string, Field>(FindIndexVal(node, col_idx));
    index_val[index].first = res->first;
    index_val[index].second = &res->second;
  }

  for (auto index: plan_->indexes_) {
    vector<Field> row;
    vector<RowId> results, prevRes;
    index_results_.swap(prevRes);
    row.emplace_back(*index_val[index].second);
    index->GetIndex()->ScanKey(Row(row), results, nullptr, index_val[index].first);
    sort(results.begin(), results.end(), RowIdComp);
    if (*plan_->indexes_.begin() != index){
      std::set_intersection(results.begin(), results.end(), prevRes.begin(), prevRes.end(),
                            std::back_inserter(index_results_), RowIdComp);
    }
    else {
      index_results_ = results;
    }
  }
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_);
  it_ = index_results_.begin();
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  while(it_ != index_results_.end()) {
    Row* tuple = new Row(*it_);
    table_->GetTableHeap()->GetTuple(tuple, nullptr);
    if (plan_->GetPredicate()->Evaluate(tuple).CompareEquals(Field(kTypeInt, 1))) {
      vector <Field> fields;
      for (auto column: original_schema_->GetColumns()) {
        for (auto target: plan_->OutputSchema()->GetColumns()) {
          if (!target->GetName().compare(column->GetName())) {
            fields.push_back(*tuple->GetField(column->GetTableInd()));
          }
        }
      }
      *row = Row(fields);
      row->SetRowId(*it_);
      delete tuple;
      *rid = *it_;
      ++it_;
      return true;
    }
    delete tuple;
    it_++;
  }
  return false;
}
