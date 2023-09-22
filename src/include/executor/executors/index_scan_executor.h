#pragma once

#include <vector>

#include "executor/execute_context.h"
#include "executor/executors/abstract_executor.h"
#include "executor/plans/index_scan_plan.h"
#include "planner/expressions/column_value_expression.h"
#include "planner/expressions/constant_value_expression.h"
#include "planner/expressions/comparison_expression.h"

/**
 * The IndexScanExecutor executor can over a table.
 */
class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next row from the sequential scan.
   * @param[out] row The next row produced by the scan
   * @param[out] rid The next row RID produced by the scan
   * @return `true` if a row was produced, `false` if there are no more rows
   */
  bool Next(Row *row, RowId *rid) override;

  /** @return The output schema for the sequential scan */
  const Schema *GetOutputSchema() const override { return plan_->OutputSchema(); }

  // Student added member function
  const IndexScanPlanNode* GetPlan() const { return plan_; }

 private:

  /** The sequential scan plan node to be executed */
  const IndexScanPlanNode *plan_;

  //Student added members
  vector<RowId> index_results_;
  vector<RowId>::iterator it_;
  Schema* original_schema_;
  TableInfo* table_;
};