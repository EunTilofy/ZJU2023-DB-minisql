#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** After you finish the code for the CatalogManager section,
   *  you can uncomment the commented code.   **/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }

  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const string &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  string db_file_name = "databases/" + db_name + ".db";
  std::ifstream check_db_file(db_file_name, std::ios::in);
  if (check_db_file.is_open()) {
    std::cout << "Database " << db_name << " is already created." << std::endl;
    return DB_ALREADY_EXIST;
  }
  std::ofstream db_file(db_file_name, std::ios::out);
  if (!db_file.is_open()) {
    std::cout << "Failed to create database " << db_name << "." << std::endl;
    return DB_FAILED;
  }
  dbs_[db_name + ".db"] = new DBStorageEngine(db_name+".db");
  std::cout << "Database " << db_name << " is created." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  string db_file_name = "databases/" + db_name + ".db";
  std::ifstream db_file(db_file_name, std::ios::in);
  if (!db_file.is_open()) {
    std::cout << "Database " << db_name << " does not exist." << std::endl;
    return DB_NOT_EXIST;
  }
  db_file.close();
  remove(db_file_name.c_str());
  std::ifstream check_db_file(db_file_name, std::ios::in);
  if (db_file.is_open()) {
    std::cout << "Failed to delete database " << db_name << "." << std::endl;
    return DB_FAILED;
  }
  dbs_.erase(db_name + ".db");
  std::cout << "Database " << db_name << " is deleted." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  cout << endl;
  int count = 0;
  for (auto i: dbs_) {
    cout << i.first.substr(0, i.first.size()-3) << std::endl;
    count ++;
  }
  cout << count << " databases listed." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  string db_file_name = db_name + ".db";
  if (dbs_.find(db_file_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  current_db_ = db_file_name;
  std::cout << "Database " << db_name << " is used." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  cout << endl;
  string db_name = current_db_;
  vector <TableInfo*> tables;
  dbs_[db_name]->catalog_mgr_->GetTables(tables);
  for (auto i: tables) {
    cout << i->GetTableName() << endl;
  }
  cout << tables.size() << " tables listed." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) {
    std::cout << "Please use a database." << std::endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  pSyntaxNode columnDefinitions = ast->child_->next_;
  vector <Column*> columns;
  int index = 0;
  vector<string> pk_col_names;
  vector<string> unique_col_names;
  // the following loop is for parsing the syntax tree, and it is too long to display.
  for (pSyntaxNode columnDef = columnDefinitions->child_; columnDef; columnDef = columnDef->next_, ++index) {
    Column *new_col = nullptr;
    string col_name;
    TypeId col_type;
    int col_len;
    if (!columnDef->val_) {
      col_name = columnDef->child_->val_;
      if (!strcmp(columnDef->child_->next_->val_, "int")) {
        col_type = kTypeInt;
      }
      else if (!strcmp(columnDef->child_->next_->val_, "float")) {
        col_type = kTypeFloat;
      }
      else if (!strcmp(columnDef->child_->next_->val_, "char")) {
        col_type = kTypeChar;
        col_len = stoi(columnDef->child_->next_->child_->val_);
      }
      else col_type = kTypeInvalid;
      if (col_type == kTypeChar) {
        new_col = new Column(col_name, col_type, col_len, index, true,false);
      }
      else new_col = new Column(col_name, col_type, index, true, false);
      columns.emplace_back(new_col);
    }
    else if (!strcmp(columnDef->val_, "unique")) {
      col_name = columnDef->child_->val_;
      if (!strcmp(columnDef->child_->next_->val_, "int")) {
        col_type = kTypeInt;
      }
      else if (!strcmp(columnDef->child_->next_->val_, "float")) {
        col_type = kTypeFloat;
      }
      else if (!strcmp(columnDef->child_->next_->val_, "char")) {
        col_type = kTypeChar;
        col_len = stoi(columnDef->child_->next_->child_->val_);
      }
      else col_type = kTypeInvalid;
      if (col_type == kTypeChar) {
        new_col = new Column(col_name, col_type, col_len, index, false, true);
      }
      else new_col = new Column(col_name, col_type, index, false, true);
      columns.emplace_back(new_col);
      unique_col_names.emplace_back(col_name);
    }
    else if (!strcmp(columnDef->val_, "primary keys")) {
      for (pSyntaxNode pk_col_name = columnDef->child_; pk_col_name; pk_col_name = pk_col_name->next_) {
        pk_col_names.emplace_back(pk_col_name->val_);
      }
      for (auto pk_col_name: pk_col_names) {
        for (auto column: columns) {
          if (!column->GetName().compare(pk_col_name)) {
            if (pk_col_names.size() == 1){
              column->SetUnique(true);
            }
            column->SetNullable(false);
            break;
          }
        }
      }
    }
  }
  auto *schema = new Schema(columns);
  auto *table = TableInfo::Create();
  auto err = context->GetCatalog()->CreateTable(table_name, schema, nullptr, table);
  if (err != DB_SUCCESS) {
    return err;
  }
  int unique_index = 0;
  for (auto i : unique_col_names) {
    auto *index = IndexInfo::Create();
    dberr_t res = context->GetCatalog()->CreateIndex(table_name, "__Unique" + to_string(unique_index), {i}, nullptr, index, "bptree");
    unique_index ++;
    if (res != DB_SUCCESS) {
      return res;
    }
  }
  auto *indexInfo = IndexInfo::Create();
  if (pk_col_names.size() == 1) {
    cout << "Table " << table_name << " created successfully." << endl;
    return context->GetCatalog()->CreateIndex(table_name, "PRIMARY_KEY_", pk_col_names, nullptr, indexInfo, "bptree");
  }
  cout << "Table " << table_name << " created successfully." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  dberr_t res = context->GetCatalog()->DropTable(ast->child_->val_);
  if (res == DB_SUCCESS) {
    cout << "Table " << ast->child_->val_ << " deleted successfully." << endl;
  }
  return res;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  cout << endl;
  vector<TableInfo*> tables;
  int count = 0;
  context->GetCatalog()->GetTables(tables);
  for (auto table: tables) {
    string table_name = table->GetTableName();
    vector<IndexInfo*> indexes;
    context->GetCatalog()->GetTableIndexes(table_name, indexes);
    for (auto index: indexes) {
      string index_name = index->GetIndexName();
      cout << "table: " << table_name << " \t\tindex: " << index_name << std::endl;
      count++;
    }
  }
  cout << count << " index(es) listed." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;
  pSyntaxNode columnList = ast->child_->next_->next_;
  vector <string> columns;
  for (pSyntaxNode column = columnList->child_; column; column = column->next_) {
    columns.emplace_back(column->val_);
  }
  string index_type = columnList->next_ ? (strcmp(columnList->next_->child_->val_, "hash") ? "bptree" : "hash") : "bptree";
  auto *indexInfo = IndexInfo::Create();
  dberr_t res = context->GetCatalog()->CreateIndex(table_name, index_name, columns, nullptr, indexInfo, index_type);
  if (res == DB_SUCCESS) {
    cout << "Index " << index_name << " created successfully." << endl;
  }
  return res;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  vector<TableInfo*> tables;
  int count = 0;
  context->GetCatalog()->GetTables(tables);
  for (auto table: tables) {
    string table_name = table->GetTableName();
    vector<IndexInfo*> indexes;
    context->GetCatalog()->GetTableIndexes(table_name, indexes);
    for (auto index: indexes) {
      string index_name = index->GetIndexName();
      if (!index_name.compare(ast->child_->val_)) {
        dberr_t res = context->GetCatalog()->DropIndex(table_name, ast->child_->val_);
        if (res == DB_SUCCESS) {
          cout << "Index " << ast->child_->val_ << " deleted successfully." << endl;
        }
        return res;
      }
    }
  }
  return DB_INDEX_NOT_FOUND;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */

extern "C" {
int yyparse(void);
#include <parser/minisql_lex.h>
#include <parser/parser.h>
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  char* file_name = ast->child_->val_;
  FILE* file = fopen(file_name, "r");
  if (file == nullptr) {
    cout << "File " << file_name << " not found!" << endl;
    return DB_FAILED;
  }
  char input[1024];
  while(!feof(file)) {
    memset(input, 0, 1024);
    int i = 0;
    char ch;
    while (!feof(file) && (ch = getc(file)) != ';') {
      input[i++] = ch;
    }
    if (feof(file)) continue;
    input[i] = ch;
    YY_BUFFER_STATE bp = yy_scan_string(input);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    }

    auto result = this->Execute(MinisqlGetParserRootNode());

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    ExecuteInformation(result);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_QUIT;
}
