#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(CATALOG_METADATA_MAGIC_NUM) + sizeof(decltype(index_meta_pages_.size())) + sizeof(decltype(table_meta_pages_.size())) + index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t))
         + table_meta_pages_.size() * (sizeof (index_id_t) + sizeof (page_id_t));
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if(!init)
  {
    auto catalog_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(catalog_page->GetData()));
    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();
    for(auto it : catalog_meta_->table_meta_pages_){
      auto table_meta_page = buffer_pool_manager_->FetchPage(it.second);
      TableMetadata *table_meta;
      TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);
      table_names_[table_meta->GetTableName()] = table_meta->GetTableId();
      auto table_heap = TableHeap::Create(buffer_pool_manager, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager_, lock_manager_);
      TableInfo *table_info = TableInfo::Create();
      table_info->Init(table_meta, table_heap);
      tables_[table_meta->GetTableId()] = table_info;
      if (table_meta->GetTableId() >= next_table_id_) {
        next_table_id_ = table_meta->GetTableId() + 1;
      }
    }
    for(auto it : catalog_meta_->index_meta_pages_){
      auto index_meta_page = buffer_pool_manager_->FetchPage(it.second);
      IndexMetadata *index_meta;
      IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);
      index_names_[tables_[index_meta->GetTableId()]->GetTableName()][index_meta->GetIndexName()] = index_meta->GetIndexId();
      IndexInfo *index_info = IndexInfo::Create();
      index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
      indexes_[index_meta->GetIndexId()] = index_info;
      if (index_meta->GetIndexId() >= next_index_id_) {
        next_index_id_ = index_meta->GetIndexId() + 1;
      }
    }
  }
  else{
    catalog_meta_ = CatalogMeta::NewInstance();
  }

}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if(table_names_.count(table_name) != 0)
  {
    return DB_TABLE_ALREADY_EXIST;
  }
  table_id_t table_id = next_table_id_;
  table_names_.emplace(table_name, table_id);

  page_id_t page_id;
  auto table_meta_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->table_meta_pages_.emplace(table_id, page_id);

  auto table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  auto table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema);
  table_meta->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);
  next_table_id_++;
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  tables_[table_id] = table_info;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto temp = table_names_.find(table_name);
  if(temp == table_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_.find(temp->second)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(auto &it : tables_)
  {
    tables.push_back(it.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  auto temp = table_names_.find(table_name);
  if(temp == table_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  auto temp_ = index_names_.find(table_name);
  if(temp_ != index_names_.end() && temp_->second.find(index_name) != temp_->second.end())
  {
    return DB_INDEX_ALREADY_EXIST;
  }
  auto table_info = tables_.find(temp->second);
  auto schema = table_info->second->GetSchema();
  std::vector<uint32_t> key_map;
  for(auto &it : index_keys){
    uint32_t column_index;
    if(schema->GetColumnIndex(it, column_index) == DB_COLUMN_NAME_NOT_EXIST)
    {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(column_index);
  }
  index_id_t index_id = next_index_id_;
  index_names_[table_name][index_name] = index_id;
  page_id_t page_id;
  auto index_meta_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->index_meta_pages_[index_id] = page_id;
  buffer_pool_manager_->UnpinPage(page_id, true);
  auto index_meta = IndexMetadata::Create(index_id, index_name, table_names_[table_name], key_map);
  index_meta->SerializeTo(index_meta_page->GetData());
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info->second, buffer_pool_manager_);
  auto table_heap = table_info->second->GetTableHeap();
  vector<Field> f;
  for (auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++) {
    f.clear();
    for (auto pos : key_map) {
      f.push_back(*(it->GetField(pos)));
    }
    Row row(f);
    index_info->GetIndex()->InsertEntry(row, it->GetRowId(), nullptr);
  }
  indexes_[index_id] = index_info;
  next_index_id_++;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto index_vec = index_names_.find(table_name);
  if(index_vec == index_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  auto index = index_vec->second.find(index_name);
  if(index == index_vec->second.end())
  {
    return DB_INDEX_NOT_FOUND;
  }
  index_info = indexes_.find(index->second)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto index_vec = index_names_.find(table_name);
  if(index_vec == index_names_.end())
  {
    return DB_INDEX_NOT_FOUND;
  }
  indexes.clear();
  for(auto &it : index_vec->second)
  {
    indexes.push_back(indexes_.find(it.second)->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  auto temp = table_names_.find(table_name);
  if(temp == table_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  auto table_id = temp->second;
  table_names_.erase(table_name);

  page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];
  catalog_meta_->table_meta_pages_.erase(table_id);

  buffer_pool_manager_->DeletePage(page_id);
  tables_.erase(table_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto temp = index_names_.find(table_name);
  if(temp == index_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  if((temp->second.find(index_name) == temp->second.end()))
  {
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id = index_names_[table_name][index_name];
  index_names_.erase(table_name);

  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);

  buffer_pool_manager_->DeletePage(page_id);
  indexes_.erase(index_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto CatalogMetaPage = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(CatalogMetaPage->GetData());
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if(tables_.count(table_id) != 0){
    return DB_TABLE_ALREADY_EXIST;
  }
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  auto table_meta_page = buffer_pool_manager_->FetchPage(page_id);

  TableMetadata *table_meta;
  TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);
  auto table_name = table_meta->GetTableName();
  table_names_[table_name] = table_id;

  auto table_heap = TableHeap::Create(buffer_pool_manager_, page_id, table_meta->GetSchema(), log_manager_, lock_manager_);
  auto table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  tables_.emplace(table_id, table_info);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if(indexes_.count(index_id) != 0)
  {
    return DB_TABLE_ALREADY_EXIST;
  }
  catalog_meta_->index_meta_pages_[index_id] = page_id;
  auto index_meta_page = buffer_pool_manager_->FetchPage(page_id);

  IndexMetadata *index_meta;
  IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);
  auto table_info = tables_[index_meta->GetTableId()];
  auto table_name = table_info->GetTableName();
  index_names_[table_name][index_meta->GetIndexName()] = index_id;

  auto index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_.emplace(index_id, index_info);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto temp = tables_.find(table_id);
  if(temp == tables_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_info = temp->second;
  return DB_SUCCESS;
}