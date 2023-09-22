//
// Created by PaperCloud on 2023/6/8.
//
#include "index/hash_table.h"
#include "index/hash_index.h"
#include "index/hash_index_iterator.h"
#include "page/hash_page.h"
#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/utils.h"
#include <string>
#include "index/generic_key.h"

static const std::string db_name = "hash_index_test.db";

TEST(HashIndexTests, HashIndexSimpleTest) {
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<uint32_t> index_key_map{0, 1};
  const TableSchema table_schema(columns);
  auto *index_schema = Schema::ShallowCopySchema(&table_schema, index_key_map);
  auto *index = new HashIndex(0, index_schema, 256, engine.bpm_);
  for (int i = 0; i < 10; i++) {
    std::vector<Field> fields{Field(TypeId::kTypeInt, i),
                              Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
    Row row(fields);
    RowId rid(1000, i);
    ASSERT_EQ(DB_SUCCESS, index->InsertEntry(row, rid, nullptr));
    LOG(INFO) << "successfully insert entry !" << std::endl;
  }
  //  Test Scan
  std::vector<RowId> ret;
  for (int i = 0; i < 10; i++) {
    std::vector<Field> fields{Field(TypeId::kTypeInt, i),
                              Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
    Row row(fields);
    RowId rid(1000, i);
    ASSERT_EQ(DB_SUCCESS, index->ScanKey(row, ret, nullptr));
    ASSERT_EQ(rid.Get(), ret[i].Get());
  }
  // Iterator Scan
  HashIndexIterator iter = index->GetBeginIterator();
  uint32_t i = 0;
  for (; iter != index->GetEndIterator(); ++iter) {
    ASSERT_EQ(1000, (*iter).second.GetPageId());
    ASSERT_EQ(i, (*iter).second.GetSlotNum());
    i++;
  }
  delete index;
}

TEST(HashIndexTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  auto *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 21);
  HashTable tree(0, engine.bpm_, KP);
  // Prepare data
  const int n = 2000;
  vector<GenericKey *> keys;
  vector<RowId> values;
  vector<GenericKey *> delete_seq;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.emplace_back(i);
    delete_seq.push_back(key);
  }
  vector<GenericKey *> keys_copy(keys);
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < 140; i++) {
    tree.Insert(keys[i], values[i]);
  }
  for (int i = 140; i < n; ++i) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Search keys
  vector<RowId> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(keys_copy[i], ans);
    ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
    tree.Remove(delete_seq[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
  ASSERT_TRUE(tree.Check());
}

TEST(HashIndexTests, IndexIteratorTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  auto *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 21);
  BPlusTree tree(0, engine.bpm_, KP);
  // Generate insert record
  vector<GenericKey *> insert_key;

  int n = 10000;

  for (int i = 1; i <= n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    insert_key.emplace_back(key);
    tree.Insert(key, RowId(i * 100), nullptr);
  }
  // Generate delete record
  vector<GenericKey *> delete_key;
  for (int i = 2; i <= n; i += 2) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    delete_key.emplace_back(key);
    tree.Remove(key);
  }
  ASSERT_TRUE(tree.Check());
  // Search keys
  vector<RowId> v;
  vector<GenericKey *> not_delete_key;
  for (auto key : delete_key) {
    ASSERT_FALSE(tree.GetValue(key, v));
  }
  for (int i = 1; i <= n-1; i += 2) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    not_delete_key.emplace_back(key);
    ASSERT_TRUE(tree.GetValue(key, v));
    ASSERT_EQ(i * 100, v[v.size() - 1].Get());
  }
  ASSERT_TRUE(tree.Check());
  // Iterator
  int i = 0;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    ASSERT_TRUE(KP.CompareKeys(not_delete_key[i++], (*iter).first) == 0);  // if equal, CompareKeys return 0
    EXPECT_EQ(RowId((2 * i - 1) * 100), (*iter).second);
  }
  ASSERT_TRUE(tree.Check());
}