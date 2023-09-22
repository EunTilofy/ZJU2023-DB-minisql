#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"

char *chars[] = {const_cast<char *>(""), const_cast<char *>("hello"), const_cast<char *>("world!"),
                 const_cast<char *>("\0")};

Field int_fields[] = {
    Field(TypeId::kTypeInt, 188), Field(TypeId::kTypeInt, -65537), Field(TypeId::kTypeInt, 33389),
    Field(TypeId::kTypeInt, 0),   Field(TypeId::kTypeInt, 999),
};
Field float_fields[] = {
    Field(TypeId::kTypeFloat, -2.33f),
    Field(TypeId::kTypeFloat, 19.99f),
    Field(TypeId::kTypeFloat, 999999.9995f),
    Field(TypeId::kTypeFloat, -77.7f),
};
Field char_fields[] = {Field(TypeId::kTypeChar, chars[0], strlen(chars[0]), false),
                       Field(TypeId::kTypeChar, chars[1], strlen(chars[1]), false),
                       Field(TypeId::kTypeChar, chars[2], strlen(chars[2]), false),
                       Field(TypeId::kTypeChar, chars[3], 1, false)};
Field null_fields[] = {Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)};

TEST(TupleTest, FieldSerializeDeserializeTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  for (int i = 0; i < 4; i++) {
    p += int_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 3; i++) {
    p += float_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 4; i++) {
    p += char_fields[i].SerializeTo(p);
  }
  // Deserialize phase
  uint32_t ofs = 0;
  Field *df = nullptr;
  for (int i = 0; i < 4; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeInt, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(int_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(int_fields[4]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(int_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(int_fields[2]));
    delete df;
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeFloat, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(float_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(float_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(float_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(float_fields[2]));
    delete df;
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeChar, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(char_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(char_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[2]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(char_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(char_fields[2]));
    delete df;
    df = nullptr;
  }
}

TEST (TupleTest, ColumnSerializationTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  vector<Column*> columns1{nullptr, nullptr, nullptr};
  vector<Column*> columns2 = columns;
  for (int i = 0; i < 3; ++i) {
    columns[i]->SerializeTo(buffer);
    Column::DeserializeFrom(buffer, columns1[i]);
  }
  ASSERT_EQ(columns1.size(), columns2.size());
  for (uint32_t i = 0; i < columns1.size(); i++) {
    auto column1 = columns1[i];
    auto column2 = columns2[i];
    ASSERT_FALSE(column1 == nullptr);
    ASSERT_FALSE(column2 == nullptr);
    ASSERT_EQ(column1->GetSerializedSize(), column2->GetSerializedSize());
    ASSERT_EQ(column1->GetName(), column2->GetName());
    ASSERT_EQ(column1->GetType(), column2->GetType());
    ASSERT_EQ(column1->GetLength(), column2->GetLength());
    ASSERT_EQ(column1->GetTableInd(), column2->GetTableInd());
    ASSERT_EQ(column1->IsNullable(), column2->IsNullable());
    ASSERT_EQ(column1->GetSerializedSize(), column2->GetSerializedSize());
  }
}

TEST (TupleTest, SchemaSerializationTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  Schema *schema1 = new Schema(columns), *schema2 = nullptr;
  schema1->SerializeTo(buffer);
  Schema::DeserializeFrom(buffer, schema2);
  ASSERT_EQ(schema1->GetColumnCount(), schema2->GetColumnCount());
  vector<Column*> columns1 = schema1->GetColumns();
  vector<Column*> columns2 = schema2->GetColumns();
  for (uint32_t i = 0; i < columns1.size(); i++) {
    auto column1 = columns1[i];
    auto column2 = columns2[i];
    ASSERT_FALSE(column1 == nullptr);
    ASSERT_FALSE(column2 == nullptr);
    ASSERT_EQ(column1->GetSerializedSize(), column2->GetSerializedSize());
    ASSERT_EQ(column1->GetName(), column2->GetName());
    ASSERT_EQ(column1->GetType(), column2->GetType());
    ASSERT_EQ(column1->GetLength(), column2->GetLength());
    ASSERT_EQ(column1->GetTableInd(), column2->GetTableInd());
    ASSERT_EQ(column1->IsNullable(), column2->IsNullable());
    ASSERT_EQ(column1->GetSerializedSize(), column2->GetSerializedSize());
  }
}

TEST (TupleTest, RowSerializationTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};
  Schema *schema = new Schema(columns);
  Row row1(fields), row2;
  row1.SerializeTo(buffer, schema);
  row2.DeserializeFrom(buffer, schema);
  ASSERT_EQ(row1.GetSerializedSize(schema), row2.GetSerializedSize(schema));
  ASSERT_EQ(row1.GetFieldCount(), row2.GetFieldCount());
  ASSERT_EQ(row1.GetRowId().GetPageId(), row2.GetRowId().GetPageId());
  ASSERT_EQ(row1.GetRowId().GetSlotNum(), row2.GetRowId().GetSlotNum());
  auto fields1 = row1.GetFields();
  auto fields2 = row2.GetFields();
  for (uint32_t i = 0; i < fields.size(); i++) {
    Field *field1 = fields1[i];
    Field *field2 = fields2[i];
    ASSERT_EQ(field1->IsNull(), field2->IsNull());
    ASSERT_EQ(field1->GetTypeId(), field2->GetTypeId());
    if (field1->GetTypeId() == kTypeChar) {
      ASSERT_EQ(field1->GetLength(), field2->GetLength());
      ASSERT_FALSE(std::string(field1->GetData()).compare(field2->GetData()));
    }
    else {
      ASSERT_TRUE(field1->CheckComparable(*field2));
      ASSERT_TRUE(field1->CompareEquals(*field2));
    }
  }
}

TEST(TupleTest, RowTest) {
  TablePage table_page;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};
  auto schema = std::make_shared<Schema>(columns);
  Row row(fields);
  table_page.Init(0, INVALID_PAGE_ID, nullptr, nullptr);
  table_page.InsertTuple(row, schema.get(), nullptr, nullptr, nullptr);
  RowId first_tuple_rid;
  ASSERT_TRUE(table_page.GetFirstTupleRid(&first_tuple_rid));
  ASSERT_EQ(row.GetRowId(), first_tuple_rid);
  Row row2(row.GetRowId());
  ASSERT_TRUE(table_page.GetTuple(&row2, schema.get(), nullptr, nullptr));
  std::vector<Field *> &row2_fields = row2.GetFields();
  ASSERT_EQ(3, row2_fields.size());
  for (size_t i = 0; i < row2_fields.size(); i++) {
    ASSERT_EQ(CmpBool::kTrue, row2_fields[i]->CompareEquals(fields[i]));
  }
  ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
}