#include "record/row.h"

/**
 * Student added functions about null bitmap in serialization.
 */

namespace NullBitmap {

bool IsFieldNull(unsigned char *bitmap, size_t num) {
  size_t byte_offset = num / 8, bit_offset = num % 8;
  unsigned char byte = bitmap[byte_offset];
  unsigned char mask = '\01' << bit_offset;
  return !(byte & mask);
}

bool SetNotNull(unsigned char *bitmap, size_t num) {
  size_t byte_offset = num / 8, bit_offset = num % 8;
  unsigned char *byte = bitmap + byte_offset;
  unsigned char mask = '\01' << bit_offset;
  *byte |= mask;
  return !IsFieldNull(bitmap, num);
}

bool SetNull(unsigned char *bitmap, size_t num) {
  size_t byte_offset = num / 8, bit_offset = num % 8;
  unsigned char *byte = bitmap + byte_offset;
  unsigned char mask = '\01' << bit_offset;
  *byte &= ~mask;
  return IsFieldNull(bitmap, num);
}

}


/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  size_t fieldNum = schema->GetColumnCount();
  uint32_t size = 0;
  MACH_WRITE_TO(RowId, buf + size, this->rid_);
  size += sizeof this->rid_;
  MACH_WRITE_TO(size_t, buf + size, fieldNum);
  size_t bitmapSize = (fieldNum - 1) / 8 + 1;
  size += sizeof fieldNum;
  auto bitmap = new unsigned char[bitmapSize];
  for (size_t i = 0; i < fieldNum; ++i) {
    if (fields_[i]->IsNull()) {
      NullBitmap::SetNull(bitmap, i);
    } else {
      NullBitmap::SetNotNull(bitmap, i);
    }
  }
  for (size_t i = 0; i < bitmapSize; ++i) {
    MACH_WRITE_TO(unsigned char, buf + size + i * sizeof(unsigned char), bitmap[i]);
  }
  size += sizeof(unsigned char) * bitmapSize;
  for (auto i: fields_) {
    if (i->IsNull()) continue;
    size += i->SerializeTo(buf + size);
  }
  return size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  uint32_t size = 0;
  this->rid_ = MACH_READ_FROM(RowId, buf + size);
  size += sizeof this->rid_;
  size_t fieldNum = MACH_READ_FROM(size_t, buf + size);
  size += sizeof fieldNum;
  size_t bitmapSize = (fieldNum - 1) / 8 + 1;
  auto bitmap = new unsigned char [bitmapSize];
  for (size_t i = 0; i < bitmapSize; ++i) {
    bitmap[i] = MACH_READ_FROM(unsigned char, buf + size + i * sizeof(unsigned char));
  }
  size += sizeof(unsigned char) * bitmapSize;
  for (int i = 0; i < fieldNum; ++i) {
    fields_.push_back(nullptr);
    if (NullBitmap::IsFieldNull(bitmap, i)) {
      fields_[i] = new Field(schema->GetColumn(i)->GetType());
    }
    else {
      size += Field::DeserializeFrom(buf + size, schema->GetColumn(i)->GetType(), &fields_[i], false);
    }
  }
  return size;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 0;
  size += sizeof this->rid_ + sizeof (size_t) + ((schema->GetColumnCount() - 1) / 8 + 1);
  for (auto i: fields_) {
    size += i->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
