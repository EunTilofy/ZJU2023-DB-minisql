#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
//  LOG(INFO) << "Schema::SerializeTo() called." << std::endl;
  uint32_t size = 0;
  MACH_WRITE_TO(size_t, buf + size, columns_.size());
  size += sizeof columns_.size();
  for (auto i : columns_) {
    size += i->SerializeTo(buf + size);
  }
  MACH_WRITE_TO(bool, buf + size, is_manage_);
  size += sizeof is_manage_;
//  LOG(INFO) << "Schema::SerializeFrom() succeeded:" << (this->is_manage_ ? "Managed " : "Unmanaged ")
//            << "schema serialized to buffer " << (void*)buf << std::endl;
  return size;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 0;
  size += sizeof columns_.size() + sizeof is_manage_;
  for (auto i: columns_) {
    size += i->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
//  LOG(INFO) << "Schema::DeserializeFrom() called" << std::endl;
  if (schema != nullptr) {
//    LOG(INFO) << "Schema::DeserializeFrom() failed: Schema has already been serialized!" << std::endl;
    return 0;
  }
  delete schema;
  uint32_t size = 0;
  std::vector<Column*> columns;
  bool is_manage;
  size_t numColumns = MACH_READ_FROM(size_t, buf + size);
  size += sizeof numColumns;
  for (int i = 0; i < numColumns; ++i) {
    columns.push_back(nullptr);
    size += Column::DeserializeFrom(buf + size, columns[i]);
  }
  is_manage = MACH_READ_FROM(bool, buf + size);
  schema = new Schema(columns, is_manage);
  if (schema == nullptr) {
//    LOG(ERROR) << "Schema::Deserialization() crashed" << std::endl;
    return 0;
  }
//  LOG(INFO) << "Schema::DeserializeFrom() succeeded:" << (schema->is_manage_ ? "Managed " : "Unmanaged ")
//            << "schema deserialized from buffer " << (void*)buf << std::endl;
  return size;
}