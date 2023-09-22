#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t size = 0;
  size_t name_size = this->name_.length();
  MACH_WRITE_TO(size_t, buf + size, name_size);
  size += sizeof name_size;
  MACH_WRITE_STRING(buf + size, this->name_);
  size += name_.length();
  MACH_WRITE_TO(TypeId, buf + size, this->type_);
  size += sizeof type_;
  if (this->type_ == kTypeChar) {
    MACH_WRITE_TO(uint32_t, buf + size, this->len_);
    size += sizeof len_;
  }
  MACH_WRITE_TO(uint32_t, buf + size, this->table_ind_);
  size += sizeof table_ind_;
  MACH_WRITE_TO(bool, buf + size, this->nullable_);
  size += sizeof nullable_;
  MACH_WRITE_TO(bool, buf + size, this->unique_);
  size += sizeof unique_;
//  LOG(INFO) << "Column::SerializeTo() called: " << "Column (name: " << this->name_ << ", type: " << TypeToString(this->type_)
//            << ", length: " << this->len_ << ", table_index: " << this->table_ind_ << ", nullable: " << this->nullable_
//            << ", unique: " << this->unique_ << ") is serialized into buffer " << (void*)buf << std::endl;
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return name_.length() + sizeof (size_t) + sizeof type_ + sizeof table_ind_ + sizeof nullable_ + sizeof unique_ + (type_ == kTypeChar ? sizeof len_ : 0);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if (column != nullptr) {
//    LOG(INFO) << "Column::DeserializeFrom() failed: Column has already been serialized!" << std::endl;
    return 0;
  }
  size_t name_size;
  uint32_t size = 0, len;
  std::string name = "";
  char c;
  name_size = MACH_READ_FROM(size_t, buf + size);
  size += sizeof name_size;
  for (size_t i = 0; i < name_size; ++i) {
    c = MACH_READ_FROM(char, buf + size + i * sizeof(char));
    name.push_back(c);
  }
  size += name.length();
  TypeId type = MACH_READ_FROM(TypeId, buf + size);
  size += sizeof type;
  if (type == kTypeChar){
    len = MACH_READ_FROM(uint32_t, buf + size);
    size += sizeof len;
  }
  uint32_t table_ind = MACH_READ_FROM(uint32_t, buf + size);
  size += sizeof table_ind;
  bool nullable = MACH_READ_FROM(bool, buf + size);
  size += sizeof nullable;
  bool unique = MACH_READ_FROM(bool, buf + size);
  size += sizeof unique;
  if (type == kTypeChar) {
    column = new Column(name, type, len, table_ind, nullable, unique);
  }
  else {
    column = new Column(name, type, table_ind, nullable, unique);
  }
  if (column == nullptr) {
//    LOG(INFO) << "Column::DeserializeFrom() crashed!" << std::endl;
    return 0;
  }
//  LOG(INFO) << "Column::DeserializeFrom() called: " << "Column (name: " << column->name_ << ", type: " << TypeToString(column->type_)
//            << ", length: " << column->len_ << ", table_index: " << column->table_ind_ << ", nullable: " << column->nullable_
//            << ", unique: " << column->unique_ << ") is deserialized from buffer " << (void*)buf << std::endl;
  return size;
}
