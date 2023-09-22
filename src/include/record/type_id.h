#ifndef MINISQL_TYPE_ID_H
#define MINISQL_TYPE_ID_H

#include <string>

enum TypeId { kTypeInvalid = 0, kTypeInt, kTypeFloat, kTypeChar, KMaxTypeId = kTypeChar };

inline std::string TypeToString(TypeId type) {
  if (type == kTypeInvalid) {
    return "Invalid";
  }
  else if (type == kTypeInt) {
    return "Int";
  }
  else if (type == kTypeChar) {
    return "Char";
  }
  else return "Error";
}

#endif  // MINISQL_TYPE_ID_H
