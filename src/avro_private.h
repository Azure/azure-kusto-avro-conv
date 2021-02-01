/*
 * Some definitions needed for accessing schema info.
 */

#ifndef AVRO_PRIVATE_H
#define AVRO_PRIVATE_H

#include "st.h"

struct avro_record_field_t {
  int index;
  char *name;
  avro_schema_t type;
};

struct avro_record_schema_t {
  struct avro_obj_t obj;
  char *name;
  char *space;
  st_table *fields;
  st_table *fields_byname;
};

struct avro_union_schema_t {
	struct avro_obj_t obj;
	st_table *branches;
	st_table *branches_byname;
};

#define container_of(ptr_, type_, member_)                                     \
  ((type_ *)((char *)ptr_ - (size_t) & ((type_ *)0)->member_))

#define avro_schema_to_record(schema_)                                         \
  (container_of(schema_, struct avro_record_schema_t, obj))

#define avro_schema_to_union(schema_)                                          \
  (container_of(schema_, struct avro_union_schema_t, obj))

#endif // AVRO_PRIVATE_H
