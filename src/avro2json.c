#include <avro.h>
#include <avro/schema.h>
#include <errno.h>
#include <jansson.h>
#include <stdlib.h>
#include <string.h>

#include "avro_private.h"
#include "logical.h"

#if defined(_WIN32) || defined(_WIN64)
#define strtok_r strtok_s
#endif

typedef struct {
  int prune;
  int logical_types;
  int show_schema;
  int32_t *columns;
  size_t columns_size;
} config_t;

typedef struct {
  decimal_t *dec;
  char *str;
  size_t str_size;
  uint8_t *utf8;
  size_t utf8_size;
} cache_t;

static cache_t *cache_new() {
  cache_t *cache = (cache_t *)malloc(sizeof(cache_t));
  memset(cache, 0, sizeof(cache_t));
  cache->dec = decimal_new();
  return cache;
}

static void cache_free(cache_t *cache) {
  decimal_free(cache->dec);
  free(cache->str);
  free(cache->utf8);
  free(cache);
}

/*
 * Converts a binary buffer into a NUL-terminated JSON UTF-8 string.
 * Avro bytes and fixed values are encoded in JSON as a string, and JSON
 * strings must be in UTF-8.  For these Avro types, the JSON string is
 * restricted to the characters U+0000..U+00FF, which corresponds to the
 * ISO-8859-1 character set.  This function performs this conversion.
 */
static int encode_utf8_bytes(const void *src, size_t src_len, void **dest,
                             size_t *dest_len, cache_t *cache) {

  // First, determine the size of the resulting UTF-8 buffer.
  // Bytes in the range 0x00..0x7f will take up one byte; bytes in
  // the range 0x80..0xff will take up two.
  const uint8_t *src8 = (const uint8_t *)src;

  size_t utf8_len = src_len + 1; // +1 for NUL terminator
  size_t i;
  for (i = 0; i < src_len; i++) {
    if (src8[i] & 0x80) {
      utf8_len++;
    }
  }

  // Reuse a cached buffer for the UTF-8 string and fill it in (resize the
  // buffer when needed).
  uint8_t *dest8 = cache->utf8;
  if (cache->utf8_size < utf8_len) {
    cache->utf8 = (uint8_t *)realloc(cache->utf8, sizeof(uint8_t) * utf8_len);
    cache->utf8_size = utf8_len;
    dest8 = cache->utf8;
  }
  if (dest8 == NULL) {
    avro_set_error("Cannot allocate JSON bytes buffer");
    return ENOMEM;
  }

  uint8_t *curr = dest8;
  for (i = 0; i < src_len; i++) {
    if (src8[i] & 0x80) {
      *curr++ = (0xc0 | (src8[i] >> 6));
      *curr++ = (0x80 | (src8[i] & 0x3f));
    } else {
      *curr++ = src8[i];
    }
  }

  *curr = '\0';

  // And we're good.
  *dest = dest8;
  *dest_len = utf8_len;
  return 0;
}

#ifndef isnan
#ifndef __sun
static int isnan(double x) { return x != x; }
#endif
#endif
#ifndef isinf
static int isinf(double x) { return !isnan(x) && isnan(x - x); }
#endif

#define return_json(type, exp)                                                 \
  {                                                                            \
    json_t *result = exp;                                                      \
    if (result == NULL) {                                                      \
      avro_set_error("Cannot allocate JSON " type);                            \
    }                                                                          \
    return result;                                                             \
  }

#define return_json_real(type, val)                                            \
  {                                                                            \
    if (isinf(val)) {                                                          \
      return_json("string", json_string("Infinity"));                          \
    }                                                                          \
    if (isnan(val)) {                                                          \
      return_json("string", json_string("NaN"));                               \
    }                                                                          \
    return_json(type, json_real(val));                                         \
  }

#define check_return(retval, call)                                             \
  do {                                                                         \
    int __rc;                                                                  \
    __rc = call;                                                               \
    if (__rc != 0) {                                                           \
      return retval;                                                           \
    }                                                                          \
  } while (0)

static json_t *avro_bytes_value_to_json_t(const avro_value_t *value,
                                          const void *bytes, size_t size,
                                          const config_t *conf,
                                          cache_t *cache) {
  avro_logical_schema_t *logical_type = NULL;
  if (conf->logical_types) {
    logical_type = avro_logical_schema(avro_value_get_schema(value));
  }

  if (logical_type != NULL) {
    if (logical_type->type != AVRO_DECIMAL) {
      avro_set_error("Unsupported logical type annotation in BYTES/FIXED type");
      return NULL;
    }

    decimal_from_bytes(cache->dec, (int8_t *)bytes, size, logical_type->scale);
    char *str = decimal_to_str(cache->dec, &cache->str, &cache->str_size);
    if (str == NULL) {
      avro_set_error("Cannot render decimal value");
      return NULL;
    }
    return_json("string", json_string(str));
  }

  void *encoded = NULL;
  size_t encoded_size = 0;
  if (encode_utf8_bytes(bytes, size, &encoded, &encoded_size, cache)) {
    return NULL;
  }
  return_json("string", json_stringn((const char *)encoded, encoded_size - 1));
}

static json_t *avro_value_to_json_t(const avro_value_t *value, int top_level,
                                    const config_t *conf, cache_t *cache) {
  switch (avro_value_get_type(value)) {
  case AVRO_BOOLEAN: {
    int val;
    check_return(NULL, avro_value_get_boolean(value, &val));
    return_json("boolean", val ? json_true() : json_false());
  }

  case AVRO_BYTES: {
    const void *val;
    size_t size;
    check_return(NULL, avro_value_get_bytes(value, &val, &size));
    return avro_bytes_value_to_json_t(value, val, size, conf, cache);
  }

  case AVRO_DOUBLE: {
    double val;
    check_return(NULL, avro_value_get_double(value, &val));
    return_json_real("double", val);
  }

  case AVRO_FLOAT: {
    float val;
    check_return(NULL, avro_value_get_float(value, &val));
    return_json_real("float", val);
  }

  case AVRO_INT32: {
    int32_t val;
    check_return(NULL, avro_value_get_int(value, &val));

    avro_logical_schema_t *logical_type = NULL;
    if (conf->logical_types) {
      logical_type = avro_logical_schema(avro_value_get_schema(value));
    }

    if (logical_type != NULL) {
      if (logical_type->type == AVRO_DATE) {
        return_json("string", json_string(epoch_days_to_str(val)));
      }
      if (logical_type->type == AVRO_TIME_MILLIS) {
        return_json("string", json_string(time_millis_to_str(val)));
      }
      avro_set_error("INT type is annotated by an unsupported logical type");
      return NULL;
    }
    return_json("int", json_integer(val));
  }

  case AVRO_INT64: {
    int64_t val;
    check_return(NULL, avro_value_get_long(value, &val));

    avro_logical_schema_t *logical_type = NULL;
    if (conf->logical_types) {
      logical_type = avro_logical_schema(avro_value_get_schema(value));
    }

    if (logical_type != NULL) {
      if (logical_type->type == AVRO_TIME_MICROS) {
        return_json("string", json_string(time_micros_to_str(val)));
      }
      if (logical_type->type == AVRO_TIMESTAMP_MILLIS) {
        return_json("string", json_string(timestamp_millis_to_str(val)));
      }
      if (logical_type->type == AVRO_TIMESTAMP_MICROS) {
        return_json("string", json_string(timestamp_micros_to_str(val)));
      }
      avro_set_error("LONG type is annotated by an unsupported logical type");
      return NULL;
    }
    return_json("long", json_integer(val));
  }

  case AVRO_NULL: {
    check_return(NULL, avro_value_get_null(value));
    return_json("null", json_null());
  }

  case AVRO_STRING: {
    const char *val;
    size_t size;
    check_return(NULL, avro_value_get_string(value, &val, &size));
    return_json("string", json_string(val));
  }

  case AVRO_ARRAY: {
    int rc;
    size_t element_count, i;
    json_t *result = json_array();
    if (result == NULL) {
      avro_set_error("Cannot allocate JSON array");
      return NULL;
    }

    rc = avro_value_get_size(value, &element_count);
    if (rc != 0) {
      json_decref(result);
      return NULL;
    }

    for (i = 0; i < element_count; i++) {
      avro_value_t element;
      rc = avro_value_get_by_index(value, i, &element, NULL);
      if (rc != 0) {
        json_decref(result);
        return NULL;
      }

      json_t *element_json = avro_value_to_json_t(&element, 0, conf, cache);
      if (element_json == NULL) {
        json_decref(result);
        return NULL;
      }

      if (json_array_append_new(result, element_json)) {
        avro_set_error("Cannot append element to array");
        json_decref(result);
        return NULL;
      }
    }

    return result;
  }

  case AVRO_ENUM: {
    avro_schema_t enum_schema;
    int symbol_value;
    const char *symbol_name;

    check_return(NULL, avro_value_get_enum(value, &symbol_value));
    enum_schema = avro_value_get_schema(value);
    symbol_name = avro_schema_enum_get(enum_schema, symbol_value);
    return_json("enum", json_string(symbol_name));
  }

  case AVRO_FIXED: {
    const void *val;
    size_t size;
    check_return(NULL, avro_value_get_fixed(value, &val, &size));
    return avro_bytes_value_to_json_t(value, val, size, conf, cache);
  }

  case AVRO_MAP: {
    int rc;
    size_t element_count, i;
    json_t *result = json_object();
    if (result == NULL) {
      avro_set_error("Cannot allocate JSON map");
      return NULL;
    }

    rc = avro_value_get_size(value, &element_count);
    if (rc != 0) {
      json_decref(result);
      return NULL;
    }

    for (i = 0; i < element_count; i++) {
      const char *key;
      avro_value_t element;

      rc = avro_value_get_by_index(value, i, &element, &key);
      if (rc != 0) {
        json_decref(result);
        return NULL;
      }

      json_t *element_json = avro_value_to_json_t(&element, 0, conf, cache);
      if (element_json == NULL) {
        json_decref(result);
        return NULL;
      }

      if (json_object_set_new(result, key, element_json)) {
        avro_set_error("Cannot append element to map");
        json_decref(result);
        return NULL;
      }
    }

    return result;
  }

  case AVRO_RECORD: {
    int rc;
    size_t field_count, i;
    json_t *result = json_object();
    if (result == NULL) {
      avro_set_error("Cannot allocate new JSON record");
      return NULL;
    }

    rc = avro_value_get_size(value, &field_count);
    if (rc != 0) {
      json_decref(result);
      return NULL;
    }

    int filter_cols = top_level && conf->columns_size > 0;

    for (i = 0; i < field_count; i++) {
      if (filter_cols && (i >= conf->columns_size || !conf->columns[i])) {
        continue;
      }

      const char *field_name;
      avro_value_t field;

      rc = avro_value_get_by_index(value, i, &field, &field_name);
      if (rc != 0) {
        json_decref(result);
        return NULL;
      }

      json_t *field_json = avro_value_to_json_t(&field, 0, conf, cache);
      if (field_json == NULL) {
        json_decref(result);
        return NULL;
      }

      if (conf->prune &&
          (json_is_null(field_json) ||
           (json_is_object(field_json) && !json_object_size(field_json)) ||
           (json_is_array(field_json) && !json_array_size(field_json)))) {
        continue;
      }

      if (json_object_set_new(result, field_name, field_json)) {
        avro_set_error("Cannot append field to record");
        json_decref(result);
        return NULL;
      }
    }

    return result;
  }

  case AVRO_UNION: {
    avro_value_t branch;
    check_return(NULL, avro_value_get_current_branch(value, &branch));
    return avro_value_to_json_t(&branch, 0, conf, cache);
  }

  default:
    return NULL;
  }
}

static int avro_to_json(const avro_value_t *value, char **json_str,
                        const config_t *conf, cache_t *cache) {
  json_t *json = avro_value_to_json_t(value, 1, conf, cache);
  if (json == NULL) {
    return ENOMEM;
  }

  *json_str =
      json_dumps(json, JSON_ENCODE_ANY | JSON_COMPACT | JSON_ENSURE_ASCII);
  json_decref(json);
  return 0;
}

// Nullable types are represented as UNION of NULL and target schema.
// This function extracts the target schema from such a UNION.
static avro_schema_t get_nullable_schema(avro_schema_t schema) {
  if (is_avro_union(schema)) {
    const struct avro_union_schema_t *uschema = avro_schema_to_union(schema);
    if (uschema->branches->num_entries == 2) {
      union {
        st_data_t data;
        avro_schema_t schema;
      } val1;
      st_lookup(uschema->branches, 0, &val1.data);

      union {
        st_data_t data;
        avro_schema_t schema;
      } val2;
      st_lookup(uschema->branches, 1, &val2.data);

      if (val2.schema->type == AVRO_NULL) {
        schema = val1.schema;
      } else if (val1.schema->type == AVRO_NULL) {
        schema = val2.schema;
      }
    }
  }
  return schema;
}

static int print_schema(avro_schema_t schema) {
  schema = get_nullable_schema(schema);

  if (!is_avro_record(schema)) {
    avro_set_error("Can't find root record schema");
    return EINVAL;
  }

  struct avro_record_schema_t *record_schema = avro_schema_to_record(schema);
  json_t *result = json_array();
  if (result == NULL) {
    avro_set_error("Cannot allocate JSON array");
    return ENOMEM;
  }

  for (int i = 0; i < record_schema->fields->num_entries; i++) {
    union {
      st_data_t data;
      struct avro_record_field_t *field;
    } val;
    st_lookup(record_schema->fields, i, &val.data);

    avro_schema_t field_schema = get_nullable_schema(val.field->type);
    avro_logical_schema_t *logical_type = avro_logical_schema(field_schema);

    json_t *obj = json_object();
    if (obj == NULL) {
      avro_set_error("Cannot allocate JSON object");
      return ENOMEM;
    }
    json_object_set_new(obj, "name", json_string(val.field->name));

    int found_logical_type = 0;
    if (logical_type != NULL) {
      switch (logical_type->type) {
      case AVRO_DECIMAL:
        json_object_set_new(obj, "type", json_string("decimal"));
        found_logical_type = 1;
        break;
      case AVRO_DATE:
      case AVRO_TIME_MILLIS:
      case AVRO_TIME_MICROS:
      case AVRO_TIMESTAMP_MILLIS:
      case AVRO_TIMESTAMP_MICROS:
        json_object_set_new(obj, "type", json_string("datetime"));
        found_logical_type = 1;
        break;
      case AVRO_DURATION:
        json_object_set_new(obj, "type", json_string("timespan"));
        found_logical_type = 1;
        break;
      }
    }

    if (!found_logical_type) {
      switch (field_schema->type) {
      case AVRO_FIXED:
      case AVRO_NULL:
      case AVRO_STRING:
      case AVRO_ENUM:
        json_object_set_new(obj, "type", json_string("string"));
        break;
      case AVRO_INT32:
        json_object_set_new(obj, "type", json_string("int"));
        break;
      case AVRO_INT64:
        json_object_set_new(obj, "type", json_string("long"));
        break;
      case AVRO_FLOAT:
      case AVRO_DOUBLE:
        json_object_set_new(obj, "type", json_string("real"));
        break;
      case AVRO_BOOLEAN:
        json_object_set_new(obj, "type", json_string("bool"));
        break;
      case AVRO_BYTES:
      default:
        json_object_set_new(obj, "type", json_string("dynamic"));
        break;
      }
    }
    json_array_append_new(result, obj);
  }

  char *json_str =
      json_dumps(result, JSON_ENCODE_ANY | JSON_COMPACT | JSON_ENSURE_ASCII);
  json_decref(result);
  printf("%s", json_str);
  return EOF;
}

static void process_file(const char *filename, const config_t *conf) {
  avro_file_reader_t reader;
  if (avro_file_reader(filename, &reader)) {
    fprintf(stderr, "Error opening file '%s': %s\n", filename, avro_strerror());
    exit(1);
  }

  int rval;
  avro_schema_t wschema = avro_file_reader_get_writer_schema(reader);

  if (!conf->show_schema) {
    avro_value_iface_t *iface = avro_generic_class_from_schema(wschema);
    avro_value_t value;
    avro_generic_value_new(iface, &value);
    cache_t *cache = cache_new();

    while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
      char *json;

      if (avro_to_json(&value, &json, conf, cache)) {
        fprintf(stderr, "Error converting value to JSON: %s\n",
                avro_strerror());
      } else {
        int ret = printf("%s\n", json);
        free(json);
        if (ret < 0) {
          rval = EOF;
          break;
        }
      }

      avro_value_reset(&value);
    }

    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    cache_free(cache);

  } else {
    rval = print_schema(wschema);
  }

  avro_schema_decref(wschema);
  avro_file_reader_close(reader);
}

static void print_usage(const char *exe) {
  fprintf(stderr,
          "Usage: %s [OPTIONS] FILE\n"
          "\n"
          "Where options are:\n"
          " --show-schema      Only show Avro file schema, and exit\n"
          " --prune            Omit null values as well as empty lists and "
          "objects\n"
          " --logical-types    Convert logical types automatically\n"
          " --columns 1,2,...  Only output specified columns numbers\n",
          exe);
  exit(1);
}

static int parse_columns_indices(char *cols_list, int32_t **columns,
                                 size_t *columns_size) {
  const char *p = cols_list;
  size_t cols_num = 1;
  while (*p) {
    cols_num += *p++ == ',' ? 1 : 0;
  }
  int32_t *cols_indices = (int32_t *)malloc(sizeof(int32_t) * cols_num);
  char *saveptr;
  char *col = strtok_r(cols_list, ",", &saveptr);
  int32_t *c = cols_indices;
  do {
    int col_idx = atoi(col);
    if (col_idx <= 0) {
      return EINVAL;
      free(cols_indices);
    }
    *c++ = col_idx;
    col = strtok_r(NULL, ",", &saveptr);
  } while (col != NULL);

  size_t size = 0;
  for (int i = 0; i < cols_num; ++i) {
    if (cols_indices[i] > size) {
      size = cols_indices[i];
    }
  }
  int32_t *cols = (int32_t *)malloc(sizeof(int32_t) * size);
  memset(cols, 0, sizeof(int32_t) * size);
  for (int i = 0; i < cols_num; ++i) {
    cols[cols_indices[i] - 1] = 1;
  }
  free(cols_indices);
  *columns = cols;
  *columns_size = size;
  return 0;
}

static const char *parse_args(int argc, char **argv, config_t *conf) {
  int arg_idx;
  for (arg_idx = 1; arg_idx < argc - 1; ++arg_idx) {
    if (!strcmp(argv[arg_idx], "--prune")) {
      conf->prune = 1;
    } else if (!strcmp(argv[arg_idx], "--logical-types")) {
      conf->logical_types = 1;
    } else if (!strcmp(argv[arg_idx], "--show-schema")) {
      conf->show_schema = 1;
    } else if (!strcmp(argv[arg_idx], "--columns") && arg_idx < argc - 1) {
      if (parse_columns_indices(argv[++arg_idx], &conf->columns,
                                &conf->columns_size)) {
        print_usage(argv[0]);
      }
    } else {
      print_usage(argv[0]);
    }
  }

  if (arg_idx != argc - 1) {
    print_usage(argv[0]);
  }

  return argv[arg_idx];
}

int main(int argc, char **argv) {
  config_t conf = {.prune = 0,
                   .logical_types = 0,
                   .show_schema = 0,
                   .columns = NULL,
                   .columns_size = 0};

  const char *file = parse_args(argc, argv, &conf);
  process_file(file, &conf);

  if (conf.columns) {
    free(conf.columns);
  }
  return 0;
}
