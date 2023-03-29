#include <avro.h>
#include <avro/schema.h>
#include <errno.h>
#include <jansson.h>
#include <stdlib.h>
#if defined(_WIN32)
#include <jemalloc.h>
#endif
#include <string.h>

#include "avro_private.h"
#include "logical.h"

#if defined(_WIN32) || defined(_WIN64)
#define strtok_r strtok_s
#endif

typedef struct {
  int prune;
  int logical_types;
  int ms_hadoop_logical_types;
  int show_schema;
  int output_csv;
  int32_t *columns;
  size_t columns_size;
} config_t;

typedef struct {
  decimal_t *dec;
  char *str;
  size_t str_size;
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
  free(cache);
}

int JSON_ENCODE_FLAGS = JSON_ENCODE_ANY | JSON_COMPACT | JSON_ENSURE_ASCII;

#ifndef isnan
#ifndef __sun
static int isnan(double x) { return x != x; }
#endif
#endif
#ifndef isinf
static int isinf(double x) { return !isnan(x) && isnan(x - x); }
#endif

#define CHECKED_ALLOC(var, exp)                                                \
  do {                                                                         \
    var = exp;                                                                 \
    if (var == NULL) {                                                         \
      return ENOMEM;                                                           \
    }                                                                          \
  } while (0)

#define CHECKED_EV(call)                                                       \
  do {                                                                         \
    int __rc;                                                                  \
    __rc = call;                                                               \
    if (__rc != 0) {                                                           \
      return __rc;                                                             \
    }                                                                          \
  } while (0)

#define CHECKED_PRINT(dest, str)                                               \
  do {                                                                         \
    if (fprintf(dest, "%s", str) < 0) {                                        \
      return ferror(dest);                                                     \
    }                                                                          \
  } while (0)

#define CHECKED_PRINTF(dest, fmt, str)                                         \
  do {                                                                         \
    if (fprintf(dest, fmt, str) < 0) {                                         \
      return ferror(dest);                                                     \
    }                                                                          \
  } while (0)

// Guid is formatted as 36 characters (32 nibbles plus 4 hyphens): xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
// The byte order is a bit tricky: https://stackoverflow.com/questions/10862171/convert-byte-or-object-to-guid
#define GUID_FORMAT "%02hhX%02hhX%02hhX%02hhX-%02hhX%02hhX-%02hhX%02hhX-%02hhX%02hhX-%02hhX%02hhX%02hhX%02hhX%02hhX%02hhX"
#define GUID_ARG(guid) (guid)[3], (guid)[2], (guid)[1], (guid)[0], (guid)[5], (guid)[4], (guid)[7], (guid)[6], (guid)[8], (guid)[9], (guid)[10], (guid)[11], (guid)[12], (guid)[13], (guid)[14], (guid)[15]

static int is_ms_hadoop_logical_type_guid(const avro_value_t *value, size_t size) {
  if (size == 16) {
    avro_schema_t schema = avro_value_get_schema(value);
    return !strcmp(avro_schema_namespace(schema), "System") && !strcmp(avro_schema_name(schema), "Guid");
  }
  return 0;
}

static int record_field_to_csv(FILE *dest, const avro_value_t *value,
                              size_t field_idx, int print_comma, const config_t *conf, cache_t *cache);

static int record_field_to_json(json_t *result, const avro_value_t *value,
                              size_t field_idx, const config_t *conf, cache_t *cache);

int avro_byte_array_to_json_t(json_t **json, const unsigned char *bytes, size_t element_count) {
  int rval = 0;
  json_t *result = json_array();
  if (result == NULL) {
    avro_set_error("Cannot allocate JSON array");
    return ENOMEM;
  }

  static int printedByteArrayTelemetry = 0;
  if(!printedByteArrayTelemetry++) {
    fprintf(stderr, "Byte array detected\n", avro_strerror());
  }

  for (size_t i = 0; i < element_count; i++) {
    if ((rval = json_array_append_new(result, json_integer((unsigned char)bytes[i]))) != 0) {
      avro_set_error("Cannot append element to array");
      json_decref(result);
      return rval;
    }
  }

  CHECKED_ALLOC(*json, result);
  return 0;
}

static int avro_value_to_json_t(const avro_value_t *value, json_t **json,
                                int top_level, const config_t *conf,
                                cache_t *cache);

static int avro_bytes_value_to_json_t(const avro_value_t *value, json_t **json,
                                      const void *bytes, size_t size,
                                      const config_t *conf, cache_t *cache) {
  avro_logical_schema_t *logical_type = NULL;

  if (conf->logical_types) {
    logical_type = avro_logical_schema(avro_value_get_schema(value));
  }

  if (logical_type != NULL) {
    if (logical_type->type != AVRO_DECIMAL) {
      avro_set_error("Unsupported logical type annotation in BYTES/FIXED type");
      return EINVAL;
    }

    decimal_from_bytes(cache->dec, (int8_t *)bytes, size, logical_type->scale);
    char *str = decimal_to_str(cache->dec, &cache->str, &cache->str_size);
    if (str == NULL) {
      return ENOMEM;
    }
    CHECKED_ALLOC(*json, json_string_nocheck(str));
    return 0;
  }

  return avro_byte_array_to_json_t(json, bytes, size);
}

static int avro_array_to_json_t(const avro_value_t *value, json_t **json,
                                const config_t *conf, cache_t *cache) {
  int rval = 0;
  json_t *result = json_array();
  if (result == NULL) {
    avro_set_error("Cannot allocate JSON array");
    return ENOMEM;
  }

  size_t element_count;
  if ((rval = avro_value_get_size(value, &element_count)) != 0) {
    json_decref(result);
    return rval;
  }

  for (size_t i = 0; i < element_count; i++) {
    avro_value_t element;
    if ((rval = avro_value_get_by_index(value, i, &element, NULL)) != 0) {
      json_decref(result);
      return rval;
    }

    json_t *element_json = NULL;
    if ((rval = avro_value_to_json_t(&element, &element_json, 0, conf,
                                     cache)) != 0) {
      json_decref(result);
      return rval;
    }

    if ((rval = json_array_append_new(result, element_json)) != 0) {
      avro_set_error("Cannot append element to array");
      json_decref(element_json);
      json_decref(result);
      return rval;
    }
  }

  *json = result;
  return 0;
}

static int avro_map_to_json_t(const avro_value_t *value, json_t **json,
                              const config_t *conf, cache_t *cache) {
  int rval = 0;
  json_t *result = json_object();
  if (result == NULL) {
    avro_set_error("Cannot allocate JSON map");
    return ENOMEM;
  }

  size_t element_count;
  if ((rval = avro_value_get_size(value, &element_count)) != 0) {
    json_decref(result);
    return rval;
  }

  for (size_t i = 0; i < element_count; i++) {
    const char *key;
    avro_value_t element;

    if ((rval = avro_value_get_by_index(value, i, &element, &key)) != 0) {
      json_decref(result);
      return rval;
    }

    json_t *element_json = NULL;
    if ((rval = avro_value_to_json_t(&element, &element_json, 0, conf,
                                     cache)) != 0) {
      json_decref(result);
      return rval;
    }

    if ((rval = json_object_set_new_nocheck(result, key, element_json)) != 0) {
      avro_set_error("Cannot append element to map");
      json_decref(element_json);
      json_decref(result);
      return rval;
    }
  }

  *json = result;
  return 0;
}

static int avro_value_to_json_t(const avro_value_t *value, json_t **json,
                                int top_level, const config_t *conf,
                                cache_t *cache) {
  switch (avro_value_get_type(value)) {
  case AVRO_BOOLEAN: {
    int val;
    CHECKED_EV(avro_value_get_boolean(value, &val));
    CHECKED_ALLOC(*json, val ? json_true() : json_false());
    return 0;
  }

  case AVRO_BYTES: {
    const void *val;
    size_t size;
    CHECKED_EV(avro_value_get_bytes(value, &val, &size));
    CHECKED_EV(avro_bytes_value_to_json_t(value, json, val, size, conf, cache));
    return 0;
  }

  case AVRO_DOUBLE: {
    double val;
    CHECKED_EV(avro_value_get_double(value, &val));
    if (isinf(val)) {
      CHECKED_ALLOC(*json, json_string_nocheck("Infinity"));
      return 0;
    }
    if (isnan(val)) {
      CHECKED_ALLOC(*json, json_string_nocheck("NaN"));
      return 0;
    }
    CHECKED_ALLOC(*json, json_real(val));
    return 0;
  }

  case AVRO_FLOAT: {
    float val;
    CHECKED_EV(avro_value_get_float(value, &val));
    if (isinf(val)) {
      CHECKED_ALLOC(*json, json_string_nocheck("Infinity"));
      return 0;
    }
    if (isnan(val)) {
      CHECKED_ALLOC(*json, json_string_nocheck("NaN"));
      return 0;
    }
    CHECKED_ALLOC(*json, json_real(val));
    return 0;
  }

  case AVRO_INT32: {
    int32_t val;
    CHECKED_EV(avro_value_get_int(value, &val));

    avro_logical_schema_t *logical_type = NULL;
    if (conf->logical_types) {
      logical_type = avro_logical_schema(avro_value_get_schema(value));
    }

    if (logical_type != NULL) {
      if (logical_type->type == AVRO_DATE) {
        CHECKED_ALLOC(*json, json_string_nocheck(epoch_days_to_str(val)));
        return 0;
      }
      if (logical_type->type == AVRO_TIME_MILLIS) {
        CHECKED_ALLOC(*json, json_string_nocheck(time_millis_to_str(val)));
        return 0;
      }
      avro_set_error("INT type is annotated by an unsupported logical type");
      return EINVAL;
    }
    CHECKED_ALLOC(*json, json_integer(val));
    return 0;
  }

  case AVRO_INT64: {
    int64_t val;
    CHECKED_EV(avro_value_get_long(value, &val));

    avro_logical_schema_t *logical_type = NULL;
    if (conf->logical_types) {
      logical_type = avro_logical_schema(avro_value_get_schema(value));
    }

    if (logical_type != NULL) {
      if (logical_type->type == AVRO_TIME_MICROS) {
        CHECKED_ALLOC(*json, json_string_nocheck(time_micros_to_str(val)));
        return 0;
      }
      if (logical_type->type == AVRO_TIMESTAMP_MILLIS) {
        CHECKED_ALLOC(*json, json_string_nocheck(timestamp_millis_to_str(val)));
        return 0;
      }
      if (logical_type->type == AVRO_TIMESTAMP_MICROS) {
        CHECKED_ALLOC(*json, json_string_nocheck(timestamp_micros_to_str(val)));
        return 0;
      }
      avro_set_error("LONG type is annotated by an unsupported logical type");
      return EINVAL;
    }
    CHECKED_ALLOC(*json, json_integer(val));
    return 0;
  }

  case AVRO_NULL: {
    CHECKED_EV(avro_value_get_null(value));
    CHECKED_ALLOC(*json, json_null());
    return 0;
  }

  case AVRO_STRING: {
    const char *val;
    size_t size;
    CHECKED_EV(avro_value_get_string(value, &val, &size));
    CHECKED_ALLOC(*json, json_stringn(val, size - 1));
    return 0;
  }

  case AVRO_ARRAY: {
    CHECKED_EV(avro_array_to_json_t(value, json, conf, cache));
    return 0;
  }

  case AVRO_ENUM: {
    avro_schema_t enum_schema;
    int symbol_value;
    const char *symbol_name;

    CHECKED_EV(avro_value_get_enum(value, &symbol_value));
    enum_schema = avro_value_get_schema(value);
    symbol_name = avro_schema_enum_get(enum_schema, symbol_value);
    CHECKED_ALLOC(*json, json_string(symbol_name));
    return 0;
  }

  case AVRO_FIXED: {
    const void *val;
    size_t size;
    CHECKED_EV(avro_value_get_fixed(value, &val, &size));

    if (conf->ms_hadoop_logical_types && is_ms_hadoop_logical_type_guid(value, size)) {
        char guid_val[37]; // Guid is formatted as 36 characters (32 nibbles plus 4 hyphens): xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, and we need a null-terminator

        snprintf(guid_val, 37, GUID_FORMAT, GUID_ARG((char*)val));
        CHECKED_ALLOC(*json, json_stringn(guid_val, 36));
        return 0;
    }

    CHECKED_EV(avro_bytes_value_to_json_t(value, json, val, size, conf, cache));
    return 0;
  }

  case AVRO_MAP: {
    CHECKED_EV(avro_map_to_json_t(value, json, conf, cache));
    return 0;
  }

  case AVRO_RECORD: {
    int rval = 0;
    json_t *result;
    CHECKED_ALLOC(result, json_object());

    size_t field_count = conf->columns_size;
    int filter_cols = top_level && conf->columns_size > 0;

    if(!filter_cols) {
      // --columns was not provided, print all the fields then
      if ((rval = avro_value_get_size(value, &field_count)) != 0) {
        json_decref(result);
        return rval;
      }
    }

    for (size_t i = 0; i < field_count; i++) {
      size_t field_idx = i;
      if(filter_cols) {
        // --columns was provided, print requested field
        field_idx = conf->columns[i];
      }
      if ((rval = record_field_to_json(result, value, field_idx, conf, cache)) != 0) {
        json_decref(result);
        return rval;
      }
    }

    *json = result;
    return 0;
  }

  case AVRO_UNION: {
    avro_value_t branch;
    CHECKED_EV(avro_value_get_current_branch(value, &branch));
    CHECKED_EV(avro_value_to_json_t(&branch, json, top_level, conf, cache));
    return 0;
  }
  }
  return 0;
}

static int record_field_to_json(json_t *result, const avro_value_t *value,
                              size_t field_idx, const config_t *conf,
                              cache_t *cache) {
  int rval = 0;
  const char *field_name;
  avro_value_t field;

  if ((rval = avro_value_get_by_index(value, field_idx, &field, &field_name)) != 0) {
    return rval;
  }

  json_t *field_json = NULL;
  if ((rval = avro_value_to_json_t(&field, &field_json, 0, conf, cache)) != 0) {
    return rval;
  }

  if (conf->prune &&
      (json_is_null(field_json) ||
        (json_is_object(field_json) && !json_object_size(field_json)) ||
        (json_is_array(field_json) && !json_array_size(field_json)))) {
    json_decref(field_json);
    return rval;
  }

  if ((rval = json_object_set_new_nocheck(result, field_name, field_json)) != 0) {
    json_decref(field_json);
    return rval;
  }

  return rval;
}

static int avro_file_to_json(avro_file_reader_t reader, avro_schema_t wschema,
                             const config_t *conf) {
  avro_value_iface_t *iface = avro_generic_class_from_schema(wschema);
  avro_value_t value;
  avro_generic_value_new(iface, &value);
  cache_t *cache = cache_new();
  int rval = 0;
  while (avro_file_reader_read_value(reader, &value) == 0) {
    json_t *json = NULL;

    if ((rval = avro_value_to_json_t(&value, &json, 1, conf, cache)) != 0) {
      break;
    }
    rval = json_dumpf(json, stdout, JSON_ENCODE_FLAGS);
    json_decref(json);
    if (rval < 0) {
      break;
    }
    if (fputc('\n', stdout) < 0) {
      rval = ferror(stdout);
      break;
    }
    avro_value_reset(&value);
  }

  avro_value_decref(&value);
  avro_value_iface_decref(iface);
  cache_free(cache);
  return rval;
}

static int write_escape_quotes(FILE *dest, const char *str, size_t size) {
  for (int i = 0; i < size; ++i) {
    int ch = *(str + i);
    if (ch == '"') {
      if (fputc('"', dest) < 0) {
        return ferror(dest);
      }
    }
    if (fputc(ch, dest) < 0) {
      return ferror(dest);
    }
  }
  return 0;
}

static int write_escaped_str_to_csv(FILE *dest, const char *str, size_t size) {
  if (size > 0) {
    if (!memchr(str, '"', size) && !memchr(str, ',', size)) {
      if (fwrite(str, size, 1, dest) < 1) {
        return ferror(dest);
      }
    } else {
      if (fputc('"', dest) < 0) {
        return ferror(dest);
      }
      write_escape_quotes(dest, str, size);
      if (fputc('"', dest) < 0) {
        return ferror(dest);
      }
    }
  }
  return 0;
}

static int write_byte_array_to_csv(FILE *dest, const char *bytes, size_t size) {
  static int printedByteArrayTelemetry = 0;
  if(!printedByteArrayTelemetry++) {
    fprintf(stderr, "Byte array detected\n", avro_strerror());
  }
  CHECKED_PRINT(dest, "\"[");
  for (int i = 0; i < size; ++i) {
    CHECKED_PRINTF(dest, "%d", (unsigned char)bytes[i]);

    if(i != size - 1){
      if (fputc(',', dest) < 0) {
        return ferror(dest);
      }
    }
  }
  CHECKED_PRINT(dest, "]\"");
  return 0;
}

static int dump_to_csv(const char *buffer, size_t size, void *data) {
  return write_escape_quotes((FILE *)data, buffer, size);
}

int json_dump_to_csv(FILE *dest, const json_t *json, size_t flags) {
  if (fputc('"', dest) < 0) {
    return ferror(dest);
  }
  CHECKED_EV(json_dump_callback(json, dump_to_csv, (void *)dest, flags));
  if (fputc('"', dest) < 0) {
    return ferror(dest);
  }
  return 0;
}

static int avro_bytes_value_to_csv(FILE *dest, const avro_value_t *value,
                                   const void *bytes, size_t size,
                                   const config_t *conf, cache_t *cache) {
  avro_logical_schema_t *logical_type = NULL;
  if (conf->logical_types) {
    logical_type = avro_logical_schema(avro_value_get_schema(value));
  }

  if (logical_type != NULL) {
    if (logical_type->type != AVRO_DECIMAL) {
      avro_set_error("Unsupported logical type annotation in BYTES/FIXED type");
      return EINVAL;
    }

    decimal_from_bytes(cache->dec, (int8_t *)bytes, size, logical_type->scale);
    char *str;
    CHECKED_ALLOC(str,
                  decimal_to_str(cache->dec, &cache->str, &cache->str_size));
    CHECKED_PRINT(dest, str);
    return 0;
  }

  return write_byte_array_to_csv(dest, (const char *)bytes, size);
}

static int avro_value_to_csv(FILE *dest, const avro_value_t *value,
                             int top_level, const config_t *conf,
                             cache_t *cache) {
  switch (avro_value_get_type(value)) {
  case AVRO_BOOLEAN: {
    int val;
    CHECKED_EV(avro_value_get_boolean(value, &val));
    CHECKED_PRINT(dest, val ? "true" : "false");
    return 0;
  }

  case AVRO_BYTES: {
    const void *val;
    size_t size;
    CHECKED_EV(avro_value_get_bytes(value, &val, &size));
    return avro_bytes_value_to_csv(dest, value, val, size, conf, cache);
  }

  case AVRO_DOUBLE: {
    double val;
    CHECKED_EV(avro_value_get_double(value, &val));
    if (isinf(val)) {
      CHECKED_PRINT(dest, "Infinity");
      return 0;
    }
    if (isnan(val)) {
      CHECKED_PRINT(dest, "NaN");
      return 0;
    }
    CHECKED_PRINTF(dest, "%.17g", val);
    return 0;
  }

  case AVRO_FLOAT: {
    float val;
    CHECKED_EV(avro_value_get_float(value, &val));
    if (isinf(val)) {
      CHECKED_PRINT(dest, "Infinity");
      return 0;
    }
    if (isnan(val)) {
      CHECKED_PRINT(dest, "NaN");
      return 0;
    }
    CHECKED_PRINTF(dest, "%.17g", val);
    return 0;
  }

  case AVRO_INT32: {
    int32_t val;
    CHECKED_EV(avro_value_get_int(value, &val));

    avro_logical_schema_t *logical_type = NULL;
    if (conf->logical_types) {
      logical_type = avro_logical_schema(avro_value_get_schema(value));
    }

    if (logical_type != NULL) {
      if (logical_type->type == AVRO_DATE) {
        CHECKED_PRINT(dest, epoch_days_to_str(val));
        return 0;
      }
      if (logical_type->type == AVRO_TIME_MILLIS) {
        CHECKED_PRINT(dest, time_millis_to_str(val));
        return 0;
      }
      avro_set_error("INT type is annotated by an unsupported logical type");
      return EINVAL;
    }
    CHECKED_PRINTF(dest, "%" JSON_INTEGER_FORMAT, (long long int)val);
    return 0;
  }

  case AVRO_INT64: {
    int64_t val;
    CHECKED_EV(avro_value_get_long(value, &val));

    avro_logical_schema_t *logical_type = NULL;
    if (conf->logical_types) {
      logical_type = avro_logical_schema(avro_value_get_schema(value));
    }

    if (logical_type != NULL) {
      if (logical_type->type == AVRO_TIME_MICROS) {
        CHECKED_PRINT(dest, time_micros_to_str(val));
        return 0;
      }
      if (logical_type->type == AVRO_TIMESTAMP_MILLIS) {
        CHECKED_PRINT(dest, timestamp_millis_to_str(val));
        return 0;
      }
      if (logical_type->type == AVRO_TIMESTAMP_MICROS) {
        CHECKED_PRINT(dest, timestamp_micros_to_str(val));
        return 0;
      }
      avro_set_error("LONG type is annotated by an unsupported logical type");
      return EINVAL;
    }
    CHECKED_PRINTF(dest, "%" JSON_INTEGER_FORMAT, (long long int)val);
    return 0;
  }

  case AVRO_NULL: {
    CHECKED_EV(avro_value_get_null(value));
    return 0;
  }

  case AVRO_STRING: {
    const char *val;
    size_t size;
    CHECKED_EV(avro_value_get_string(value, &val, &size));
    return write_escaped_str_to_csv(dest, val, size - 1);
  }

  case AVRO_ARRAY: {
    json_t *result = NULL;
    CHECKED_EV(avro_array_to_json_t(value, &result, conf, cache));
    int rval = 0;
    if (!conf->prune || json_array_size(result)) {
      rval = json_dump_to_csv(dest, result, JSON_ENCODE_FLAGS);
    }
    json_decref(result);
    return rval;
  }

  case AVRO_ENUM: {
    avro_schema_t enum_schema;
    int symbol_value;
    const char *symbol_name;
    CHECKED_EV(avro_value_get_enum(value, &symbol_value));
    enum_schema = avro_value_get_schema(value);
    symbol_name = avro_schema_enum_get(enum_schema, symbol_value);
    CHECKED_PRINT(dest, symbol_name);
    return 0;
  }

  case AVRO_FIXED: {
    const void *val;
    size_t size;
    CHECKED_EV(avro_value_get_fixed(value, &val, &size));

    if (conf->ms_hadoop_logical_types && is_ms_hadoop_logical_type_guid(value, size)) {
        CHECKED_PRINTF(dest, GUID_FORMAT, GUID_ARG((char*)val));
        return 0;
    }
    return avro_bytes_value_to_csv(dest, value, val, size, conf, cache);
  }

  case AVRO_MAP: {
    json_t *result = NULL;
    CHECKED_EV(avro_map_to_json_t(value, &result, conf, cache));
    int rval = 0;
    if (!conf->prune || json_object_size(result)) {
      rval = json_dump_to_csv(dest, result, JSON_ENCODE_FLAGS);
    }
    json_decref(result);
    return rval;
  }

  case AVRO_RECORD: {
    if (top_level) {
      size_t field_count = conf->columns_size;
      int filter_cols = conf->columns_size > 0;

      if(!filter_cols) {
        // --columns was not provided, print all the fields then
        CHECKED_EV(avro_value_get_size(value, &field_count));
      }

      int print_comma = 0;

      for(size_t i = 0; i < field_count; i++) {
        size_t field_idx = i;
        if(filter_cols) {
          // --columns was provided, print requested field
          field_idx = conf->columns[i];
        }
        CHECKED_EV(record_field_to_csv(dest, value, field_idx, print_comma, conf, cache));
        print_comma = 1;
      }

      return 0;
    }

    json_t *record;
    CHECKED_EV(avro_value_to_json_t(value, &record, 0, conf, cache));
    int rval = 0;
    if (!conf->prune || json_object_size(record)) {
      rval = json_dump_to_csv(dest, record, JSON_ENCODE_FLAGS);
    }
    json_decref(record);
    return rval;
  }

  case AVRO_UNION: {
    avro_value_t branch;
    CHECKED_EV(avro_value_get_current_branch(value, &branch));
    return avro_value_to_csv(dest, &branch, top_level, conf, cache);
  }
  }
  return 0;
}

static int record_field_to_csv(FILE *dest, const avro_value_t *value,
                              size_t field_idx, int print_comma, const config_t *conf,
                              cache_t *cache) {
  const char *field_name;
  avro_value_t field;

  if (print_comma) {
    if (fputc(',', dest) < 0) {
      return ferror(dest);
    }
  }
  CHECKED_EV(avro_value_get_by_index(value, field_idx, &field, &field_name));
  CHECKED_EV(avro_value_to_csv(dest, &field, 0, conf, cache));
  return 0;
}

static int avro_file_to_csv(avro_file_reader_t reader, avro_schema_t wschema,
                            const config_t *conf) {
  avro_value_iface_t *iface = avro_generic_class_from_schema(wschema);
  avro_value_t value;
  avro_generic_value_new(iface, &value);
  cache_t *cache = cache_new();

  int rval = 0;
  while (avro_file_reader_read_value(reader, &value) == 0) {
    if ((rval = avro_value_to_csv(stdout, &value, 1, conf, cache)) != 0) {
      break;
    }
    if (fputc('\n', stdout) < 0) {
      rval = ferror(stdout);
      break;
    }
    avro_value_reset(&value);
  }

  avro_value_decref(&value);
  avro_value_iface_decref(iface);
  cache_free(cache);
  return rval;
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

    json_t *obj;
    CHECKED_ALLOC(obj, json_object());
    json_object_set_new(obj, "name", json_string(val.field->name));

    int found_logical_type = 0;
    if (logical_type != NULL) {
      switch (logical_type->type) {
      case AVRO_DECIMAL:
        json_object_set_new(obj, "type", json_string_nocheck("decimal"));
        found_logical_type = 1;
        break;
      case AVRO_DATE:
      case AVRO_TIME_MILLIS:
      case AVRO_TIME_MICROS:
      case AVRO_TIMESTAMP_MILLIS:
      case AVRO_TIMESTAMP_MICROS:
        json_object_set_new(obj, "type", json_string_nocheck("datetime"));
        found_logical_type = 1;
        break;
      case AVRO_DURATION:
        json_object_set_new(obj, "type", json_string_nocheck("timespan"));
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
        json_object_set_new(obj, "type", json_string_nocheck("string"));
        break;
      case AVRO_INT32:
        json_object_set_new(obj, "type", json_string_nocheck("int"));
        break;
      case AVRO_INT64:
        json_object_set_new(obj, "type", json_string_nocheck("long"));
        break;
      case AVRO_FLOAT:
      case AVRO_DOUBLE:
        json_object_set_new(obj, "type", json_string_nocheck("real"));
        break;
      case AVRO_BOOLEAN:
        json_object_set_new(obj, "type", json_string_nocheck("bool"));
        break;
      case AVRO_BYTES:
      default:
        json_object_set_new(obj, "type", json_string_nocheck("dynamic"));
        break;
      }
    }
    json_array_append_new(result, obj);
  }

  int rval = json_dumpf(result, stdout, JSON_ENCODE_FLAGS);
  json_decref(result);
  return rval;
}

static int process_file(const char *filename, const config_t *conf) {
  avro_file_reader_t reader;
  if (avro_file_reader(filename, &reader)) {
    fprintf(stderr, "Error opening file '%s': %s\n", filename, avro_strerror());
    exit(1);
  }

  avro_schema_t wschema = avro_file_reader_get_writer_schema(reader);

  int rval;
  if (conf->show_schema) {
    rval = print_schema(wschema);
  } else if (conf->output_csv) {
    rval = avro_file_to_csv(reader, wschema, conf);
  } else {
    rval = avro_file_to_json(reader, wschema, conf);
  }
  avro_schema_decref(wschema);
  avro_file_reader_close(reader);
  return rval;
}

static void print_usage(const char *exe) {
  fprintf(stderr,
          "Usage: %s [OPTIONS] FILE\n"
          "\n"
          "Where options are:\n"
          " --show-schema                 Only show Avro file schema, and exit\n"
          " --prune                       Omit null values as well as empty lists and objects\n"
          " --logical-types               Convert logical types automatically\n"
          " --csv                         Produce output in CSV format\n"
          " --ms-hadoop-logical-types     Convert non-standard logical types of Microsoft.Hadoop.Avro (System.Guid) automatically\n"
          " --columns 1,2,...             Only output specified columns numbers\n",
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
    *c++ = col_idx-1;
    col = strtok_r(NULL, ",", &saveptr);
  } while (col != NULL);

  *columns = cols_indices;
  *columns_size = cols_num;
  return 0;
}

static const char *parse_args(int argc, char **argv, config_t *conf) {
  int arg_idx;
  for (arg_idx = 1; arg_idx < argc - 1; ++arg_idx) {
    if (!strcmp(argv[arg_idx], "--prune")) {
      conf->prune = 1;
    } else if (!strcmp(argv[arg_idx], "--logical-types")) {
      conf->logical_types = 1;
    } else if (!strcmp(argv[arg_idx], "--ms-hadoop-logical-types")) {
      conf->ms_hadoop_logical_types = 1;
    } else if (!strcmp(argv[arg_idx], "--show-schema")) {
      conf->show_schema = 1;
    } else if (!strcmp(argv[arg_idx], "--csv")) {
      conf->output_csv = 1;
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

#if defined(_WIN32)
/*
 * Allocation interface.  You can provide a custom allocator for the
 * library, should you wish.  The allocator is provided as a single
 * generic function, which can emulate the standard malloc, realloc, and
 * free functions.  The design of this allocation interface is inspired
 * by the implementation of the Lua interpreter.
 *
 * The ptr parameter will be the location of any existing memory
 * buffer.  The osize parameter will be the size of this existing
 * buffer.  If ptr is NULL, then osize will be 0.  The nsize parameter
 * will be the size of the new buffer, or 0 if the new buffer should be
 * freed.
 *
 * If nsize is 0, then the allocation function must return NULL.  If
 * nsize is not 0, then it should return NULL if the allocation fails.
 */
static void *
custom_jemalloc_allocator(void *ud, void *ptr, size_t osize, size_t nsize)
{
  // Unused params
  (void)ud;
  (void)osize;

  if (nsize == 0) {
    je_free(ptr);
    return NULL;
  } else {
    return je_realloc(ptr, nsize);
  }
}
#endif

int main(int argc, char **argv) {

#if defined(_WIN32)
/* Currently provided only for Windows, since it was tested only on Windows. */
  avro_set_allocator(custom_jemalloc_allocator, NULL);
#endif

  config_t conf = {.prune = 0,
                   .logical_types = 0,
                   .show_schema = 0,
                   .output_csv = 0,
                   .columns = NULL,
                   .columns_size = 0};

  const char *file = parse_args(argc, argv, &conf);
  int rval = process_file(file, &conf);
  if (conf.columns) {
    free(conf.columns);
  }
  return rval;
}
