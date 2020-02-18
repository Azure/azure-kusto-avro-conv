#include <avro.h>
#include <errno.h>
#include <jansson.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  int prune;
} config;

/*
 * Converts a binary buffer into a NUL-terminated JSON UTF-8 string.
 * Avro bytes and fixed values are encoded in JSON as a string, and JSON
 * strings must be in UTF-8.  For these Avro types, the JSON string is
 * restricted to the characters U+0000..U+00FF, which corresponds to the
 * ISO-8859-1 character set.  This function performs this conversion.
 * The resulting string must be freed using avro_free when you're done
 * with it.
 */
static int encode_utf8_bytes(const void *src, size_t src_len, void **dest,
                             size_t *dest_len) {

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

  // Allocate a new buffer for the UTF-8 string and fill it in.
  uint8_t *dest8 = (uint8_t *)avro_malloc(utf8_len);
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
    if (isnan(val)) {                                                          \
      return_json("string", json_string("Infinity"));                          \
    }                                                                          \
    if (isinf(val)) {                                                          \
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

static json_t *avro_value_to_json_t(const avro_value_t *value,
                                    const config *conf) {
  switch (avro_value_get_type(value)) {
  case AVRO_BOOLEAN: {
    int val;
    check_return(NULL, avro_value_get_boolean(value, &val));
    return_json("boolean", val ? json_true() : json_false());
  }

  case AVRO_BYTES: {
    const void *val;
    size_t size;
    void *encoded = NULL;
    size_t encoded_size = 0;

    check_return(NULL, avro_value_get_bytes(value, &val, &size));

    if (encode_utf8_bytes(val, size, &encoded, &encoded_size)) {
      return NULL;
    }

    json_t *result = json_stringn((const char *)encoded, encoded_size - 1);
    avro_free(encoded, encoded_size);
    if (result == NULL) {
      avro_set_error("Cannot allocate JSON bytes");
    }
    return result;
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
    return_json("int", json_integer(val));
  }

  case AVRO_INT64: {
    int64_t val;
    check_return(NULL, avro_value_get_long(value, &val));
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

      json_t *element_json = avro_value_to_json_t(&element, conf);
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
    void *encoded = NULL;
    size_t encoded_size = 0;

    check_return(NULL, avro_value_get_fixed(value, &val, &size));

    if (encode_utf8_bytes(val, size, &encoded, &encoded_size)) {
      return NULL;
    }

    json_t *result = json_stringn((const char *)encoded, encoded_size - 1);
    avro_free(encoded, encoded_size);
    if (result == NULL) {
      avro_set_error("Cannot allocate JSON fixed");
    }
    return result;
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

      json_t *element_json = avro_value_to_json_t(&element, conf);
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

    for (i = 0; i < field_count; i++) {
      const char *field_name;
      avro_value_t field;

      rc = avro_value_get_by_index(value, i, &field, &field_name);
      if (rc != 0) {
        json_decref(result);
        return NULL;
      }

      json_t *field_json = avro_value_to_json_t(&field, conf);
      if (field_json == NULL) {
        json_decref(result);
        return NULL;
      }

      if (conf->prune &&
          (json_is_null(field_json) ||
           json_is_object(field_json) && !json_object_size(field_json) ||
           json_is_array(field_json) && !json_array_size(field_json))) {
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
    return avro_value_to_json_t(&branch, conf);
  }

  default:
    return NULL;
  }
}

static int avro_to_json(const avro_value_t *value, char **json_str,
                        const config *conf) {
  json_t *json = avro_value_to_json_t(value, conf);
  if (json == NULL) {
    return ENOMEM;
  }

  *json_str =
      json_dumps(json, JSON_ENCODE_ANY | JSON_COMPACT | JSON_ENSURE_ASCII);
  json_decref(json);
  return 0;
}

static void process_file(const char *filename, const config *conf) {
  avro_file_reader_t reader;
  if (avro_file_reader(filename, &reader)) {
    fprintf(stderr, "Error opening file '%s': %s\n", filename, avro_strerror());
    exit(1);
  }

  avro_schema_t wschema = avro_file_reader_get_writer_schema(reader);
  avro_value_iface_t *iface = avro_generic_class_from_schema(wschema);

  avro_value_t value;
  avro_generic_value_new(iface, &value);

  int rval;
  while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
    char *json;

    if (avro_to_json(&value, &json, conf)) {
      fprintf(stderr, "Error converting value to JSON: %s\n", avro_strerror());
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

  if (rval != EOF) {
    fprintf(stderr, "Error reading Avro file: %s\n", avro_strerror());
  }

  avro_file_reader_close(reader);
  avro_value_decref(&value);
  avro_value_iface_decref(iface);
  avro_schema_decref(wschema);
}

static void print_usage(const char *exe) {
  fprintf(stderr,
          "Usage: %s [OPTIONS] FILE\n"
          "\n"
          "Where options are:\n"
          " --prune     Omit null values as well as empty lists and objects\n",
          exe);
  exit(1);
}

void main(int argc, char **argv) {
  config conf = {.prune = 0};

  int arg_idx;
  for (arg_idx = 1; arg_idx < argc - 1; ++arg_idx) {
    if (!strcmp(argv[arg_idx], "--prune")) {
      conf.prune = 1;
    } else {
      print_usage(argv[0]);
    }
  }
  if (arg_idx != argc - 1) {
    print_usage(argv[0]);
  }

  process_file(argv[arg_idx], &conf);
  exit(0);
}
