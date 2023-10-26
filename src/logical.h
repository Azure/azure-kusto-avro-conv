#pragma once

#include <gmp.h>
#include <stdint.h>
#include <string.h>
#include <stdint.h>

typedef struct {
  mpz_t unscaled; // unscaled decimal (without a '.')
  size_t scale;   // scale ('.' position)
  int negative;
} decimal_t;

decimal_t *decimal_new();
void decimal_free(decimal_t *decimal);

/**
 * Converts bytes in big-endian order to decimal_t.
 * The original bytes array is updated in-place when needed.
 */
void decimal_from_bytes(decimal_t *value, int8_t *bytes_be, size_t size,
                        size_t scale);

/**
 * Renders decimal as string using provided buffer of specified size.
 * Buffer is re-allocated to the needed size if it's small or when it's NULL.
 */
char *decimal_to_str(decimal_t *value, char **buf, size_t *buf_size);

/**
 * Converts days since Unix epoch time (1970-01-01) to string representation in
 * format 'yyyy-mm-dd'.
 */
char *epoch_days_to_str(int32_t days);

/**
 * Converts milliseconds into string of format "HH:MM:SS.SSS"
 */
char *time_millis_to_str(int32_t millis);

/**
 * Converts microseconds into string of format "HH:MM:SS.SSSSSS"
 */
char *time_micros_to_str(int64_t micros);

/**
 * Converts milliseconds since unix epoch time into string of format
 * "yyyy-mm-dd HH:MM:SS.SSS"
 */
char *timestamp_millis_to_str(int64_t millis);

/**
 * Converts microseconds since unix epoch time into string of format
 * "yyyy-mm-dd HH:MM:SS.SSSSSS"
 */
char *timestamp_micros_to_str(int64_t micros);

/**
 * Converts nanoseconds since Unix epoch time (1970-01-01) to string representation in ISO 8601
 * format 'yyyy-mm-ddThh::mm:ss.0000000Z'.
 */
char *epoch_nanos_to_utc_str(int64_t nanos_since_epoch);