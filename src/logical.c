#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "logical.h"

#ifndef max
#define max(a, b) (((a) > (b)) ? (a) : (b))
#endif

decimal_t *decimal_new() {
  decimal_t *value = (decimal_t *)malloc(sizeof(decimal_t));
  if (!value) {
    return NULL;
  }
  mpz_init(value->unscaled);
  value->scale = 0;
  value->negative = 0;
  return value;
}

void decimal_free(decimal_t *value) {
  mpz_clear(value->unscaled);
  free(value);
}

void decimal_from_bytes(decimal_t *value, int8_t *bytes_be, size_t size,
                        size_t scale) {

  if (bytes_be[0] < 0) {
    value->negative = 1;
    // convert to positive complement's two binary representation
    for (int i = 0; i < size; ++i) {
      bytes_be[i] = ~bytes_be[i]; // negate all bits
    }
    for (size_t i = size - 1; ++bytes_be[i] == 0; i--) // add a 'one'
      ;
  } else {
    value->negative = 0;
  }

  value->scale = scale;
  mpz_import(value->unscaled, size, 1 /* big endian */, sizeof(int8_t), 0, 0,
             bytes_be);
}

char *decimal_to_str(decimal_t *value, char **buf, size_t *buf_size) {
  const int base = 10;
  size_t scale = value->scale;

  size_t required_size =
      max(scale,
          mpz_sizeinbase(value->unscaled, base)) // see mpz_get_str() docs
      + 3;                                       // '-' + '.' + '\0'

  if (*buf == NULL || *buf_size < required_size) {
    *buf = (char *)realloc(*buf, required_size);
    if (*buf == NULL) {
      return NULL;
    }
    *buf_size = required_size;
  }
  char *num = *buf;

  num = mpz_get_str(num, base, value->unscaled);

  // return zero as is
  if (*num == '0') {
    return num;
  }

  size_t len = strlen(num);

  // add leading zeroes from the beginnig if needed
  if (scale > len) {
    size_t zeroes_to_add = scale - len + 1;
    // shift right to free space for leading zeroes
    char *p = num + len;
    for (; p >= num; --p) {
      *(p + zeroes_to_add) = *p;
    }
    // place zeroes at the beginning
    for (size_t i = 0; i < zeroes_to_add; ++i) {
      *(num + i) = '0';
    }
    len += zeroes_to_add;
  }

  // insert the '.'
  if (scale > 0) {
    // shift right whatever comes after '.'
    char *p = num + len;
    char *dot_at = num + len - scale;
    for (; p >= dot_at; --p) {
      *(p + 1) = *p;
    }
    *dot_at = '.';
    len += 1;

    // strip trailing zeros, and update the length:
    char *e = num + len - 1;
    for (; len > 0; --len, --e) {
      if (*e == '0') {
        continue;
      }
      if (*e == '.') {
        --len;
      }
      break;
    }
    *(num + len) = '\0';
  }

  // add '-' sign if it's negative number:
  if (value->negative) {
    char *p = num + len;
    for (; p >= num; --p) {
      *(p + 1) = *p;
    }
    *num = '-';
  }

  return num;
}

#define MIN_DATE "1970-01-01"
#define MAX_DATE "3000-12-31"
#define MIN_DATETIME_MILLIS "1970-01-01 00:00:00.000"
#define MAX_DATETIME_MILLIS "3000-12-31 00:00:00.000"
#define MIN_DATETIME_MICROS "1970-01-01 00:00:00.000000"
#define MAX_DATETIME_MICROS "3000-12-31 00:00:00.000000"
#define MIN_DATETIME_UTC "1970-01-01T00:00:00.0000000Z"
#define MAX_DATETIME_UTC "3000-12-31T00:00:00.0000000Z"
#define TIME_MILLIS_EMPTY "00:00:00.000"
#define TIME_MICROS_EMPTY "00:00:00.000000"
#define MILLIS_IN_SEC 1000UL
#define MILLIS_IN_MIN MILLIS_IN_SEC * 60
#define MILLIS_IN_HOUR MILLIS_IN_MIN * 60
#define MICROS_IN_SEC 1000000UL
#define MICROS_IN_MIN MICROS_IN_SEC * 60
#define MICROS_IN_HOUR MICROS_IN_MIN * 60
#define NANOS_IN_SEC 1000000000UL

char *epoch_days_to_str(int32_t days) {
  static char buf[sizeof(MIN_DATE) + 1];

  struct tm dt = {0};
  dt.tm_year = 70;
  dt.tm_mon = 0;
  dt.tm_mday = 1;
  dt.tm_mday += days;

  if (mktime(&dt) == -1) {
    if (days < 0) {
      return MIN_DATE;
    }
    return MAX_DATE;
  }

  strftime(buf, sizeof(buf), "%Y-%m-%d", &dt);
  return buf;
}

char *time_millis_to_str(int32_t millis) {
  static char buf[sizeof(TIME_MILLIS_EMPTY) + 1];

  if (millis <= 0) {
    return TIME_MILLIS_EMPTY;
  }

  int32_t hours = millis / MILLIS_IN_HOUR;
  if (hours > 99) {
    return TIME_MILLIS_EMPTY;
  }

  int32_t rem = millis % MILLIS_IN_HOUR;
  int32_t minutes = rem / MILLIS_IN_MIN;
  rem = rem % MILLIS_IN_MIN;
  int32_t secs = rem / MILLIS_IN_SEC;
  rem = rem % MILLIS_IN_SEC;

  snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d", hours, minutes, secs, rem);
  return buf;
}

char *time_micros_to_str(int64_t micros) {
  static char buf[sizeof(TIME_MICROS_EMPTY) + 1];

  if (micros <= 0) {
    return TIME_MICROS_EMPTY;
  }

  int64_t hours = micros / MICROS_IN_HOUR;
  if (hours > 99) {
    return TIME_MICROS_EMPTY;
  }

  int64_t rem = micros % MICROS_IN_HOUR;
  int64_t minutes = rem / MICROS_IN_MIN;
  rem = rem % MICROS_IN_MIN;
  int64_t secs = rem / MICROS_IN_SEC;
  rem = rem % MICROS_IN_SEC;

  snprintf(buf, sizeof(buf),
           "%02" PRId64 ":%02" PRId64 ":%02" PRId64 ".%06" PRId64, hours,
           minutes, secs, rem);
  return buf;
}

char *timestamp_millis_to_str(int64_t millis) {
  static char buf[sizeof(MIN_DATETIME_MILLIS) + 1];

  struct tm dt = {0};
  dt.tm_year = 70;
  dt.tm_mon = 0;
  dt.tm_mday = 1;
  dt.tm_sec = (int32_t)(millis / MILLIS_IN_SEC);

  if (mktime(&dt) == -1) {
    if (millis < 0) {
      return MIN_DATETIME_MILLIS;
    }
    return MAX_DATETIME_MILLIS;
  }

  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &dt);

  int32_t rem = millis % MILLIS_IN_SEC;
  char *p = buf + sizeof(MIN_DATETIME_MILLIS) - 5;
  snprintf(p, 5, ".%03d", rem);
  return buf;
}

char *timestamp_micros_to_str(int64_t micros) {
  static char buf[sizeof(MIN_DATETIME_MICROS) + 1];

  struct tm dt = {0};
  dt.tm_year = 70;
  dt.tm_mon = 0;
  dt.tm_mday = 1;
  dt.tm_sec = (int32_t)(micros / MICROS_IN_SEC);

  if (mktime(&dt) == -1) {
    if (micros < 0) {
      return MIN_DATETIME_MICROS;
    }
    return MAX_DATETIME_MICROS;
  }

  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &dt);

  int32_t rem = micros % MICROS_IN_SEC;
  char *p = buf + sizeof(MIN_DATETIME_MICROS) - 8;
  snprintf(p, 8, ".%06d", rem);
  return buf;
}

char *epoch_nanos_to_utc_str(int64_t nanos_since_epoch) {
    static char buf[45];

    struct tm dt = {0};
    dt.tm_year = 70;
    dt.tm_mon = 0;
    dt.tm_mday = 1;
    dt.tm_sec = (int32_t)(nanos_since_epoch / NANOS_IN_SEC);
    dt.tm_isdst = 1; // Daylight Saving Time

    if (mktime(&dt) == -1) {
        if (nanos_since_epoch < 0) {
          return MIN_DATETIME_UTC;
        }
      return MAX_DATETIME_UTC;
    }

    int32_t nanos = (nanos_since_epoch % NANOS_IN_SEC) / 100; // bring the nanos to 7 digits length

    snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%07dZ",
             1900 + dt.tm_year, dt.tm_mon + 1, dt.tm_mday, dt.tm_hour, dt.tm_min, dt.tm_sec, nanos);

    return buf;
}
