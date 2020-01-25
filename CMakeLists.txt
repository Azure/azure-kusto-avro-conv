project(azure-kust-avro-conv)
cmake_minimum_required(VERSION 3.12)

find_path(AVRO_INCLUDE_DIR avro.h)
find_library(AVRO_LIBRARY avro)
find_library(JANSSON_LIBRARY jansson)
find_library(LZMA_LIBRARY lzma)
find_library(ZLIB_LIBRARY zlib)
find_library(SNAPPY_LIBRARY snappy)

add_executable(avro2json
  src/avro2json.c)

target_include_directories(avro2json
  PRIVATE ${AVRO_INCLUDE_DIR})

target_link_libraries(avro2json
  ${AVRO_LIBRARY}
  ${JANSSON_LIBRARY}
  ${LZMA_LIBRARY}
  ${ZLIB_LIBRARY}
  ${SNAPPY_LIBRARY})
