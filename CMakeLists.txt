project(azure-kust-avro-conv)
cmake_minimum_required(VERSION 3.12)

find_path(AVRO_INCLUDE_DIR avro.h)
find_library(AVRO_LIBRARY avro)
find_library(JANSSON_LIBRARY jansson)
find_library(LZMA_LIBRARY lzma)
find_library(ZLIB_LIBRARY zlib)
find_library(SNAPPY_LIBRARY snappy)
if (WIN32)
find_library(JEMALLOC_LIBRARY jemalloc PATHS "${VCPKG_INSTALLED_DIR}/x64-windows-release/lib")
else (WIN32)
set(JEMALLOC_LIBRARY)
endif (WIN32)

if (WIN32)
  find_library(GMP_LIBRARY mpir)
else (WIN32)
  find_library(GMP_LIBRARY gmp)
endif (WIN32)

set(MATH_LIBRARY)
if(NOT MSVC)
    set(MATH_LIBRARY m)
endif(NOT MSVC)

add_executable(avro2json
  src/avro2json.c
  src/logical.c)

if (WIN32)
  set(ADDITIONAL_INCLUDE_DIRS include/windows;${VCPKG_INSTALLED_DIR}/x64-windows-release/include/jemalloc)
else (WIN32)
  set(ADDITIONAL_INCLUDE_DIRS)
endif (WIN32)

target_include_directories(avro2json
  PRIVATE ${AVRO_INCLUDE_DIR}
  PRIVATE ${ADDITIONAL_INCLUDE_DIRS}
)

target_link_libraries(avro2json
  ${AVRO_LIBRARY}
  ${JEMALLOC_LIBRARY}
  ${JANSSON_LIBRARY}
  ${LZMA_LIBRARY}
  ${ZLIB_LIBRARY}
  ${SNAPPY_LIBRARY}
  ${GMP_LIBRARY}
  ${MATH_LIBRARY}
)
