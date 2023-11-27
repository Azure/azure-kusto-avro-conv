#!/bin/sh -eu

tmpfile=$(mktemp)
trap "rm -f $tmpfile" EXIT

run_test() {
  tfile="$1.avro"
  efile="$2.json"
  cfile="$2.csv"
  shift; shift
  set +e
  options="$@"

  echo "Running: ./avro2json $options ../tests/${tfile}"
  ./avro2json $options "../tests/${tfile}" > $tmpfile
  if ! diff -a $tmpfile "../tests/${efile}"; then
    exit 1
  fi

  if [ -f ../tests/$cfile ]; then
    ./avro2json $options --csv "../tests/${tfile}" > $tmpfile
    if ! diff -a $tmpfile "../tests/${cfile}"; then
      exit 1
    fi
  fi
}

run_test file1 file1
run_test file1 file1-p --prune
run_test reals reals
run_test decimals decimals
run_test decimals decimals-l --logical-types
run_test decimals-bytes decimals-bytes
run_test decimals-bytes decimals-bytes-l --logical-types
run_test columns columns-1 --columns "[\"a\"]"
run_test columns columns-2 --columns "[\"b\"]"
run_test columns columns-3 --columns "[\"a\",\"d\"]"
run_test columns columns-3-non-existing-columns --columns "[\"non-existing-column1\",\"a\",\"non-existing-column2\",\"d\",\"another-non-existing-column\"]"
run_test columns columns-3-non-existing-column --columns "[\"non-existing-column1\"]"
run_test columns columns-4 --columns "[\"d\",\"a\"]"
run_test dates dates-l --logical-types
run_test datetimes datetimes-l --logical-types
run_test escaping escaping
run_test datetimes-from-unix datetimes-from-unix --columns "[[\"UnixSeconds\",\"ts-s\"],[\"UnixMilliseconds\",\"ts-ms\"],[\"UnixNanoseconds\",\"ts-ns\"]]"
