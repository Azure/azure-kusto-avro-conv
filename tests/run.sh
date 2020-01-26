#!/bin/sh -eu

tmpfile=$(mktemp)
trap "rm -f $tmpfile" EXIT

run_test() {
  testfile=$1
  set +e

  ./avro2json "../tests/${testfile}.avro" > $tmpfile
  if ! diff $tmpfile "../tests/${testfile}.json"; then
    exit 1
  fi

  ./avro2json --prune "../tests/${testfile}.avro" > $tmpfile
  if ! diff $tmpfile "../tests/${testfile}-prune.json"; then
    exit 1
  fi
}

for t in file1; do
  run_test $t
done
