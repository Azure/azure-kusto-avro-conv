#include <avro.h>
#include <iostream>

static void process_file(const char *filename) {
  avro_file_reader_t reader;
  if (avro_file_reader(filename, &reader)) {
    std::cerr << "Error opening file '" << filename << "': " << avro_strerror()
              << std::endl;
    exit(1);
  }

  avro_schema_t wschema = avro_file_reader_get_writer_schema(reader);
  avro_value_iface_t *iface = avro_generic_class_from_schema(wschema);

  avro_value_t value;
  avro_generic_value_new(iface, &value);

  int rval;
  while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
    char *json;

    if (avro_value_to_json(&value, 1, &json)) {
      std::cerr << "Error converting value to JSON: " << avro_strerror()
                << std::endl;
    } else {
      std::cout << json << std::endl;
      free(json);
    }

    avro_value_reset(&value);
  }

  if (rval != EOF) {
    std::cerr << "Error reading Avro file: " << avro_strerror() << std::endl;
  }

  avro_file_reader_close(reader);
  avro_value_decref(&value);
  avro_value_iface_decref(iface);
  avro_schema_decref(wschema);
}

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "USAGE: " << argv[0] << " <input.avro>" << std::endl;
    exit(1);
  }

  process_file(argv[1]);
}
