#include <stdio.h>

#include "kernel.h"

/* #include <glib.h> */
/* #include <arrow-glib/arrow-glib.h> */
/* #include "arrow/c/abi.h" */

typedef struct iter_data {
  int lim;
  int cur;
} iter_data;

const void* next(void* data) {
  iter_data *id = (iter_data*)data;
  if (id->cur >= id->lim) {
    return 0;
  } else {
    id->cur++;
    return &id->cur;
  }
}

void release(void* data) {
  printf("released\n");
}

void test_iter() {
  iter_data it;
  it.lim = 10;
  it.cur = 0;

  struct EngineIterator eit = {
    .data = &it,
    .get_next = &next,
  };
  iterate(&eit);
}

int main(int argc, char* argv[]) {

  if (argc < 2) {
    printf("Usage: %s [table_path]\n", argv[0]);
    return -1;
  }

  char* table_path = argv[1];
  printf("Opening table at %s\n", table_path);
  DefaultTable *table = get_table_with_default_client(table_path);
  DefaultSnapshot *ss = snapshot(table);
  uint64_t v = version(ss);
  printf("Got version: %lu\n", v);

  struct FileList fl = get_scan_files(ss, NULL);
  printf("Need to read %i files\n", fl.file_count);
  for (int i = 0;i < fl.file_count;i++) {
    printf("file %i -> %s\n", i, fl.files[i]);
  }

  test_iter();
  return 0;
}
