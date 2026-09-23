#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "schema.h"

int main(void)
{
  const KernelStringSlice inputs[] = { { "", 0 }, { "plain", 5 }, { "a\0b", 3 } };
  for (size_t i = 0; i < sizeof(inputs) / sizeof(inputs[0]); i++) {
    KernelStringSlice copy = copy_annotation_slice(inputs[i]);
    assert(copy.len == inputs[i].len);
    assert(memcmp(copy.ptr, inputs[i].ptr, copy.len) == 0);
    free((void*)copy.ptr);
  }
  return 0;
}
