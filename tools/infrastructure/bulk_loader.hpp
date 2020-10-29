#ifndef BULK_LOADER_H_ 
#define BULK_LOADER_H_ 

#include <iostream>

#include "common/debug.hpp"
#include "rocksdb/db.h"

#define BATCH_SIZE 1000

void load_batch(
    rocksdb::WriteBatch & batch,
    size_t batch_size, size_t entry_size,
    std::string value_prefix,
    std::string key_prefix);

rocksdb::Status bulk_load_random(
    std::string db_path,
    size_t entry_size,
    size_t total_entries);

#endif /*  BULK_LOADER_H_ */