#ifndef BULK_LOADER_H_ 
#define BULK_LOADER_H_ 

#include <iostream>

#include "common/debug.hpp"
#include "rocksdb/db.h"

rocksdb::Status bulk_load_random
(
    std::string db_path
);

#endif /*  BULK_LOADER_H_ */