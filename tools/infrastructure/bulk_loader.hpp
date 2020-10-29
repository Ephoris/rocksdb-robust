#ifndef BULK_LOADER_H_ 
#define BULK_LOADER_H_ 

#include <iostream>

#include "data_generator.hpp"

#include "common/debug.hpp"
#include "tmpdb/fluid_lsm_compactor.hpp"
#include "rocksdb/db.h"

#define BATCH_SIZE 1000

class LSMBulkLoader : public tmpdb::FluidCompactor
{
    LSMBulkLoader(DataGenerator & data_gen, const tmpdb::FluidOptions fluid_opt, const rocksdb::Options rocksdb_opt)
        : FluidCompactor(fluid_opt, rocksdb_opt), data_gen(&data_gen) {};

    // rocksdb::Status bulk_load_entries(rocksdb::DB * db, size_t num_entries);
    // rocksdb::Status bulk_load_levels(rocksdb::DB * db, size_t levels);

    // Override both compaction events to prevent any compactions during bulk loading
    void OnFlushCompleted(rocksdb::DB * /* db */, const ROCKSDB_NAMESPACE::FlushJobInfo & /* info */) override {};
    void ScheduleCompaction(tmpdb::CompactionTask * /* task */) override {};


private:
    DataGenerator * data_gen;
};

#endif /*  BULK_LOADER_H_ */