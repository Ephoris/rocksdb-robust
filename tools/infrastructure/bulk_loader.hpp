#ifndef BULK_LOADER_H_ 
#define BULK_LOADER_H_ 

#include <iostream>

#include "data_generator.hpp"

#include "common/debug.hpp"
#include "tmpdb/fluid_lsm_compactor.hpp"
#include "rocksdb/db.h"

#define BATCH_SIZE 1000

class FluidLSMBulkLoader : public tmpdb::FluidCompactor
{
    FluidLSMBulkLoader(DataGenerator & data_gen, const tmpdb::FluidOptions fluid_opt, const rocksdb::Options rocksdb_opt)
        : FluidCompactor(fluid_opt, rocksdb_opt), data_gen(&data_gen) {};

    rocksdb::Status bulk_load_entries(rocksdb::DB * db, size_t num_entries);
    // rocksdb::Status bulk_load_levels(rocksdb::DB * db, size_t levels);

    // Override both compaction events to prevent any compactions during bulk loading
    void OnFlushCompleted(rocksdb::DB * /* db */, const ROCKSDB_NAMESPACE::FlushJobInfo & /* info */) override {};
    void ScheduleCompaction(tmpdb::CompactionTask * /* task */) override {};


private:
    DataGenerator * data_gen;

    void bulk_load(rocksdb::DB * db, std::vector<size_t> entries_per_level, size_t num_levels);

    void bulk_load_single_level(rocksdb::DB * db, size_t level_idx, size_t num_entries, size_t num_runs);

    void bulk_load_single_run(rocksdb::DB * db, size_t level_idx, size_t num_entries);
};

#endif /*  BULK_LOADER_H_ */