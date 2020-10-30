#ifndef FLUID_LSM_COMPACTOR_H_
#define FLUID_LSM_COMPACTOR_H_

#include <cmath>
#include <set>
#include <mutex>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"

#include "tmpdb/fluid_options.hpp"
#include "common/debug.hpp"

namespace tmpdb
{

class FluidCompactor;

typedef struct CompactionTask
{
rocksdb::DB * db;
FluidCompactor * compactor;
const std::string & column_family_name;
std::vector<std::string> input_file_names;
int output_level;
rocksdb::CompactionOptions compact_options;
size_t origin_level_id;
bool retry_on_fail;

/**
 * @brief Construct a new Compaction Task object
 * 
 * @param _db
 * @param _compactor
 * @param _column_family_name
 * @param _input_file_names
 * @param _output_level
 * @param _compact_options
 * @param _origin_level_id
 * @param _retry_on_fail
 */
CompactionTask(
    rocksdb::DB * _db, FluidCompactor* _compactor,
    const std::string& _column_family_name,
    const std::vector<std::string>& _input_file_names,
    const int _output_level,
    const rocksdb::CompactionOptions& _compact_options,
    const size_t _origin_level_id,
    bool _retry_on_fail)
        : db(_db),
        compactor(_compactor),
        column_family_name(_column_family_name),
        input_file_names(_input_file_names),
        output_level(_output_level),
        compact_options(_compact_options),
        origin_level_id(_origin_level_id),
        retry_on_fail(_retry_on_fail) {}
} CompactionTask;


/**
 * @brief Container to represent one single run within the FluidLSM Tree
 * 
 */
class FluidRun
{
public:
    std::vector<rocksdb::SstFileMetaData> files;
    std::set<std::string> file_names;
    size_t rocksdb_level;

    FluidRun(size_t rocksdb_level) : files(), rocksdb_level(rocksdb_level) {};

    bool contains(std::string file_name);

    bool add_file(rocksdb::SstFileMetaData file);
};


/**
 * @brief Container to represent one single level within the FluidLSMTree
 * 
 */
class FluidLevel
{
public:
    std::vector<FluidRun> runs;

    FluidLevel() {};

    size_t size() const;

    size_t size_in_bytes() const;

    size_t num_live_runs();

    bool contains(std::string file_name);

    void add_run(FluidRun run) {runs.push_back(run);}
};


/**
 * @brief Abstract class, provides the infrastructure for a Fluid LSM Tree Compactor
 * 
 */
class FluidCompactor : public ROCKSDB_NAMESPACE::EventListener
{
public:
    FluidOptions fluid_opt;
    rocksdb::Options rocksdb_opt;
    rocksdb::CompactionOptions rocksdb_compact_opt;

    std::mutex level_mutex;
    std::vector<FluidLevel> levels;

    FluidCompactor(const FluidOptions fluid_opt, const rocksdb::Options rocksdb_opt);

    void init_open_db(rocksdb::DB * db);
    /** 
     * Picks and returns a compaction task given the specified DB and column family.
     * It is the caller's responsibility to destroy the returned CompactionTask.
     * 
     * Returns "nullptr" if it cannot find a proper compaction task.
     */
    virtual CompactionTask * PickCompaction(rocksdb::DB * db, const std::string & cf_name, const size_t level) = 0;

    // Schedule and run the specified compaction task in background.
    virtual void ScheduleCompaction(CompactionTask * task) = 0;

private:
    void add_run(rocksdb::DB * db, const std::vector<rocksdb::SstFileMetaData> & file_names, size_t fluid_level, size_t rocksdb_level);
};


class FluidLSMCompactor : public FluidCompactor
{
public:
    FluidLSMCompactor(const FluidOptions fluid_opt, const rocksdb::Options rocksdb_opt)
        : FluidCompactor(fluid_opt, rocksdb_opt) {};

    size_t largest_occupied_level() const;

    CompactionTask * PickCompaction(rocksdb::DB * db, const std::string & cf_name, const size_t level) override;

    void OnFlushCompleted(rocksdb::DB * db, const ROCKSDB_NAMESPACE::FlushJobInfo & info) override;

    static void CompactFiles(void * arg);

    void ScheduleCompaction(CompactionTask * task) override;

    /** Estimates the number of levels needed based on
     * 
     * N : Number of keys
     * T : size ratio
     * E : entry size
     * B : buffer size
     */
    static size_t estimate_levels(size_t N, double T, size_t E, size_t B);
private:
    size_t add_files_to_compaction(size_t level_id, std::vector<std::string> & file_names);
};


} /* namespace tmpdb */

#endif /* FLUID_LSM_COMPACTOR_H_ */