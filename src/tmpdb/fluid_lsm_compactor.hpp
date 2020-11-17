#ifndef FLUID_LSM_COMPACTOR_H_
#define FLUID_LSM_COMPACTOR_H_

#include <cmath>
#include <set>
#include <mutex>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"

#include "spdlog/spdlog.h"
#include "tmpdb/fluid_options.hpp"

namespace tmpdb
{

class FluidCompactor;

typedef struct CompactionTask
{
rocksdb::DB *db;
FluidCompactor *compactor;
const std::string &column_family_name;
std::vector<std::string> input_file_names;
int output_level;
rocksdb::CompactionOptions compact_options;
size_t origin_level_id;
bool retry_on_fail;
bool is_a_retry;
std::mutex &compactions_left_mutex;
int &compactions_left_count;

/**
 * @brief Construct a new Compaction Task object
 * 
 * @param db
 * @param compactor
 * @param column_family_name
 * @param input_file_names
 * @param output_level
 * @param compact_options
 * @param origin_level_id
 * @param retry_on_fail
 * @param is_a_retry
 */
CompactionTask(
    rocksdb::DB * db, FluidCompactor* compactor,
    const std::string& column_family_name,
    const std::vector<std::string>& input_file_names,
    const int output_level,
    const rocksdb::CompactionOptions& compact_options,
    const size_t origin_level_id,
    bool retry_on_fail,
    bool is_a_retry,
    std::mutex &compactions_left_mutex,
    int &compactions_left_count)
        : db(db),
        compactor(compactor),
        column_family_name(column_family_name),
        input_file_names(input_file_names),
        output_level(output_level),
        compact_options(compact_options),
        origin_level_id(origin_level_id),
        retry_on_fail(retry_on_fail),
        is_a_retry(is_a_retry),
        compactions_left_mutex(compactions_left_mutex),
        compactions_left_count(compactions_left_count) {}
} CompactionTask;


/**
 * @brief Container to represent one single run within the FluidLSM Tree
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
    std::vector<FluidLevel> levels;

    FluidCompactor(const FluidOptions fluid_opt, const rocksdb::Options rocksdb_opt);

    /**
     * @brief Maps RocksDB files to classic LSM levels
     * 
     * @param db An open database
     */
    void init_open_db(rocksdb::DB * db);

    /** 
     * @brief Picks and returns a compaction task given the specified DB and column family.
     * It is the caller's responsibility to destroy the returned CompactionTask.
     *
     * @param db An open database
     * @param cf_name Names of the column families
     * @param level Target level id
     *
     * @returns CompactionTask Will return a "nullptr" if it cannot find a proper compaction task.
     */
    virtual CompactionTask * PickCompaction(rocksdb::DB * db, const std::string & cf_name, const size_t level) = 0;

    /**
     * @brief Schedule and run the specified compaction task in background.
     * 
     * @param task 
     */
    virtual void ScheduleCompaction(CompactionTask * task) = 0;

private:
    /**
     * @brief Maps a run of RocksDB to the FluidLSM
     * 
     * @param db 
     * @param file_names 
     * @param fluid_level 
     * @param rocksdb_level 
     */
    void add_run(rocksdb::DB * db, const std::vector<rocksdb::SstFileMetaData> & file_names, size_t fluid_level, size_t rocksdb_level);
};


class FluidLSMCompactor : public FluidCompactor
{
public:
    std::mutex compactions_left_mutex;
    int compactions_left_count = 0;
    int compactions_failed_count = 0;

    FluidLSMCompactor(const FluidOptions fluid_opt, const rocksdb::Options rocksdb_opt)
        : FluidCompactor(fluid_opt, rocksdb_opt) {};

    size_t largest_occupied_level() const;

    CompactionTask * PickCompaction(rocksdb::DB * db, const std::string & cf_name, const size_t level) override;

    void OnFlushCompleted(rocksdb::DB * db, const ROCKSDB_NAMESPACE::FlushJobInfo & info) override;

    static void CompactFiles(void * arg);

    void ScheduleCompaction(CompactionTask * task) override;

    /**
     * @brief Estimates the number of levels needed based on
     * 
     * @param N Total number of entries
     * @param T Size ratio
     * @param E Entry size
     * @param B Buffer size
     * @return size_t Number of levels
     */
    static size_t estimate_levels(size_t N, double T, size_t E, size_t B);

    /**
     * @brief Adds all file names eligible for compaction to file_names
     * 
     * @param level_id Index of the level
     * @param file_names Empty list to be filled with names of compacted files
     * @return size_t Target level for compaction
     */
    size_t add_files_to_compaction(size_t level_id, std::vector<std::string> & file_names);

    size_t fluid_level_to_rocksdb_start_idx(size_t fluid_level);
};


} /* namespace tmpdb */

#endif /* FLUID_LSM_COMPACTOR_H_ */