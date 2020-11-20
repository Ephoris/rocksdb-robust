#include "tmpdb/fluid_lsm_compactor.hpp"

using namespace tmpdb;


FluidCompactor::FluidCompactor(const FluidOptions fluid_opt, const rocksdb::Options rocksdb_opt)
    : fluid_opt(fluid_opt), rocksdb_opt(rocksdb_opt), rocksdb_compact_opt()
{
    this->rocksdb_compact_opt.compression = this->rocksdb_opt.compression;
    this->rocksdb_compact_opt.output_file_size_limit = this->rocksdb_opt.target_file_size_base;
}


int FluidLSMCompactor::largest_occupied_level(rocksdb::DB *db) const
{
    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);
    int largest_level_idx = 0;

    for (size_t level_idx = cf_meta.levels.size() - 1; level_idx > 0; level_idx--)
    {
        if (cf_meta.levels[level_idx].files.empty()) {continue;}
        largest_level_idx = level_idx;
        break;
    }
    
    if (largest_level_idx == 0)
    {
        if (cf_meta.levels[0].files.empty())
        {
            spdlog::error("Database is empty, exiting");
            exit(EXIT_FAILURE);
        }
    }

    return largest_level_idx;
}


CompactionTask *FluidLSMCompactor::PickCompaction(rocksdb::DB *db, const std::string &cf_name, const size_t level_idx)
{
    int T = this->fluid_opt.size_ratio;

    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);

    std::vector<std::string> input_file_names;

    for (auto &file : cf_meta.levels[level_idx].files)
    {
        if (file.being_compacted) {continue;}
        input_file_names.push_back(file.name);
    }
    size_t level_capacity = (T - 1) * std::pow(T, level_idx + 1) * (this->fluid_opt.buffer_size);
    if ((int) level_idx == this->largest_occupied_level(db) - 1) //> Last level we restrict number of runs to Z
    {
        this->rocksdb_compact_opt.output_file_size_limit = level_capacity / this->fluid_opt.largest_level_run_max;
    }
    else
    {
        this->rocksdb_compact_opt.output_file_size_limit = level_capacity / this->fluid_opt.lower_level_run_max;
    }

    return new CompactionTask(
        db, this, cf_name, input_file_names, level_idx + 1, this->rocksdb_compact_opt, level_idx, true, false);
}


void FluidLSMCompactor::OnFlushCompleted(rocksdb::DB *db, const ROCKSDB_NAMESPACE::FlushJobInfo &info)
{
    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);

    int largest_level_idx = this->largest_occupied_level(db);
    int live_runs;

    for (int level_idx = 0; level_idx < largest_level_idx; level_idx++)
    {
        live_runs = 0;
        for (auto &run : cf_meta.levels[level_idx].files)
        {
            if (run.being_compacted) {continue;}
            live_runs++;
        }
        bool level_1_needs_compact = ((level_idx == 0) && (live_runs > this->fluid_opt.size_ratio - 1));
        bool lower_levels_need_compact = ((level_idx < largest_level_idx) && (live_runs > this->fluid_opt.lower_level_run_max));
        bool last_levels_need_compact = ((level_idx == largest_level_idx) && (live_runs > this->fluid_opt.largest_level_run_max));

        if (!level_1_needs_compact && !lower_levels_need_compact && !last_levels_need_compact) {continue;}

        CompactionTask *task = PickCompaction(db, info.cf_name, level_idx);

        if (!task) {continue;}

        task->retry_on_fail = info.triggered_writes_stop;
        ScheduleCompaction(task);
    }
}


void FluidLSMCompactor::CompactFiles(void *arg)
{
    std::unique_ptr<CompactionTask> task(reinterpret_cast<CompactionTask *>(arg));
    assert(task);
    assert(task->db);
    assert(task->output_level > (int) task->origin_level_id);

    std::vector<std::string> *output_file_names = new std::vector<std::string>();
    rocksdb::Status s = task->db->CompactFiles(
        task->compact_options,
        task->input_file_names,
        task->output_level,
        -1,
        output_file_names
    );

    if (!s.ok() && !s.IsIOError() && task->retry_on_fail)
    {
        // If a compaction task with its retry_on_fail=true failed,
        // try to schedule another compaction in case the reason
        // is not an IO error.

        // spdlog::warn("CompactFile {} -> {} with {} files did not finish: {}",
        //     task->origin_level_id + 1,
        //     task->output_level + 1,
        //     task->input_file_names.size(),
        //     s.ToString());
        CompactionTask *new_task = new CompactionTask(
            task->db,
            task->compactor,
            task->column_family_name,
            task->input_file_names,
            task->output_level,
            task->compact_options,
            task->origin_level_id,
            task->retry_on_fail,
            true
        );
        task->compactor->ScheduleCompaction(new_task);

        return;
    }

    ((FluidLSMCompactor *) task->compactor)->compactions_left_mutex.lock();
    ((FluidLSMCompactor *) task->compactor)->compactions_left_count--;
    ((FluidLSMCompactor *) task->compactor)->compactions_left_mutex.unlock();
    spdlog::trace("CompactFiles level {} -> {} finished with status : {}", task->origin_level_id + 1, task->output_level + 1, s.ToString());

    return;
}


void FluidLSMCompactor::ScheduleCompaction(CompactionTask *task)
{
    if (!task->is_a_retry)
    {
        this->compactions_left_mutex.lock();
        this->compactions_left_count++;
        this->compactions_left_mutex.unlock();
    }
    this->rocksdb_opt.env->Schedule(&FluidLSMCompactor::CompactFiles, task);

    return;
}


size_t FluidLSMCompactor::estimate_levels(size_t N, double T, size_t E, size_t B)
{
    if ((N * E) < B)
    {
        spdlog::warn("Number of entries (N = {}) fits in the in-memory buffer, defaulting to 1 level", N);
        return 1;
    }

    size_t estimated_levels = std::ceil(std::log((N * E / B) + 1) / std::log(T));

    return estimated_levels;
}
