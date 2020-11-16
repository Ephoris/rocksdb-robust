#include "tmpdb/fluid_lsm_compactor.hpp"

using namespace tmpdb;


bool FluidRun::contains(std::string file_name)
{
    return this->file_names.count(file_name) == 1;
}


bool FluidRun::add_file(rocksdb::SstFileMetaData file)
{
    // TODO: Any error checking?
    files.push_back(file);
    file_names.insert(file.name);

    return true;
}


size_t FluidLevel::size() const
{
    size_t num_runs = 0;
    for (auto & run : runs)
    {
        num_runs += (run.files.size() > 0);
    }

    return num_runs;
}


size_t FluidLevel::size_in_bytes() const
{
    size_t bytes = 0;
    for (auto & run : runs)
    {
        for (auto & file : run.files)
        {
            bytes += file.size;
        }
    }

    return bytes;
}


size_t FluidLevel::num_live_runs()
{
    size_t num_live_runs = 0;
    for (auto & run : runs)
    {
        bool being_compacted = false;
        for (auto & file : run.files)
        {
            being_compacted |= file.being_compacted;
        }
        num_live_runs += (!being_compacted) && (!run.files.empty());
    }

    return num_live_runs;
}


bool FluidLevel::contains(std::string file_name)
{
    for (auto & run : runs)
    {
        if (run.contains(file_name))
        {
            return true;
        }
    }
    return false;
}


FluidCompactor::FluidCompactor(const FluidOptions fluid_opt, const rocksdb::Options rocksdb_opt)
    : fluid_opt(fluid_opt), rocksdb_opt(rocksdb_opt), rocksdb_compact_opt()
{
    this->rocksdb_compact_opt.compression = this->rocksdb_opt.compression;
    this->rocksdb_compact_opt.output_file_size_limit = this->rocksdb_opt.target_file_size_base;

    this->levels.resize(this->rocksdb_opt.num_levels);
}


void FluidCompactor::init_open_db(rocksdb::DB * db)
{
    spdlog::debug("Initializing open database: {}", db->GetName());

    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);

    for (size_t idx = 0; idx < this->levels.size(); idx++)
    {
        this->levels[idx].runs.clear();
    }

    size_t num_runs_in_fluid_level_1 = cf_meta.levels[0].files.size() + !cf_meta.levels[1].files.empty();

    std::vector<rocksdb::SstFileMetaData> file_names;
    for (size_t idx = num_runs_in_fluid_level_1; idx < this->fluid_opt.lower_level_run_max; idx++)
    {
        this->add_run(db, file_names, 0, 0);
    }

    for (auto file : cf_meta.levels[0].files)
    {
        file_names.push_back(file);
        this->add_run(db, file_names, 0, 0);
        spdlog::trace("Added file {} from fluid level {} -> {} rocksdb level", file.name, 0, 0);
        file_names.clear();
    }

    for (auto file : cf_meta.levels[1].files)
    {
        file_names.push_back(file);
        spdlog::trace("Adding file {} from fluid level {} -> {} rocksdb level", file.name, 0, 1);
    }
    this->add_run(db, file_names, 0, 1);
    file_names.clear();

    double K = this->fluid_opt.lower_level_run_max;
    for (size_t idx = 2; idx < cf_meta.levels.size(); idx++)
    {
        rocksdb::LevelMetaData current_level = cf_meta.levels[idx];
        size_t fluid_level = std::ceil(((double) current_level.level - 1) / (K + 1));

        file_names.clear();
        for (auto file : current_level.files)
        {
            file_names.push_back(file);
            spdlog::trace("Adding file {} from fluid level {} -> {} rocksdb level", file.name, fluid_level, current_level.level);
        }
        this->add_run(db, file_names, fluid_level, current_level.level);
    }
}


void FluidCompactor::add_run(
    rocksdb::DB * db,
    std::vector<rocksdb::SstFileMetaData> const & file_names,
    size_t fluid_level,
    size_t rocksdb_level)
{
    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);

    FluidRun run(rocksdb_level);
    for (rocksdb::SstFileMetaData file : file_names)
    {
        run.add_file(file);
    }
    this->levels[fluid_level].add_run(run);
}


size_t FluidLSMCompactor::largest_occupied_level() const
{
    size_t largest_level = 0;
    for (size_t idx = 0; idx < this->levels.size(); idx++)
    {
        for (auto & run : this->levels[idx].runs)
        {
            // We're returning the last level that is occupied
            // TODO: Optimize by just starting from the end and finidng the first occupied
            if (run.files.size() <= 0) { continue; }
            largest_level = idx;
        }
    }

    return largest_level;
}


size_t FluidLSMCompactor::add_files_to_compaction(size_t level_idx, std::vector<std::string> & file_names)
{
    spdlog::trace("Adding files to compact at level {}", level_idx);
    size_t compaction_size_bytes = 0;
    for(auto & run : this->levels[level_idx].runs)
    {
        for (auto & file : run.files)
        {
            if (file.being_compacted) { continue; }

            spdlog::trace("Adding file {} to compact", file.name);
            file_names.push_back(file.name);
            compaction_size_bytes += file.size;
        }
    }

    size_t target_level = level_idx;
    double T = this->fluid_opt.size_ratio;
    size_t current_level_capacticty = this->rocksdb_opt.write_buffer_size * std::pow(T, level_idx + 1) * (T - 1) / T;
    if (compaction_size_bytes > current_level_capacticty)
    {
        target_level += 1;
    }

    return target_level;
}


CompactionTask * FluidLSMCompactor::PickCompaction(rocksdb::DB * db, const std::string & cf_name, const size_t level_id)
{
    spdlog::trace("Picking compaciton at level {}", 0);
    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);

    std::vector<std::string> input_file_names;

    size_t target_level = this->add_files_to_compaction(level_id, input_file_names);

    return new CompactionTask(
        db, this, cf_name, input_file_names,
        target_level, this->rocksdb_compact_opt, level_id, false);
}


void FluidLSMCompactor::OnFlushCompleted(rocksdb::DB * db, const ROCKSDB_NAMESPACE::FlushJobInfo & info)
{
    spdlog::trace("Running flush complete subroutine");
    size_t largest_level = this->largest_occupied_level();
    size_t level_idx;
    spdlog::trace("Largest level {}", largest_level);
    for (size_t level = 0; level < largest_level; level++)
    {
        level_idx = level - 1;
        size_t runs = this->levels[level_idx].num_live_runs();
        bool valid_lower_levels = (level_idx < largest_level && runs > this->fluid_opt.lower_level_run_max);
        bool valid_last_level = (level_idx == largest_level && runs > this->fluid_opt.largest_level_run_max);

        if (!valid_lower_levels & !valid_last_level) { continue; }
        spdlog::trace("Picking compaction");
        CompactionTask * task = PickCompaction(db, info.cf_name, level_idx);

        if (!task) { continue; }

        spdlog::trace("task compact level {} -> {}", task->origin_level_id, task->output_level);
        for (auto name : task->input_file_names)
        {
            spdlog::trace("Compacting file {}", name);
        }

        task->retry_on_fail = info.triggered_writes_stop;
        // Schedule compaction in a different thread.
        ScheduleCompaction(task);
    }
}


void FluidLSMCompactor::CompactFiles(void * arg)
{
    std::unique_ptr<CompactionTask> task(reinterpret_cast<CompactionTask *>(arg));
    assert(task);
    assert(task->db);
    assert(task->output_level > (int) task->origin_level_id);

    std::vector<std::string> * output_file_names = new std::vector<std::string>();
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

        // spdlog::warn("CompactFile() did not finish, rescheduling.");
        CompactionTask * new_task = new CompactionTask(
            task->db,
            task->compactor,
            task->column_family_name,
            task->input_file_names,
            task->output_level,
            task->compact_options,
            task->origin_level_id,
            task->retry_on_fail
        );
        task->compactor->ScheduleCompaction(new_task);
    }
}


void FluidLSMCompactor::ScheduleCompaction(CompactionTask * task)
{
    this->rocksdb_opt.env->Schedule(& FluidLSMCompactor::CompactFiles, task);
}


size_t FluidLSMCompactor::estimate_levels(size_t N, double T, size_t E, size_t B)
{
    if ((N * E) < B)
    {
        spdlog::warn("Number of entries (N = {}) fits in the in-memory buffer, defaulting to 1 level", N);
        return 1;
    }

    size_t estimated_levels = std::ceil(std::log(((N * E) / B) + T) / std::log(T));

    return estimated_levels;
}


size_t FluidLSMCompactor::fluid_level_to_rocksdb_start_idx(size_t fluid_level)
{
    if (fluid_level == 1)
    {
        return 0;
    }

    // Note that we set slots per level to be K + 1 to keep an empty space 
    size_t slots_per_level = this->fluid_opt.lower_level_run_max + 1;

    return (slots_per_level * (fluid_level - 2)) + 1;
}