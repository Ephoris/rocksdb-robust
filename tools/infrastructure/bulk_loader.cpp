#include "bulk_loader.hpp"


rocksdb::Status FluidLSMBulkLoader::bulk_load_entries(rocksdb::DB *db, size_t num_entries)
{
    spdlog::info("Bulk loading DB with {} entries", num_entries);
    rocksdb::Status status;

    size_t E = this->fluid_opt.entry_size;
    size_t B = this->fluid_opt.buffer_size;
    double T = this->fluid_opt.size_ratio;
    size_t estimated_levels = tmpdb::FluidLSMCompactor::estimate_levels(num_entries, T, E, B);
    spdlog::debug("Estimated levels: {}", estimated_levels);

    size_t entries_in_buffer = (B / E);
    spdlog::debug("Number of entries that can fit in the buffer: {}", entries_in_buffer);

    std::vector<size_t> capacity_per_level(estimated_levels);
    capacity_per_level[0] = (entries_in_buffer) * (T - 1);
    for (size_t level_idx = 1; level_idx < estimated_levels; level_idx++)
    {
        capacity_per_level[level_idx] = capacity_per_level[level_idx - 1] * T;
    }

    if (spdlog::get_level() <= spdlog::level::debug)
    {
        std::string capacity_str = "";
        for (auto & capacity : capacity_per_level)
        {
            capacity_str += std::to_string(capacity) + ", ";
        }
        capacity_str = capacity_str.substr(0, capacity_str.size() - 2);
        spdlog::debug("Capcaity per level : [{}]", capacity_str);
    }

    status = this->bulk_load(db, capacity_per_level, estimated_levels, num_entries);

    return status;
}


rocksdb::Status FluidLSMBulkLoader::bulk_load_levels(rocksdb::DB *db, size_t num_levels)
{
    spdlog::info("Bulk loading DB with {} levels", num_levels);
    rocksdb::Status status;

    size_t entries_in_buffer = (this->fluid_opt.buffer_size / this->fluid_opt.entry_size);
    spdlog::debug("Number of entries that can fit in the buffer: {}", entries_in_buffer);

    std::vector<size_t> capacity_per_level(num_levels);
    capacity_per_level[0] = entries_in_buffer;
    for (size_t level_idx = 1; level_idx < num_levels; level_idx++)
    {
        capacity_per_level[level_idx] = capacity_per_level[level_idx - 1] * this->fluid_opt.size_ratio;
    }

    if (spdlog::get_level() <= spdlog::level::debug)
    {
        std::string capacity_str = "";
        for (auto & capacity : capacity_per_level)
        {
            capacity_str += std::to_string(capacity) + ", ";
        }
        capacity_str = capacity_str.substr(0, capacity_str.size() - 2);
        spdlog::debug("Capcaity per level : [{}]", capacity_str);
    }

    status = this->bulk_load(db, capacity_per_level, num_levels, INT_MAX);

    return status;
}


rocksdb::Status FluidLSMBulkLoader::bulk_load(
    rocksdb::DB *db,
    std::vector<size_t> capacity_per_level,
    size_t num_levels,
    size_t max_entries)
{
    rocksdb::Status status;
    size_t level_idx;
    size_t num_runs;
    size_t num_entries_loaded = 0;

    // Fill up levels starting rom the BOTTOM
    for (size_t level = num_levels; level > 0; level--)
    {
        level_idx = level - 1;
        if (capacity_per_level[level_idx] == 0) { continue; }
        spdlog::debug("Bulk loading level {} with {} entries.", level, capacity_per_level[level_idx]);

        if (level_idx == 0)
        {
            num_runs = this->fluid_opt.size_ratio - 1;
        }
        else if (level == num_levels) //> Last level has Z max runs
        {
            num_runs = this->fluid_opt.largest_level_run_max;
        }
        else //> Every other level inbetween has K max runs
        {
            num_runs = this->fluid_opt.lower_level_run_max;
        }

        status = this->bulk_load_single_level(db, level_idx, capacity_per_level[level_idx], num_runs);
        num_entries_loaded += capacity_per_level[level_idx];
        if (num_entries_loaded > max_entries)
        {
            spdlog::debug("Already reached max entries, stopping bulk loading.");
            break;
        }
    }

    return status;
}


rocksdb::Status FluidLSMBulkLoader::bulk_load_single_level(
    rocksdb::DB *db,
    size_t level_idx,
    size_t capacity_per_level,
    size_t num_runs)
{
    rocksdb::Status status;
    size_t entries_per_run = capacity_per_level / num_runs;
    size_t level = level_idx + 1;

    for (size_t run_idx = 0; run_idx < num_runs; run_idx++)
    {
        spdlog::trace("Loading run {} at level {} with {} entries (file size ~ {} MB)",
            run_idx, level, entries_per_run, (entries_per_run * this->fluid_opt.entry_size) >> 20);

        status = this->bulk_load_single_run(db, level_idx, entries_per_run);
    }

    // Level 1 (IDX = 0) items do not need to be forced down to any level, leave it as is
    if (level == 1) { return status; }

    // Force all runs in this level to be mapped to their respective level
    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);

    std::vector<std::string> file_names;
    for (auto & file : cf_meta.levels[0].files)
    {
        if (file.being_compacted) { continue; }
        file_names.push_back(file.name);
    }

    // We add an extra 3% to size per output file size in order to compensate for meta-data
    this->rocksdb_compact_opt.output_file_size_limit = (uint64_t) (entries_per_run * this->fluid_opt.entry_size * 1.03);
    spdlog::trace("File size limit : ~ {} MB", this->rocksdb_compact_opt.output_file_size_limit >> 20);
    tmpdb::CompactionTask *task = new tmpdb::CompactionTask(
        db, this, "default", file_names,
        level_idx, this->rocksdb_compact_opt, 0, true,
        false, this->compactions_left_mutex, this->compactions_left_count);
    this->ScheduleCompaction(task);

    return status;
}


rocksdb::Status FluidLSMBulkLoader::bulk_load_single_run(rocksdb::DB *db, size_t level_idx, size_t num_entries)
{
    rocksdb::WriteOptions write_opt;
    write_opt.sync = false;
    write_opt.disableWAL = true;
    write_opt.no_slowdown = false;
    write_opt.low_pri = true; // every insert is less important than compaction

    size_t buffer_size = this->fluid_opt.entry_size * num_entries * 8;
    rocksdb::Status status = db->SetOptions({{"write_buffer_size", std::to_string(buffer_size)}});

    std::string value_prefix = std::to_string(level_idx) + "|";
    std::string key_prefix = value_prefix;

    size_t batch_size = std::min((size_t) BATCH_SIZE, num_entries);
    for (size_t entry_num = 0; entry_num < num_entries; entry_num += batch_size)
    {
        rocksdb::WriteBatch batch;
        for (int i = 0; i < (int) batch_size; i++)
        {
            std::pair<std::string, std::string> key_value =
                this->data_gen->generate_kv_pair(this->fluid_opt.entry_size, key_prefix, value_prefix);
            batch.Put(key_value.first, key_value.second);
        }
        status = db->Write(write_opt, &batch);
        if (!status.ok())
        {
            spdlog::error("{}", status.ToString());
        }
    }

    rocksdb::FlushOptions flush_opt;
    flush_opt.wait = true;
    db->Flush(flush_opt);

    return status;
}
