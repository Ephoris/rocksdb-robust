#include "bulk_loader.hpp"


rocksdb::Status FluidLSMBulkLoader::bulk_load_entries(rocksdb::DB * db, size_t num_entries)
{
    spdlog::info("Bulk loading DB with {} entries", num_entries);
    rocksdb::Status status;

    size_t E = this->fluid_opt.entry_size;
    size_t B = this->fluid_opt.buffer_size;
    double T = this->fluid_opt.size_ratio;
    // double K = this->fluid_opt.lower_level_run_max;
    // double Z = this->fluid_opt.largest_level_run_max;
    size_t estimated_levels = tmpdb::FluidLSMCompactor::estimate_levels(num_entries, T, E, B);
    spdlog::debug("Estimated levels: {}", estimated_levels);

    size_t entries_in_buffer = (B / E);
    spdlog::debug("Number of entries that can fit in the buffer: {}", entries_in_buffer);

    std::vector<size_t> entries_per_level_per_run(estimated_levels);

    // size_t current_level_size = (B / E) * std::pow((T / K), estimated_levels - 2) * (T / Z);
    size_t current_level_size = (num_entries * (T - 1) / T) / (this->fluid_opt.lower_level_run_max);
    spdlog::debug("Lowest level ({}) size: {}", estimated_levels, current_level_size);

    size_t level_idx;
    for (size_t level = estimated_levels; level > 0; level--)
    {
        level_idx = level - 1;
        entries_per_level_per_run[level_idx] = current_level_size;
        current_level_size /= T;

        if (level_idx == 0 && entries_per_level_per_run[level_idx] < entries_in_buffer)
        {
            spdlog::info("Top level entries per level per run is smaller than possible entries in buffer ({} < {}). Changing to {}.",
                entries_per_level_per_run[level_idx], entries_in_buffer, entries_in_buffer);
            entries_per_level_per_run[level_idx] = entries_in_buffer;
        }
    }
    status = this->bulk_load(db, entries_per_level_per_run, estimated_levels);

    if (spdlog::get_level() <= spdlog::level::debug)
    {
        std::string entries_per_level_str = "";
        for (auto & entries : entries_per_level_per_run)
        {
            entries_per_level_str += std::to_string((int) (entries * this->fluid_opt.lower_level_run_max)) + ", ";
        }
        entries_per_level_str = entries_per_level_str.substr(0, entries_per_level_str.size() - 2);
        spdlog::debug("Entries per level per run: [{}]", entries_per_level_str);
    }

    return status;
}


rocksdb::Status FluidLSMBulkLoader::bulk_load(rocksdb::DB * db, std::vector<size_t> entries_per_level_per_run, size_t num_levels)
{
    rocksdb::Status status;
    size_t level_idx;
    for (size_t level = num_levels; level > 0; level--)
    {
        level_idx = level - 1;
        if (entries_per_level_per_run[level_idx] == 0) { continue; }
        spdlog::debug("Bulk loading level {} with {} entries",
            level_idx, entries_per_level_per_run[level_idx] * (int) this->fluid_opt.lower_level_run_max);
        status = this->bulk_load_single_level(db, level_idx,
                                              entries_per_level_per_run[level_idx],
                                              this->fluid_opt.lower_level_run_max);
    }

    return status;
}


rocksdb::Status FluidLSMBulkLoader::bulk_load_single_level(
    rocksdb::DB *db,
    size_t level_idx,
    size_t num_entries_per_run,
    size_t num_runs)
{
    rocksdb::Status status;
    for (size_t run_idx = 0; run_idx < num_runs; run_idx++)
    {
        spdlog::trace("Loading run {} at level {} with {} entries (file size ~ {} KB)",
            run_idx, level_idx, num_entries_per_run, (num_entries_per_run * this->fluid_opt.entry_size) >> 10);
        status = this->bulk_load_single_run(db, level_idx, num_entries_per_run);
    }

    return status;
}


rocksdb::Status FluidLSMBulkLoader::bulk_load_single_run(rocksdb::DB * db, size_t level_idx, size_t num_entries)
{
    rocksdb::WriteOptions write_opt;
    write_opt.sync = false;
    write_opt.disableWAL = true;
    write_opt.no_slowdown = false;
    // write_opt.low_pri = true; // every insert is less important than compaction

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
            std::pair<std::string, std::string> key_value = this->data_gen->generate_kv_pair(this->fluid_opt.entry_size, key_prefix, value_prefix);
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

    if (level_idx == 0)
    {
        return status;
    }

    // Force all runs in this level to be mapped to their respective level
    spdlog::trace("Force compacting run to level idx {}", level_idx);
    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);
    std::vector<std::string> file_names;
    for (auto & file : cf_meta.levels[0].files)
    {
        if (file.being_compacted) { continue; }
        file_names.push_back(file.name);
    }
    tmpdb::CompactionTask *task = new tmpdb::CompactionTask(db, this, "default", file_names, level_idx, this->rocksdb_compact_opt, 0, false);
    this->ScheduleCompaction(task);

    return status;
}
