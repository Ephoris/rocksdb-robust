#include "bulk_loader.hpp"


rocksdb::Status FluidLSMBulkLoader::bulk_load_entries(rocksdb::DB * db, size_t num_entries)
{
    spdlog::info("Bulk loading DB with {} entries", num_entries);
    rocksdb::Status status;

    size_t E = this->fluid_opt.entry_size;
    size_t B = this->fluid_opt.buffer_size;
    double T = this->fluid_opt.size_ratio;
    size_t estimated_levels = std::ceil(std::log((num_entries * E * (T - 1)) / (B * T )) / std::log(T));

    size_t entries_buffer = (B / E);
    std::vector <size_t> entries_per_level(estimated_levels);

    size_t current_level_size = num_entries * (T - 1) / T;
    size_t level_idx;
    spdlog::debug("Estimated levels: {}", estimated_levels);
    spdlog::debug("Entries buffer: {}", entries_buffer);
    spdlog::debug("Lowest level size: {}", current_level_size);
    for (size_t level = estimated_levels; level > 0; level--)
    {
        level_idx = level - 1;
        entries_per_level[level_idx] = current_level_size;
        current_level_size /= T;

        if (level_idx == 0 && entries_per_level[level_idx] < entries_buffer)
        {
            entries_per_level[level_idx] = entries_buffer;
        }
    }
    status = this->bulk_load(db, entries_per_level, estimated_levels);

    return status;
}


rocksdb::Status FluidLSMBulkLoader::bulk_load(rocksdb::DB * db, std::vector<size_t> entries_per_level, size_t num_levels)
{
    rocksdb::Status status;
    size_t level_idx;
    for (size_t level = num_levels; level > 0; level--)
    {
        level_idx = level - 1;
        spdlog::trace("Inserting {} to level {}", entries_per_level[level_idx], level_idx);
        if (entries_per_level[level_idx] == 0) { continue; }
        status = this->bulk_load_single_level(db, level_idx, entries_per_level[level_idx], this->fluid_opt.lower_level_run_max);
    }

    return status;
}

rocksdb::Status FluidLSMBulkLoader::bulk_load_single_level(rocksdb::DB * db, size_t level_idx, size_t num_entries, size_t num_runs)
{
    rocksdb::Status status;
    size_t num_entries_per_run = num_entries / num_runs;
    for (size_t run_idx = 0; run_idx < num_runs; run_idx++)
    {
        spdlog::trace("Loading run {} at level {} with {} entries", run_idx, level_idx, num_entries_per_run);
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

    size_t buffer_size = this->fluid_opt.entry_size * num_entries * 2;
    rocksdb::Status status = db->SetOptions({{"write_buffer_size", std::to_string(buffer_size)}});

    std::string value_prefix = std::to_string(level_idx) + "|";
    std::string key_prefix = value_prefix;

    for (size_t entry_num = 0; entry_num < num_entries; entry_num += BATCH_SIZE)
    {
        rocksdb::WriteBatch batch;
        for (int i = 0; i < BATCH_SIZE; i++)
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

    return status;
}