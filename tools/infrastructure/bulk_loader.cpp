#include "bulk_loader.hpp"


rocksdb::Status FluidLSMBulkLoader::bulk_load_entries(rocksdb::DB * db, size_t num_entries)
{
    size_t E = this->fluid_opt.entry_size;
    size_t B = this->fluid_opt.buffer_size;
    double T = this->fluid_opt.size_ratio;
    size_t estimated_levels = std::ceil(std::log((num_entries * E * (T - 1)) / (B * T )) / std::log(T));

    size_t entries_buffer = (B / E);
    std::vector <size_t> entries_per_level(estimated_levels);

    // new bulk loading technique for number of entries (proportional per level)
    size_t current_level_size = num_entries * (T - 1) / T;
    size_t level_idx;
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

    return rocksdb::Status::Busy();
}


// rocksdb::Status LSMBulkLoader::bulk_load_levels(rocksdb::DB * db, size_t levels)
// {
//     return rocksdb::Status::Busy();
// }
