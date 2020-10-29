#include "bulk_loader.hpp"


// rocksdb::Status LSMBulkLoader::bulk_load_entries(rocksdb::DB * db, size_t num_entries)
// {
//     // size_t E = this->fluid_opt.entry_size;
//     // size_t B = this->fluid_opt.buffer_size;
//     // double T = this->fluid_opt.size_ratio;

//     // size_t derived_num_levels = ceil(log((num_entries * E * (T - 1)) / (B * T )) / log(T));

//     return rocksdb::Status::Busy();
// }


// rocksdb::Status LSMBulkLoader::bulk_load_levels(rocksdb::DB * db, size_t levels)
// {
//     return rocksdb::Status::Busy();
// }
