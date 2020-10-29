#include "bulk_loader.hpp"

rocksdb::Status bulk_load_random(std::string db_path, size_t entry_size, size_t total_entries)
{
    rocksdb::Options options;
    options.create_if_missing = true;
    options.compaction_style = rocksdb::kCompactionStyleNone;
    options.write_buffer_size = entry_size * total_entries; // Increase the buffer size to quickly bulk load
    options.use_direct_reads = false;
    options.compression = rocksdb::kNoCompression;

    rocksdb::DB * db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);

    if (!status.ok())
    {
        fprintf(stderr, "[ERROR OPENING DB] : %s\n", status.ToString().c_str());
        delete db;
        return status;
    }

    delete db;
    return status;
}