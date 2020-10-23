#include "bulk_loader.hpp"

// void print_env(environment env)
// {
//     printf("ENVIRONMENT VARIABLES:\n");
//     switch(env.mode)
//     {
//         case cmd_mode::BUILD:
//             printf("\tMode : BUILD\n");
//             printf("\tDatabase path : %s\n", env.db_path.c_str());
//             switch(env.build_fill)
//             {
//                 case build_mode::ENTRIES: printf("\tFilled via ENTRIES (N) : %zu\n", env.N); break;
//                 case build_mode::LEVELS: printf("\tFilled via LEVELS (L) : %zu\n", env.L); break;
//             }
//             printf("\tSize ratio (T) : %zu\n", env.T);
//             printf("\tBuffer size (B): %zu\n", env.B);
//             printf("\tEntry size (E) : %zu\n", env.E);
//             printf("\tBits per Element : %zu\n", env.bits_per_element);
//             break;
//         case cmd_mode::EXECUTE:
//             printf("\tMode : EXECUTE\n");
//             printf("\tDatabase path: %s\n", env.db_path.c_str());
//             printf("\tEmpty Reads (Z0) : %zu\n", env.non_empty_reads);
//             printf("\tNon Empty Reads (Z1) : %zu\n", env.empty_reads);
//             printf("\tWrites (W) : %zu\n", env.writes);
//             break;
//     }
// }


// rocksdb::Status bulk_load_random()
// {
//     rocksdb::Options options;
//     options.create_if_missing = true;
//     options.compaction_style = rocksdb::kCompactionStyleNone;
//     options.write_buffer_size = 8 * env.B; // Increase the buffer size to quickly bulk load
//     options.use_direct_reads = false;
//     options.compression = rocksdb::kNoCompression;

//     rocksdb::DB * db = nullptr;
//     rocksdb::Status status = rocksdb::DB::Open(options, env.db_path, &db);    

//     if (!status.ok()) 
//     {
//         fprintf(stderr, "[ERROR OPENING DB] : %s\n", status.ToString().c_str());
//         delete db;
//         return status;
//     }

//     delete db;
//     return status;
// }