#include <chrono>
#include <ctime>
#include <iostream>

#include "clipp.h"
#include "spdlog/spdlog.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "tmpdb/fluid_lsm_compactor.hpp"
#include "infrastructure/data_generator.hpp"


typedef struct
{
    std::string db_path;

    size_t non_empty_reads = 0;
    size_t empty_reads = 0;
    size_t writes = 0;

    int rocksdb_max_levels = 10;
    int parallelism = 1;

    int compaction_readahead_size = 64;
    int seed = std::time(nullptr);
    int max_open_files = 128;

    int verbose = 0;
} environment;


environment parse_args(int argc, char * argv[])
{
    using namespace clipp;
    using std::to_string;

    environment env;
    bool help = false;

    auto general_opt = "general options" % (
        (option("-v", "--verbose") & integer("level", env.verbose))
            % ("Logging levels (DEFAULT: INFO, 1: DEBUG, 2: TRACE)"),
        (option("-h", "--help").set(help, true)) % "prints this message"
    );

    auto execute_opt = "execute options:" % (
        (value("db_path", env.db_path)) % "path to the db",
        (option("-e", "--empty_reads") & integer("num", env.empty_reads))
            % ("empty queries, [default: " + to_string(env.empty_reads) + "]"),
        (option("-r", "--non_empty_reads") & integer("num", env.non_empty_reads))
            % ("empty queries, [default: " + to_string(env.non_empty_reads) + "]"),
        (option("-w", "--writes") & integer("num", env.writes))
            % ("empty queries, [default: " + to_string(env.writes) + "]")
    );

    auto minor_opt = "minor options:" % (
        (option("--parallelism") & integer("threads", env.parallelism))
            % ("Threads allocated for RocksDB [default: " + to_string(env.parallelism) + "]"),
        (option("--compact-readahead") & integer("size", env.compaction_readahead_size))
            % ("Use 2048 for HDD, 64 for flash [default: " + to_string(env.compaction_readahead_size) + "]")
    );


    auto cli = (
        general_opt,
        execute_opt,
        minor_opt
    );

    if (!parse(argc, argv, cli) || help)
    {
        auto fmt = doc_formatting{}.doc_column(42);
        std::cout << make_man_page(cli, "exp_robust", fmt);
        exit(EXIT_FAILURE);
    }

    return env;
}


std::vector<std::string> get_all_valid_keys(environment env)
{
    // TODO: In reality we should be saving the list of all keys while building the DB to speed up testing. No need to
    // go through and retrieve all keys manually
    spdlog::info("Grabbing existing keys");
    std::vector<std::string> existing_keys;
    rocksdb::Options rocksdb_opt;
    tmpdb::FluidOptions fluid_opt(env.db_path + "/fluid_config.json");

    rocksdb_opt.create_if_missing = false;
    rocksdb_opt.error_if_exists = false;
    rocksdb_opt.compaction_style = rocksdb::kCompactionStyleNone;
    rocksdb_opt.compression = rocksdb::kNoCompression;

    // rocksdb_opt.num_levels = env.rocksdb_max_levels;
    // rocksdb_opt.use_direct_reads = true;
    // rocksdb_opt.IncreaseParallelism(env.parallelism);

    tmpdb::FluidLSMCompactor *fluid_compactor = new tmpdb::FluidLSMCompactor(fluid_opt, rocksdb_opt);
    rocksdb_opt.listeners.emplace_back(fluid_compactor);

    rocksdb::DB *db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(rocksdb_opt, env.db_path, &db);
    if (!status.ok())
    {
        spdlog::error("Problems opening DB");
        spdlog::error("{}", status.ToString());
        delete db;
        exit(EXIT_FAILURE);
    }

    rocksdb::Iterator *rocksdb_it = db->NewIterator(rocksdb::ReadOptions());
    rocksdb_it->SeekToFirst();
    while (rocksdb_it->Valid())
    {
        spdlog::trace("{}", rocksdb_it->key().ToString());
        rocksdb_it->Next();
    }
    // if (!rocksdb_it->status().ok())
    // {
    //     spdlog::error("Unable to retrieve all keys : {}", rocksdb_it->status().ToString());
    //     delete rocksdb_it;
    //     delete db;
    //     exit(EXIT_FAILURE);
    // }

    db->Close();
    delete rocksdb_it;
    delete db;

    return existing_keys;
}


rocksdb::Status run_random_non_empty_reads(environment env, std::vector<std::string> existing_keys)
{
    spdlog::info("Opening DB for random reads: {}", env.db_path);
    rocksdb::Options rocksdb_opt;
    tmpdb::FluidOptions fluid_opt(env.db_path + "/fluid_config.json");

    rocksdb_opt.create_if_missing = false;
    rocksdb_opt.error_if_exists = false;
    rocksdb_opt.compaction_style = rocksdb::kCompactionStyleNone;
    rocksdb_opt.compression = rocksdb::kNoCompression;

    rocksdb_opt.num_levels = env.rocksdb_max_levels;
    rocksdb_opt.IncreaseParallelism(env.parallelism);

    tmpdb::FluidLSMCompactor *fluid_compactor = new tmpdb::FluidLSMCompactor(fluid_opt, rocksdb_opt);
    rocksdb_opt.listeners.emplace_back(fluid_compactor);

    rocksdb::DB *db = nullptr;
    rocksdb::Status status = rocksdb::DB::OpenForReadOnly(rocksdb_opt, env.db_path, &db);
    if (!status.ok())
    {
        spdlog::error("Problems opening DB");
        spdlog::error("{}", status.ToString());
        delete db;
        exit(EXIT_FAILURE);
    }

    rocksdb::ReadOptions read_opt;
    spdlog::trace("Key example: {}", existing_keys[0]);

    db->Close();
    delete db;

    return rocksdb::Status::OK();
}


rocksdb::Status run_random_empty_reads(environment env, std::vector<std::string> existing_keys)
{
    spdlog::info("Opening DB for random reads: {}", env.db_path);
    rocksdb::Options rocksdb_opt;
    tmpdb::FluidOptions fluid_opt(env.db_path + "/fluid_config.json");

    rocksdb_opt.create_if_missing = false;
    rocksdb_opt.error_if_exists = false;
    rocksdb_opt.compaction_style = rocksdb::kCompactionStyleNone;
    rocksdb_opt.compression = rocksdb::kNoCompression;

    rocksdb_opt.num_levels = env.rocksdb_max_levels;
    rocksdb_opt.IncreaseParallelism(env.parallelism);

    tmpdb::FluidLSMCompactor *fluid_compactor = new tmpdb::FluidLSMCompactor(fluid_opt, rocksdb_opt);
    rocksdb_opt.listeners.emplace_back(fluid_compactor);

    rocksdb::DB *db = nullptr;
    rocksdb::Status status = rocksdb::DB::OpenForReadOnly(rocksdb_opt, env.db_path, &db);
    if (!status.ok())
    {
        spdlog::error("Problems opening DB");
        spdlog::error("{}", status.ToString());
        delete db;
        exit(EXIT_FAILURE);
    }

    rocksdb::ReadOptions read_opt;

    db->Close();
    delete db;

    return rocksdb::Status::OK();
}


rocksdb::Status run_random_inserts(environment env)
{
    spdlog::info("Opening DB: {}", env.db_path);
    rocksdb::Options rocksdb_opt;
    tmpdb::FluidOptions fluid_opt(env.db_path + "/fluid_config.json");

    rocksdb_opt.create_if_missing = false;
    rocksdb_opt.error_if_exists = false;
    rocksdb_opt.compaction_style = rocksdb::kCompactionStyleNone;
    rocksdb_opt.compression = rocksdb::kNoCompression;
    rocksdb_opt.IncreaseParallelism(env.parallelism);

    rocksdb_opt.write_buffer_size = fluid_opt.buffer_size; //> "Level 0" or the in memory buffer
    rocksdb_opt.num_levels = env.rocksdb_max_levels;
    rocksdb_opt.compaction_readahead_size = 1024 * env.compaction_readahead_size;
    rocksdb_opt.use_direct_io_for_flush_and_compaction = true;
    rocksdb_opt.max_open_files = env.max_open_files;

    // Note that level 0 in RocksDB is traditionally level 1 in an LSM model. The write buffer is what we normally would
    // label as level 0. Here we want level 1 to contain T sst files before trigger a compaction. Need to test whether
    // this mattesrs given our custom compaction listener
    rocksdb_opt.level0_file_num_compaction_trigger = fluid_opt.size_ratio;

    // Number of files in level 0 to slow down writes. Since we're prioritizing compactions we will wait for those to
    // finish up first by slowing down the write speed
    rocksdb_opt.level0_slowdown_writes_trigger = fluid_opt.size_ratio * 2;

    rocksdb_opt.IncreaseParallelism(env.parallelism);
    tmpdb::FluidLSMCompactor *fluid_compactor = new tmpdb::FluidLSMCompactor(fluid_opt, rocksdb_opt);
    rocksdb_opt.listeners.emplace_back(fluid_compactor);

    rocksdb::DB *db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(rocksdb_opt, env.db_path, &db);
    if (!status.ok())
    {
        spdlog::error("Problems opening DB");
        spdlog::error("{}", status.ToString());
        delete db;
        exit(EXIT_FAILURE);
    }

    rocksdb::WriteOptions write_opt;
    write_opt.sync = false; //> make every write wait for sync with log (so we see real perf impact of insert)
    write_opt.low_pri = true; //> every insert is less important than compaction
    write_opt.disableWAL = false; 
    write_opt.no_slowdown = false; //> enabling this will make some insertions fail

    int max_writes_failed = env.writes * 0.1;
    int writes_failed = 0;

    spdlog::info("Writing {} key-value pairs", env.writes);
    RandomGenerator data_gen = RandomGenerator(env.seed);
    auto start_write_time = std::chrono::high_resolution_clock::now();
    for (size_t write_idx = 0; write_idx < env.writes; write_idx++)
    {
        std::pair<std::string, std::string> entry = data_gen.generate_kv_pair(fluid_opt.entry_size);
        status = db->Put(write_opt, entry.first, entry.second);
        if (!status.ok())
        {
            spdlog::warn("Unable to put key {}", write_idx);
            spdlog::error("{}", status.ToString());
            writes_failed++;
            if (writes_failed > max_writes_failed)
            {
                spdlog::error("10% of total writes have failed, aborting");
                db->Close();
                delete db;
                exit(EXIT_FAILURE);
            }
        }
    }

    rocksdb::FlushOptions flush_opt;
    flush_opt.wait = true;
    db->Flush(flush_opt);

    while(fluid_compactor->compactions_left_count > 0);

    // We perform one more flush and wait for any last minute remaining compactions due to RocksDB interntally renaming
    // SST files during parallel compactions
    flush_opt.wait = true;
    db->Flush(flush_opt);

    spdlog::debug("Waiting for all remaining compactions to finish before after writes");
    while(fluid_compactor->compactions_left_count > 0);

    auto end_write_time = std::chrono::high_resolution_clock::now();
    auto write_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_write_time - start_write_time);
    spdlog::info("Write time elapsed : {} us", write_duration.count());

    if (spdlog::get_level() <= spdlog::level::debug)
    {
        spdlog::debug("Files per level");
        rocksdb::ColumnFamilyMetaData cf_meta;
        db->GetColumnFamilyMetaData(&cf_meta);

        std::vector<std::string> file_names;
        int level_idx = 1;
        for (auto & level : cf_meta.levels)
        {
            std::string level_str = "";
            for (auto & file : level.files)
            {
                level_str += file.name + ", ";
            }
            level_str = level_str == "" ? "EMPTY" : level_str.substr(0, level_str.size() - 2);
            spdlog::debug("Level {} : {}", level_idx, level_str);
            level_idx++;
        }
    }

    spdlog::info("Finshed writes, closing DB");
    db->Close();
    delete db;

    return status;
}


int main(int argc, char * argv[])
{
    spdlog::set_pattern("[%T.%e] %^[%l]%$ %v");
    environment env = parse_args(argc, argv);

    spdlog::info("Welcome to the db_runner");
    spdlog::info("Welcome to db_builder!");
    if(env.verbose == 1)
    {
        spdlog::info("Log level: DEBUG");
        spdlog::set_level(spdlog::level::debug);
    }
    else if(env.verbose == 2)
    {
        spdlog::info("Log level: TRACE");
        spdlog::set_level(spdlog::level::trace);
    }
    else
    {
        spdlog::info("Log level: INFO");
        spdlog::set_level(spdlog::level::info);
    }

    if (env.writes > 0)
    {
        rocksdb::Status write_status = run_random_inserts(env);
    }

    if (env.non_empty_reads > 0 || env.empty_reads > 0)
    {
        std::vector<std::string> existing_keys = get_all_valid_keys(env);
        if (env.non_empty_reads > 0)
        {
            rocksdb::Status non_empty_read_status = run_random_non_empty_reads(env, existing_keys);
        }
        if (env.empty_reads > 0)
        {
            rocksdb::Status empty_read_status = run_random_empty_reads(env, existing_keys); 
        }
    }

    return 0;
}