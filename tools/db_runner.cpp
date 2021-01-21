#include <chrono>
#include <ctime>
#include <iostream>
#include <random>
#include <unistd.h>

#include "clipp.h"
#include "spdlog/spdlog.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/env.h"

#include "tmpdb/fluid_lsm_compactor.hpp"
#include "infrastructure/data_generator.hpp"


typedef struct
{
    std::string db_path;

    size_t non_empty_reads = 0;
    size_t empty_reads = 0;
    size_t writes = 0;

    int rocksdb_max_levels = 35;
    int parallelism = 1;

    int compaction_readahead_size = 64;
    int seed = std::time(nullptr);
    int max_open_files = 512;

    std::string write_out_path;
    bool write_out = false;

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
            % ("empty queries, [default: " + to_string(env.writes) + "]"),
        (option("-o", "--output").set(env.write_out) & value("file", env.write_out_path))
            % ("optional write out all recorded times [default: off]")
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
    spdlog::debug("Grabbing existing keys");
    std::vector<std::string> existing_keys;
    rocksdb::Options rocksdb_opt;
    tmpdb::FluidOptions fluid_opt(env.db_path + "/fluid_config.json");

    rocksdb_opt.create_if_missing = false;
    rocksdb_opt.error_if_exists = false;
    rocksdb_opt.compaction_style = rocksdb::kCompactionStyleNone;
    rocksdb_opt.compression = rocksdb::kNoCompression;
    rocksdb_opt.num_levels = env.rocksdb_max_levels;

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
    for (rocksdb_it->SeekToFirst(); rocksdb_it->Valid(); rocksdb_it->Next())
    {
        existing_keys.push_back(rocksdb_it->key().ToString());
    }

    if (!rocksdb_it->status().ok())
    {
        spdlog::error("Unable to retrieve all keys : {}", rocksdb_it->status().ToString());
        delete rocksdb_it;
        delete db;
        exit(EXIT_FAILURE);
    }

    db->Close();
    delete db;

    return existing_keys;
}


int run_random_non_empty_reads(environment env, std::vector<std::string> existing_keys)
{
    spdlog::debug("Opening DB for random non empty reads: {}", env.db_path);
    rocksdb::Options rocksdb_opt;
    tmpdb::FluidOptions fluid_opt(env.db_path + "/fluid_config.json");

    rocksdb_opt.create_if_missing = false;
    rocksdb_opt.error_if_exists = false;
    rocksdb_opt.compaction_style = rocksdb::kCompactionStyleNone;
    rocksdb_opt.compression = rocksdb::kNoCompression;

    rocksdb_opt.use_direct_reads = true;
    rocksdb_opt.num_levels = env.rocksdb_max_levels;
    rocksdb_opt.IncreaseParallelism(env.parallelism);

    tmpdb::FluidLSMCompactor *fluid_compactor = new tmpdb::FluidLSMCompactor(fluid_opt, rocksdb_opt);
    rocksdb_opt.listeners.emplace_back(fluid_compactor);

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(fluid_opt.bits_per_element, false));
    rocksdb_opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

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

    std::string value;
    std::mt19937 engine;
    std::uniform_int_distribution<int> dist(0, existing_keys.size() - 1);

    auto non_empty_read_start = std::chrono::high_resolution_clock::now();
    for (size_t read_count = 0; read_count < env.non_empty_reads; read_count++)
    {
        status = db->Get(read_opt, existing_keys[dist(engine)], &value);
    }
    auto non_empty_read_end = std::chrono::high_resolution_clock::now();
    auto non_empty_read_duration = std::chrono::duration_cast<std::chrono::milliseconds>(non_empty_read_end - non_empty_read_start);
    spdlog::info("Non empty read time elapsed : {} ms", non_empty_read_duration.count());

    db->Close();
    delete db;

    return non_empty_read_duration.count();
}


int run_random_empty_reads(environment env)
{
    spdlog::debug("Opening DB for random empty reads: {}", env.db_path);
    rocksdb::Options rocksdb_opt;
    tmpdb::FluidOptions fluid_opt(env.db_path + "/fluid_config.json");

    rocksdb_opt.create_if_missing = false;
    rocksdb_opt.error_if_exists = false;
    rocksdb_opt.compaction_style = rocksdb::kCompactionStyleNone;
    rocksdb_opt.compression = rocksdb::kNoCompression;

    rocksdb_opt.use_direct_reads = true;
    rocksdb_opt.num_levels = env.rocksdb_max_levels;
    rocksdb_opt.IncreaseParallelism(env.parallelism);

    tmpdb::FluidLSMCompactor *fluid_compactor = new tmpdb::FluidLSMCompactor(fluid_opt, rocksdb_opt);
    rocksdb_opt.listeners.emplace_back(fluid_compactor);

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(fluid_opt.bits_per_element, false));
    rocksdb_opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

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

    std::string value;
    std::mt19937 engine;
    std::uniform_int_distribution<int> dist(KEY_DOMAIN + 1, 2 * KEY_DOMAIN);

    auto empty_read_start = std::chrono::high_resolution_clock::now();
    for (size_t read_count = 0; read_count < env.non_empty_reads; read_count++)
    {
        status = db->Get(read_opt, std::to_string(dist(engine)), &value);
    }
    auto empty_read_end = std::chrono::high_resolution_clock::now();
    auto empty_read_duration = std::chrono::duration_cast<std::chrono::milliseconds>(empty_read_end - empty_read_start);
    spdlog::info("Empty read time elapsed : {} ms", empty_read_duration.count());

    db->Close();
    delete db;

    return empty_read_duration.count();
}


int run_random_inserts(environment env)
{
    spdlog::debug("Opening DB for writes: {}", env.db_path);
    rocksdb::Options rocksdb_opt;
    tmpdb::FluidOptions fluid_opt(env.db_path + "/fluid_config.json");

    rocksdb_opt.create_if_missing = false;
    rocksdb_opt.error_if_exists = false;
    rocksdb_opt.compaction_style = rocksdb::kCompactionStyleNone;
    rocksdb_opt.compression = rocksdb::kNoCompression;
    rocksdb_opt.IncreaseParallelism(env.parallelism);

    rocksdb_opt.write_buffer_size = fluid_opt.buffer_size; //> "Level 0" or the in memory buffer
    rocksdb_opt.num_levels = env.rocksdb_max_levels;
    // rocksdb_opt.compaction_readahead_size = 1024 * env.compaction_readahead_size;
    rocksdb_opt.use_direct_reads = true;
    rocksdb_opt.use_direct_io_for_flush_and_compaction = true;
    rocksdb_opt.max_open_files = env.max_open_files;
    rocksdb_opt.IncreaseParallelism(env.parallelism);

    // Prevents rocksdb from limiting file size
    rocksdb_opt.target_file_size_base = UINT64_MAX;

    // Note that level 0 in RocksDB is traditionally level 1 in an LSM model. The write buffer is what we normally would
    // label as level 0. Here we want level 1 to contain T sst files before trigger a compaction. Need to test whether
    // this mattesrs given our custom compaction listener
    rocksdb_opt.level0_file_num_compaction_trigger = fluid_opt.lower_level_run_max + 1;

    // Number of files in level 0 to slow down writes. Since we're prioritizing compactions we will wait for those to
    // finish up first by slowing down the write speed
    rocksdb_opt.level0_slowdown_writes_trigger = 2 * (fluid_opt.lower_level_run_max + 1);
    rocksdb_opt.level0_stop_writes_trigger = 3 * (fluid_opt.lower_level_run_max + 1);

    tmpdb::FluidLSMCompactor *fluid_compactor = new tmpdb::FluidLSMCompactor(fluid_opt, rocksdb_opt);
    rocksdb_opt.listeners.emplace_back(fluid_compactor);

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(fluid_opt.bits_per_element, false));
    rocksdb_opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

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
    write_opt.low_pri = false; //> every insert is less important than compaction
    write_opt.disableWAL = true; 
    write_opt.no_slowdown = false; //> enabling this will make some insertions fail

    int max_writes_failed = env.writes * 0.1;
    int writes_failed = 0;

    spdlog::debug("Writing {} key-value pairs", env.writes);
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

    // We perform one more flush and wait for any last minute remaining compactions due to RocksDB interntally renaming
    // SST files during parallel compactions
    spdlog::debug("Flushing DB...");
    rocksdb::FlushOptions flush_opt;
    flush_opt.wait = true;
    flush_opt.allow_write_stall = true;

    db->Flush(flush_opt);

    spdlog::debug("Waiting for all remaining background compactions to finish before after writes");
    while(fluid_compactor->compactions_left_count > 0)
    {
        spdlog::debug("{} compactions left...", fluid_compactor->compactions_left_count.load());
        usleep(1000);
    }

    spdlog::debug("Checking final state of the tree and if it requires any compactions...");
    while(fluid_compactor->requires_compaction(db))
    {
        spdlog::debug("Requires compaction");
        while(fluid_compactor->compactions_left_count > 0)
        {
            spdlog::debug("{} compactions left...", fluid_compactor->compactions_left_count.load());
            usleep(1000);
        }
        usleep(1000);
    }


    auto end_write_time = std::chrono::high_resolution_clock::now();
    auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_write_time - start_write_time);
    spdlog::info("Write time elapsed : {} ms", write_duration.count());

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

    spdlog::debug("Closing DB");
    db->Close();
    delete db;

    return write_duration.count();
}


int main(int argc, char * argv[])
{
    spdlog::set_pattern("[%T.%e] %^[%l]%$ %v");
    environment env = parse_args(argc, argv);

    spdlog::info("Welcome to the db_runner!");
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
        spdlog::set_level(spdlog::level::info);
    }

    int write_duration = -1, read_duration = -1, empty_read_duration = -1;

    if (env.writes > 0)
    {
        write_duration = run_random_inserts(env);
    }

    if (env.non_empty_reads > 0)
    {
        std::vector<std::string> existing_keys = get_all_valid_keys(env);
        read_duration = run_random_non_empty_reads(env, existing_keys);
    }

    if (env.empty_reads > 0)
    {
        empty_read_duration = run_random_empty_reads(env); 
    }

    spdlog::info("(w, z1, z0) : ({}, {}, {})", write_duration, read_duration, empty_read_duration);

    return 0;
}
