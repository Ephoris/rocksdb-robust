#include <iostream>
#include <ctime>

#include "clipp.h"
#include "spdlog/spdlog.h"

#include "rocksdb/db.h"
#include "tmpdb/fluid_lsm_compactor.hpp"
#include "infrastructure/data_generator.hpp"


typedef struct
{
    std::string db_path;

    size_t non_empty_reads = 0;
    size_t empty_reads = 0;
    size_t writes = 0;

    int rocksdb_max_levels = 100;
    int parallelism = 1;

    int compaction_readahead_size = 64;
    int seed = std::time(nullptr);

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

    // rocksdb_opt.write_buffer_size = fluid_opt.buffer_size;
    // rocksdb_opt.num_levels = env.rocksdb_max_levels;
    // rocksdb_opt.compaction_readahead_size = 1024 * env.compaction_readahead_size;
    // rocksdb_opt.use_direct_reads = true;
    // rocksdb_opt.allow_mmap_writes = false;
    // rocksdb_opt.new_table_reader_for_compaction_inputs = true;
    // rocksdb_opt.writable_file_max_buffer_size = 2 * rocksdb_opt.compaction_readahead_size;
    // rocksdb_opt.use_direct_io_for_flush_and_compaction = true;
    // rocksdb_opt.allow_mmap_reads = false;
    // rocksdb_opt.allow_mmap_writes = false;

    rocksdb_opt.IncreaseParallelism(env.parallelism);
    tmpdb::FluidLSMCompactor * fluid_compactor = new tmpdb::FluidLSMCompactor(fluid_opt, rocksdb_opt);
    rocksdb_opt.listeners.emplace_back(fluid_compactor);

    spdlog::info("Opening DB for writes");
    rocksdb::DB * db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(rocksdb_opt, env.db_path, &db);
    if (!status.ok())
    {
        spdlog::error("Problems opening DB");
        spdlog::error("{}", status.ToString());
        delete db;
        exit(EXIT_FAILURE);
    }
    fluid_compactor->init_open_db(db);

    // rocksdb::WriteOptions write_opt;
    // write_opt.sync = false; //make every write wait for sync with log (so we see real perf impact of insert)
    // write_opt.low_pri = true; // every insert is less important than compaction
    // write_opt.disableWAL = false; 
    // write_opt.no_slowdown = false; // enabling this will make some insertions fail

    spdlog::info("Writing {} key-value pairs", env.writes);
    RandomGenerator data_gen = RandomGenerator(env.seed);
    for (size_t write_idx = 0; write_idx < env.writes; write_idx++)
    {
        std::pair<std::string, std::string> entry = data_gen.generate_kv_pair(fluid_opt.entry_size);
        status = db->Put(rocksdb::WriteOptions(), entry.first, entry.second);
        spdlog::trace("Finished write {}", write_idx);
        if (!status.ok())
        {
            spdlog::warn("Unable to put key {}", write_idx);
            spdlog::error("{}", status.ToString());
        }
    }

    rocksdb::FlushOptions flush_opt;
    flush_opt.wait = true;
    db->Flush(flush_opt);

    db->Close();
    delete db;

    spdlog::info("Finshed writes, closing DB");

    return status;
}


int main(int argc, char * argv[])
{
    spdlog::set_pattern("[%T.%e] %^[%l]%$ %v");
    environment env = parse_args(argc, argv);

    spdlog::info("Welcome to the db_runner");
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

    rocksdb::Status write_status = run_random_inserts(env);

    return 0;
}