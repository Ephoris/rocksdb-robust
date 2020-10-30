#include <iostream>

#include "clipp.h"

#include "rocksdb/db.h"
#include "tmpdb/fluid_lsm_compactor.hpp"
#include "infrastructure/bulk_loader.hpp"
#include "infrastructure/data_generator.hpp"
#include "common/debug.hpp"

typedef enum { BUILD, EXECUTE } cmd_mode;

typedef enum { ENTRIES, LEVELS } build_mode;

typedef struct
{
    std::string db_path;
    build_mode build_fill;

    // Build mode
    double T = 2;
    double K = 1;
    double Z = 1;
    size_t B = 1048576;
    size_t E = 8192;
    double bits_per_element = 5.0;
    size_t N = 1e6;
    size_t L = 1;

    bool verbose = false;
    bool destroy_db = false;

    int max_rocksdb_levels = 100;

} environment;


environment parse_args(int argc, char * argv[])
{
    using namespace clipp;
    using std::to_string;

    environment env;
    bool help = false;

    auto general_opt = "general options" % (
        (option("-v", "--verbose").set(env.verbose)) % "show detailed output",
        (option("-h", "--help").set(help, true)) % "prints this message"
    );

    auto build_opt = (
        "build options:" % (
            (value("db_path", env.db_path)) % "path to the db",
            (option("-T", "--size_ratio") & number("ratio", env.T))
                % ("size ratio, [default: " + to_string(env.T) + "]"),
            (option("-K", "--lower_level_size_ratio") & number("ratio", env.K))
                % ("size ratio, [default: " + to_string(env.K) + "]"),
            (option("-Z", "--largest_level_size_ratio") & number("ratio", env.Z))
                % ("size ratio, [default: " + to_string(env.Z) + "]"),
            (option("-B", "--buffer_size") & integer("size", env.B))
                % ("buffer size (in bytes), [default: " + to_string(env.B) + "]"),
            (option("-E", "--entry_size") & integer("size", env.E))
                % ("entry size (bytes) [default: " + to_string(env.E) + ", min: 32]"),
            (option("-b", "--bpe") & number("bits", env.bits_per_element))
                % ("bits per entry per bloom filter across levels [default: " + to_string(env.bits_per_element) + "]"),
            (option("-d", "--destroy").set(env.destroy_db)) % "destroy the DB if it exists at the path"
        ),
        "db fill options (pick one):" % (
            one_of(
                (option("-N", "--entries").set(env.build_fill, build_mode::ENTRIES) & integer("num", env.N))
                    % ("total entries, default pick [default: " + to_string(env.N) + "]"),
                (option("-L", "--levels").set(env.build_fill, build_mode::LEVELS) & integer("num", env.L)) 
                    % ("total filled levels [default: " + to_string(env.L) + "]")
            )
        ),
        "minor options:" % (
            (option("--max_rocksdb_level") & integer("num", env.max_rocksdb_levels))
                % ("limits the maximum levels rocksdb has [default :" + to_string(env.max_rocksdb_levels) + "]"),
            (option(""))
        )
    );

    auto cli = (
        general_opt, 
        build_opt 
    );

    if (!parse(argc, argv, cli))
        help = true;

    if (env.E < 32)
    {
        help = true;
        printf("Entry size is less then 32 bytes.\n");
    }

    if (help)
    {
        auto fmt = doc_formatting{}.doc_column(42);
        std::cout << make_man_page(cli, "db_builder", fmt);
        exit(EXIT_FAILURE);
    }

    return env;
}


void fill_fluid_opt(environment env, tmpdb::FluidOptions & fluid_opt)
{
    fluid_opt.size_ratio = env.T;
    fluid_opt.largest_level_run_max = env.Z;
    fluid_opt.lower_level_run_max = env.K;
    fluid_opt.buffer_size = env.B;
    fluid_opt.entry_size = env.E;
    fluid_opt.bits_per_element = env.bits_per_element;
}


void build_db(environment & env)
{
    printf("Building database at %s\n", env.db_path.c_str());
    rocksdb::Options rocksdb_opt;
    tmpdb::FluidOptions fluid_opt;

    rocksdb_opt.create_if_missing = true;
    rocksdb_opt.compaction_style = rocksdb::kCompactionStyleNone;
    rocksdb_opt.compression = rocksdb::kNoCompression;
    rocksdb_opt.IncreaseParallelism(1);

    rocksdb_opt.PrepareForBulkLoad();
    rocksdb_opt.num_levels = env.max_rocksdb_levels;

    fill_fluid_opt(env, fluid_opt);
    tmpdb::FluidLSMCompactor * fluid_compactor = new tmpdb::FluidLSMCompactor(fluid_opt, rocksdb_opt);
    rocksdb_opt.listeners.emplace_back(fluid_compactor);

    rocksdb::DB * db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(rocksdb_opt, env.db_path, &db);
    if (!status.ok())
    {
        fprintf(stderr, "[ERROR: Status] %s\n", status.ToString().c_str());
        delete db;
        exit(EXIT_FAILURE);
    }

    fluid_compactor->init_open_db(db);

    db->Close();
    delete db;
}


int main(int argc, char * argv[])
{
    printf("DB Builder\n");

    environment env = parse_args(argc, argv);

    if (env.destroy_db)
    {
        printf("Destroying database at: %s\n", env.db_path.c_str());
        rocksdb::DestroyDB(env.db_path, rocksdb::Options());
    }

    build_db(env);

    return EXIT_SUCCESS;
}