#include <iostream>

#include "clipp.h"
#include "spdlog/spdlog.h"

#include "rocksdb/db.h"
#include "tmpdb/fluid_lsm_compactor.hpp"
#include "infrastructure/data_generator.hpp"


typedef struct
{
    std::string db_path;

    size_t non_empty_reads = 100;
    size_t empty_reads = 100;
    size_t writes = 100;

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


    auto cli = (
        general_opt,
        execute_opt
    );

    if (!parse(argc, argv, cli) || help)
    {
        auto fmt = doc_formatting{}.doc_column(42);
        std::cout << make_man_page(cli, "exp_robust", fmt);
        exit(EXIT_FAILURE);
    }

    return env;
}


int main(int argc, char * argv[])
{
    spdlog::set_pattern("[%T.%e] %^[%l]%$ %v");
    environment env = parse_args(argc, argv);

    spdlog::info("Welcome to the db_runner");
    if(env.verbose == 1)
    {
        spdlog::set_level(spdlog::level::debug);
    }
    else if(env.verbose == 2)
    {
        spdlog::set_level(spdlog::level::trace);
    }
    else
    {
        spdlog::set_level(spdlog::level::info);
    }

    return 0;
}