#include <iostream>

#include "clipp.h"

#include "infrastructure/bulk_loader.hpp"
#include "common/debug.hpp"

typedef struct
{
    std::string db_path;
    
    size_t non_empty_reads = 100;
    size_t empty_reads = 100;
    size_t writes = 100;
} environment;


environment parse_args(int argc, char * argv[])
{
    using namespace clipp;
    using std::to_string;

    environment env;
    bool help = false;

    auto execute_opt = "execute options:" % (
        (value("db_path", env.db_path)) % "path to the db",
        (option("-e", "--empty_reads") & integer("num", env.empty_reads))
            % ("empty queries, [default: " + to_string(env.empty_reads) + "]"),
        (option("-r", "--non_empty_reads") & integer("num", env.non_empty_reads))
            % ("empty queries, [default: " + to_string(env.non_empty_reads) + "]"),
        (option("-w", "--writes") & integer("num", env.writes))
            % ("empty queries, [default: " + to_string(env.writes) + "]")
    );

    auto general_opt = "general options" % (
        option("-v", "--verbose") % "show detailed output",
        (option("-h", "--help").set(help, true)) % "prints this message"
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
    printf("DB Runner\n");

    environment env = parse_args(argc, argv);

    return 0;
}