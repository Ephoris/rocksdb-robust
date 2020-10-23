#include <iostream>

#include "clipp.h"

#include "infrastructure/bulk_loader.hpp"
#include "common/debug.hpp"

typedef enum { BUILD, EXECUTE } cmd_mode;

typedef enum { ENTRIES, LEVELS } build_mode;

typedef struct
{
    std::string db_path;
    build_mode build_fill;

    // Build mode
    size_t T = 2;
    size_t B = 1048576;
    size_t E = 8192;
    size_t bits_per_element = 5;
    size_t N = 1e6;
    size_t L = 1;

    bool verbose = false;

} environment;


environment parse_args(int argc, char * argv[])
{
    using namespace clipp;
    using std::to_string;

    environment env;
    bool help = false;

    auto build_opt = (
        "build options:" % (
            (value("db_path", env.db_path)) % "path to the db",
            (option("-T", "--size_ratio") & integer("ratio", env.T))
                % ("size ratio, [default: " + to_string(env.T) + "]"),
            (option("-B", "--buffer_size") & integer("size", env.B))
                % ("buffer size (in bytes), [default: " + to_string(env.B) + "]"),
            (option("-E", "--entry_size") & integer("size", env.E))
                % ("entry size (bytes) [default: " + to_string(env.E) + "]"),
            (option("-b", "--bpe") & number("bits", env.bits_per_element))
                % ("bits per entry per bloom filter across levels [default: " + to_string(env.bits_per_element) + "]")
        ),
        "db fill options (pick one):" % (
            one_of(
                (option("-N", "--entries").set(env.build_fill, build_mode::ENTRIES) & integer("num", env.N))
                    % ("total entries, default pick [default: " + to_string(env.N) + "]"),
                (option("-L", "--levels").set(env.build_fill, build_mode::LEVELS) & integer("num", env.L)) 
                    % ("total filled levels [default: " + to_string(env.L) + "]")
            )
        )
    );

    auto general_opt = "general options" % (
        (option("-v", "--verbose").set(env.verbose)) % "show detailed output",
        (option("-h", "--help").set(help, true)) % "prints this message"
    );

    auto cli = (
        general_opt, 
        build_opt 
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
    printf("DB Builder\n");

    environment env = parse_args(argc, argv);

    return 0;
}
