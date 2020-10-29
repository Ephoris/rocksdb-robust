#ifndef FLUID_OPTIONS_H_
#define FLUID_OPTIONS_H_

#include <cstdlib>
#include <limits>

namespace tmpdb
{

typedef enum {LEVELING, TIERING, LAZY_LEVELING} CompactionPolicy;

class FluidOptions
{
public:
    double size_ratio = 2;              //> (T)
    double largest_level_run_max = 1;   //> (Z)
    double lower_level_run_max = 1;     //> (K)
    size_t buffer_size = 1048576;       //> bytes (B)
    size_t entry_size = 8192;           //> bytes (E)
    double bits_per_element = 5.0;      //> bits per element per bloom filter at all levels (h)

    size_t file_size = std::numeric_limits<size_t>::max();

    CompactionPolicy compaction_policy = TIERING;

    FluidOptions() {};
};

} /* namespace tmpdb */

#endif /* FLUID_OPTIONS_H_ */