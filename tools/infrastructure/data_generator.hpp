#ifndef DATA_GENERATOR_H_
#define DATA_GENERATOR_H_

#include <ctime>
#include <string>
#include <vector>

#include "common/debug.hpp"

#define KEY_DOMAIN 1000000000

class DataGenerator
{
public:
    int seed = 0;

    DataGenerator();
    DataGenerator(int seed);

    std::string generate_key(const std::string key_prefix);

    std::string generate_val(size_t value_size, const std::string value_prefix);

    std::pair<std::string, std::string> generate_kv_pair(
        size_t kv_size,
        const std::string key_prefix,
        const std::string value_prefix);
};


#endif /* DATA_GENERATOR_H_ */