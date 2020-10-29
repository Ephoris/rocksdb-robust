#include "data_generator.hpp"


DataGenerator::DataGenerator()
{
    std::srand(this->seed);
}


DataGenerator::DataGenerator(int seed)
{
    this->seed = seed;
    std::srand(seed);
}


std::string DataGenerator::generate_key(const std::string key_prefix)
{
    unsigned long long rand = std::rand() % KEY_DOMAIN;
    std::string key = key_prefix + std::to_string(rand);

    return key;
}


std::string DataGenerator::generate_val(size_t value_size, const std::string value_prefix)
{
    unsigned long random_size = value_size - value_prefix.size();
    std::string value = value_prefix + std::string(random_size, 'a');

    return value;
}


std::pair<std::string, std::string> DataGenerator::generate_kv_pair(
    size_t kv_size,
    const std::string key_prefix,
    const std::string value_prefix)
{
    std::string key = this->generate_key(key_prefix);
    assert(key.size() < kv_size && "Requires larger key size");
    size_t value_size = kv_size - key.size();
    std::string value = this->generate_val(value_size, value_prefix);

    return std::pair<std::string, std::string>(key, value);
}