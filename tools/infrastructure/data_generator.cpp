#include "data_generator.hpp"


DataGenerator::DataGenerator()
{
    this->seed = std::time(nullptr);

    spdlog::trace("Data generator with seed : {}", seed);

    std::srand(this->seed);
}


DataGenerator::DataGenerator(int seed)
{
    this->seed = seed;

    spdlog::trace("Data generator with seed : {}", seed);

    std::srand(seed);
}


RandomGenerator::RandomGenerator()
{
    this->engine.seed(this->seed);
    this->dist = std::uniform_int_distribution<int>(0, KEY_DOMAIN);
}


RandomGenerator::RandomGenerator(int seed)
{
    this->seed = seed;
    RandomGenerator();
}


std::pair<std::string, std::string> DataGenerator::generate_kv_pair(size_t kv_size)
{
    return this->generate_kv_pair(kv_size, "", "");
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


std::string RandomGenerator::generate_key(const std::string key_prefix)
{
    int rand = this->dist(this->engine);
    std::string key = key_prefix + std::to_string(rand);

    return key;
}


std::string RandomGenerator::generate_val(size_t value_size, const std::string value_prefix)
{
    unsigned long random_size = value_size - value_prefix.size();
    std::string value = value_prefix + std::string(random_size, 'a');

    return value;
}
