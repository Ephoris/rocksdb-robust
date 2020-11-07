#include "tmpdb/fluid_options.hpp"

using namespace tmpdb;
using json = nlohmann::json;

FluidOptions::FluidOptions(std::string config_path)
{
    this->read_config(config_path);
}

bool FluidOptions::read_config(std::string config_path)
{
    json cfg;
    std::ifstream read_cfg(config_path);
    if (!read_cfg.is_open())
    {
        spdlog::warn("Unable to read file: {}", config_path);
        spdlog::warn("Using default fluid options");
        return false;
    }
    read_cfg >> cfg;

    this->size_ratio = cfg["size_ratio"];
    this->lower_level_run_max = cfg["lower_level_run_max"];
    this->largest_level_run_max = cfg["largest_level_run_max"];
    this->buffer_size = cfg["buffer_size"];
    this->entry_size = cfg["entry_size"];
    this->bits_per_element = cfg["bits_per_element"];

    return true;
}

bool FluidOptions::write_config(std::string config_path)
{
    json cfg;
    cfg["size_ratio"] = this->size_ratio;
    cfg["lower_level_run_max"] = this->lower_level_run_max;
    cfg["largest_level_run_max"] = this->largest_level_run_max;
    cfg["buffer_size"] = this->buffer_size;
    cfg["entry_size"] = this->entry_size;
    cfg["bits_per_element"] = this->bits_per_element;

    std::ofstream out_cfg(config_path);
    if (!out_cfg.is_open())
    {
        spdlog::error("Unable to create or open file: {}", config_path);
        return false;
    }
    spdlog::trace("Dumping configuration file at {}", config_path);
    out_cfg << cfg.dump(4) << std::endl;
    out_cfg.close();

    return true;
}