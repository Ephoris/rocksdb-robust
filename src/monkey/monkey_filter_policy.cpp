#include "monkey/monkey_filter_policy.hpp"

using namespace monkey;

MonkeyFilterPolicy::MonkeyFilterPolicy(double bits_per_element, std::vector<int> runs_per_level)
    : default_bpe(bits_per_element),
    policy(rocksdb::NewBloomFilterPolicy(bits_per_element))
{
    int num_levels = runs_per_level.size();
    this->filters_meta.resize(num_levels);
    for (int level_idx = 0; level_idx < num_levels; level_idx++)
    {
        for (int run_idx = 0; run_idx < runs_per_level[level_idx]; run_idx++)
        {
            this->filters_meta[level_idx].push_back(filter_meta_data(level_idx, bits_per_element));
        }
    }
}


void MonkeyFilterPolicy::CreateFilter(const rocksdb::Slice *keys, int n, std::string *dst) const
{
    this->policy->CreateFilter(keys, n, dst);
}


bool MonkeyFilterPolicy::KeyMayMatch(const rocksdb::Slice &key, const rocksdb::Slice &filter) const
{
    return policy->KeyMayMatch(key, filter);
}


rocksdb::FilterBitsBuilder *MonkeyFilterPolicy::GetBuilderWithContext(const rocksdb::FilterBuildingContext& context) const
{
    return this->policy->GetBuilderWithContext(context);
}


rocksdb::FilterBitsReader *MonkeyFilterPolicy::GetFilterBitsReader(const rocksdb::Slice &contents) const
{
    return this->policy->GetFilterBitsReader(contents);
}