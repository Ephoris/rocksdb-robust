#include "monkey/monkey_filter_policy.hpp"

using namespace monkey;

MonkeyFilterPolicy::MonkeyFilterPolicy(double bits_per_element, int size_ratio, size_t levels)
    : default_bpe(bits_per_element),
      size_ratio(size_ratio),
      levels(levels),
      default_policy(rocksdb::NewBloomFilterPolicy(bits_per_element))
{
    for (size_t level; level < levels; level++)
    {
        level_fpr_opt.push_back()
    }
}


double MonkeyFilterPolicy::optimal_false_positive_rate(size_t curr_level)
{
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