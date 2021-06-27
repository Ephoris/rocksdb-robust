#include "monkey/monkey_filter_policy.hpp"

using namespace monkey;

MonkeyFilterPolicy::MonkeyFilterPolicy(double bits_per_element, int size_ratio, size_t levels)
    : default_bpe(bits_per_element),
      size_ratio(size_ratio),
      levels(levels),
      default_policy(rocksdb::NewBloomFilterPolicy(bits_per_element))
{
    this->allocate_bits_per_level();
}

MonkeyFilterPolicy::~MonkeyFilterPolicy() {}

void MonkeyFilterPolicy::allocate_bits_per_level()
{
    this->policy_per_level.clear();
    this->level_fpr_opt.clear();
    this->level_bpe.clear();
    for (size_t level = 0; level < levels; level++)
    {
        this->level_fpr_opt.push_back(this->optimal_false_positive_rate(level + 1));
        this->level_bpe.push_back(-1 * std::log(this->level_fpr_opt[level]) / LOG2SQUARED);
        this->policy_per_level.push_back(rocksdb::NewBloomFilterPolicy(this->level_bpe[level]));
    }
}


double MonkeyFilterPolicy::optimal_false_positive_rate(size_t curr_level)
{
    int T = this->size_ratio;
    double front = std::pow(T, ((double) T / ((double) T - 1))) / std::pow(T, this->levels + 1 - curr_level);

    return front * std::pow(EULER, -1 * this->default_bpe * LOG2SQUARED);
}

// Incase block based filter is going on we'll use default filter policy
void MonkeyFilterPolicy::CreateFilter(const rocksdb::Slice *keys, int n, std::string *dst) const
{
    this->default_policy->CreateFilter(keys, n, dst);
}

// Incase block based filter is going on we'll use default filter policy
bool MonkeyFilterPolicy::KeyMayMatch(const rocksdb::Slice &key, const rocksdb::Slice &filter) const
{
    return default_policy->KeyMayMatch(key, filter);
}


rocksdb::FilterBitsBuilder *MonkeyFilterPolicy::GetBuilderWithContext(const rocksdb::FilterBuildingContext& context) const
{
    if (context.level_at_creation >= (int) this->policy_per_level.size())
    {
        return this->default_policy->GetBuilderWithContext(context);
    }
    return this->policy_per_level[context.level_at_creation]->GetBuilderWithContext(context);
}