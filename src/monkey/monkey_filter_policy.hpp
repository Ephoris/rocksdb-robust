#ifndef ROBUST_BLOOMFILTERS_H_
#define ROBUST_BLOOMFILTERS_H_

#include <iostream>
#include <vector>

#include "spdlog/spdlog.h"

#include "rocksdb/filter_policy.h"
#include "rocksdb/slice.h"

#define EULER 2.71828182845904523536
#define LOG2SQUARED 0.48045301391

namespace monkey {

typedef struct filter_meta_data
{
    size_t num_entries;
    int level_idx;
    int run_idx;
    double bits_per_element;
    const std::unique_ptr<const rocksdb::FilterPolicy> policy;

    filter_meta_data(int level_idx, double bits_per_element)
        : level_idx(level_idx),
        bits_per_element(bits_per_element),
        policy(rocksdb::NewBloomFilterPolicy(bits_per_element)) {}

    const size_t size() {return num_entries;}

} filter_meta_data;


class MonkeyFilterPolicy : public rocksdb::FilterPolicy
{
public:
    MonkeyFilterPolicy(double bits_per_element, std::vector<int> runs_per_level)
        : default_bpe(bits_per_element),
          policy(rocksdb::NewBloomFilterPolicy(bits_per_element)) {}

    const char *Name() const override {return "Monkey";}

    void CreateFilter(const rocksdb::Slice *keys, int n, std::string *dst) const override;

    bool KeyMayMatch(const rocksdb::Slice &key, const rocksdb::Slice &filter) const override;

    rocksdb::FilterBitsBuilder *GetBuilderWithContext(const rocksdb::FilterBuildingContext& context) const override;

    rocksdb::FilterBitsReader *GetFilterBitsReader(const rocksdb::Slice &contents) const override;

protected:
    double default_bpe;
    std::vector<std::vector<filter_meta_data>> filters_meta;
 
    const std::unique_ptr<const rocksdb::FilterPolicy> policy;
};

}

#endif /* ROBUST_BLOOMFILTERS_H_ */