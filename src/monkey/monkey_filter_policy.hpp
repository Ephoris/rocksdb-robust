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

class MonkeyFilterPolicy : public rocksdb::FilterPolicy
{
public:
    MonkeyFilterPolicy(double bits_per_element, int size_ratio, size_t levels)
        : default_bpe(bits_per_element),
          size_ratio(size_ratio),
          levels(levels),
          defualt_policy(rocksdb::NewBloomFilterPolicy(bits_per_element)) {}

    const char *Name() const override {return "Monkey";}

    void CreateFilter(const rocksdb::Slice *keys, int n, std::string *dst) const override;

    bool KeyMayMatch(const rocksdb::Slice &key, const rocksdb::Slice &filter) const override;

    double optimal_false_positive_rate(size_t curr_level);

    rocksdb::FilterBitsBuilder *GetBuilderWithContext(const rocksdb::FilterBuildingContext& context) const override;

    rocksdb::FilterBitsReader *GetFilterBitsReader(const rocksdb::Slice &contents) const override;

protected:
    double default_bpe;
    int size_ratio;
    size_t levels;

    std::vector<double> level_fpr_opt;
    std::vector<double> level_bpe;

    const std::unique_ptr<const rocksdb::FilterPolicy> default_policy;
};

}

#endif /* ROBUST_BLOOMFILTERS_H_ */