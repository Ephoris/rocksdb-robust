#ifndef ROBUST_BLOOMFILTERS_H_
#define ROBUST_BLOOMFILTERS_H_

#include <iostream>
#include <vector>

#include "common/debug.hpp"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice.h"

#define EULER 2.71828182845904523536
#define LOG2SQUARED 0.48045301391

namespace monkey {

class MonkeyFilterPolicy : public rocksdb::FilterPolicy
{
public:
    MonkeyFilterPolicy(double bits_per_element) {this->default_bpe = bits_per_element;};

//     const char * Name() { return monkey_policy->Name(); };

//     void CreateFilter( const rocksdb::Slice * keys, int n, std::string * dst );

//     bool KeyMayMatch( const rocksdb::Slice & key, const rocksdb::Slice & filter );

//     rocksdb::FilterBitsBuilder * GetFilterBitsBuilder();

//     rocksdb::FilterBitsReader * GetFilterBitsReader( const rocksdb::Slice & contents );

// protected:
//     const std::unique_ptr<const FilterPolicy> monkey_policy;
protected:
    double default_bpe;
};

}

#endif /* ROBUST_BLOOMFILTERS_H_ */