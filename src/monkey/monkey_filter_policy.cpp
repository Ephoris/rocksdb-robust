#include "monkey/monkey_filter_policy.hpp"

using namespace monkey;

// void MonkeyFilterPolicy::CreateFilter( const rocksdb::Slice * keys, int n, std::string * dst )
// {
//     monkey_policy->CreateFilter( keys, n, dst );
// }

// bool MonkeyFilterPolicy::KeyMayMatch( const rocksdb::Slice & key, const rocksdb::Slice & filter )
// {
//     return true;
// }

// rocksdb::FilterBitsBuilder * MonkeyFilterPolicy::GetFilterBitsBuilder()
// {
//     return this->monkey_policy->GetFilterBitsBuilder();
// }

// rocksdb::FilterBitsReader * MonkeyFilterPolicy::GetFilterBitsReader( const rocksdb::Slice & contents )
// {
//     return this->monkey_policy->GetFilterBitsReader( contents );
// }