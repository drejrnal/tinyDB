/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include "hash/hash_table.h"

namespace cmudb {

    template<typename K, typename V>
    class ExtendibleHash : public HashTable<K, V> {
        struct Bucket {
            explicit Bucket(int depth): local_depth(depth){};
            std::map<K, V> records; //通过records.size可知当前bucket内部大小
            int local_depth; //hash(key) & (1 << local_depth -1)得hash table中对应该bucket
        };
    public:
        // constructor
        ExtendibleHash(size_t size);

        // helper function to generate hash addressing
        size_t HashKey(const K &key);

        // helper function to get global & local depth
        int GetGlobalDepth() const;

        int GetLocalDepth(int bucket_id) const;

        int GetNumBuckets() const;

        // lookup and modifier
        bool Find(const K &key, V &value) override;

        bool Remove(const K &key) override;

        void Insert(const K &key, const V &value) override;

        std::vector<std::shared_ptr<Bucket>> address_table_;
    private:
        // add your own member variables here

        std::mutex mutex_;
        int global_depth_; //用哈希值的后global_depth位做索引值，间接表明page table的大小
        int bucket_count_; //bucket的数量
        size_t bucket_size_; //每个bucket内部capacity
        size_t key_index(const K & key, int local_depth);
        std::shared_ptr<Bucket> split(std::shared_ptr<Bucket>& bucket);
    };
} // namespace cmudb
