#include <list>
#include <functional>
#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each Bucket
 */
    template<typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash(size_t size):global_depth_(0), bucket_size_(size) {
        address_table_.emplace_back(new Bucket(0));
        bucket_count_ = 1;
    }

/*
 * helper function to calculate the hashing address of input key
 */
    template<typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K &key) {
        return std::hash<K>()(key);
    }

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const {

        return global_depth_;
    }

/*
 * helper function to return local depth of one specific Bucket
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
        if (address_table_[bucket_id])
            return address_table_[bucket_id]->local_depth;
        return -1;
    }

/*
 * helper function to return current number of Bucket in hash table
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetNumBuckets() const {
        return bucket_count_;
    }

/*
 * lookup function to find value associate with input key
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
        //int global_depth = GetGlobalDepth();
        std::lock_guard<std::mutex> lock(mutex_);
        size_t slot = key_index(key, global_depth_);
        auto bucket = address_table_[slot];
        if (bucket) {
            if (bucket->records.find(key) != bucket->records.end()) {
                value = bucket->records[key];
                return true;
            }
        }
        return false;
    }

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key) {
        //int global_depth = GetGlobalDepth();
        std::lock_guard<std::mutex> lock(mutex_);
        size_t slot = key_index(key, global_depth_);

        auto bucket = address_table_[slot];

        if (bucket) {
            auto rec = bucket->records.find(key);
            if (rec == bucket->records.end())
                return false;
            bucket->records.erase(rec);
            return true;
        }
        return false;
    }

/*
 * calculate the first slot of page table
 * where the pointer corresponding to Bucket of key stores
 */
    template<typename K, typename V>
    size_t ExtendibleHash<K, V>::key_index(const K &key, int local_depth) {
        return HashKey(key) & ((1 << local_depth) - 1);
    }

/*
 * split bucket
 */
    template<typename K, typename V>
    std::shared_ptr<typename ExtendibleHash<K, V>::Bucket>
    ExtendibleHash<K, V>::split(std::shared_ptr<Bucket> &bucket) {
        int depth = bucket->local_depth;
        auto new_bucket = std::make_shared<Bucket>(depth);
        while (new_bucket->records.empty()) {

            bucket->local_depth++;
            new_bucket->local_depth++;
            for (auto it = bucket->records.begin(); it != bucket->records.end();) {
                K key = it->first;
                /*?????? ??????bucket->local_depth????????????????????????????????????????????????0???
                 * ?????????1????????????bucket???????????????hash?????????1bit???0??????1????????????
                 * ????????????bit???0??????1??? & ( 1<< (bucket->local_depth - 1) )
                 */
                if (HashKey(key) & (1 << (bucket->local_depth - 1))) {
                    new_bucket->records.insert(*it);
                    it = bucket->records.erase(it);
                } else
                    it++;
            }
            if (bucket->records.empty()) {
                std::swap(bucket->records, new_bucket->records);
            }
        }
        ++bucket_count_;
        return new_bucket;
    }

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute Bucket when there is overflow and if necessary increase
 * global depth
 */
    template<typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
        std::lock_guard<std::mutex> lock(mutex_);

        size_t slot = key_index(key, global_depth_);

        if (!address_table_[slot]) {
            address_table_[slot] = std::make_shared<Bucket>(global_depth_);
            bucket_count_++;
        }
        auto bucket = address_table_[slot];
        if (bucket->records.find(key) != bucket->records.end()) {
            bucket->records[key] = value;
            return;
        }
        bucket->records.insert({key, value});
        /*
         * ??????records???????????????????????? ??????split ???redistribute bucket?????????
         */
        if (bucket->records.size() > bucket_size_) {
            int old_depth = bucket->local_depth;
            std::shared_ptr<Bucket> new_bucket = split(bucket);
            /*
             * ????????????bucket??????local_depth?????????global_depth
             * ??????????????????????????????bucket???slot??????????????????slot??????new_bucket
            */
            int local_depth = bucket->local_depth;

            K bucket_key = bucket->records.begin()->first;
            K newbucket_key = new_bucket->records.begin()->first;
            size_t bucket_index = key_index(bucket_key, local_depth);
            size_t newbucket_index = key_index(newbucket_key, local_depth);

            /* ?????????local_depth???????????????
             * if local_depth == global_depth
             *      then bucket???newbucket ????????????address_table_??????slot
             * if local depth < global depth
             *      then hash table?????????bucket????????????slot????????????????????????
             *           ????????????bucket???slot???????????????????????????bucket??????????????????new_bucket
             */
            if (local_depth <= global_depth_) {
                int stride = 1 << local_depth;
                //assert(bucket->local_depth == new_bucket->local_depth)
                //assert(old_index == bucket_index || old_index == newbucket_index)
                for (size_t i = bucket_index; i < address_table_.size(); i += stride) {
                    address_table_[i] = bucket;
                }
                for (size_t i = newbucket_index; i < address_table_.size(); i += stride) {
                    address_table_[i] = new_bucket;
                }
            }
                /*
                 * local_depth > global_depth ?????????address_table_?????????
                 * ??????bucket???local_depth???address_table_???global_depth??????
                 * ??? address_table_????????????slot??????????????????bucket ??? new_bucket
                 * ??????????????????????????????????????????????????????????????????????????????
                 */
            else {
                int factor = 1 << (local_depth - global_depth_);
                size_t old_size = address_table_.size();
                address_table_.resize(old_size * factor);

                global_depth_ = local_depth;
                address_table_[bucket_index] = bucket;
                address_table_[newbucket_index] = new_bucket;
                int stride = 1 << old_depth;
                /*
                 * ??????resize??????address_table_??????bucket???newbucket???shared_ptr
                 * ????????????????????????????????????
                 *      ????????????address_table->local_dpeth == global_depth, ???????????????bucket_index/newbucket_index
                 *      ???????????? >= old_size
                 * ??????reset()?????????new_bucket???bucke
                 * ????????????address_table[bucket_index]???address_table[newbucket_index]
                 */
                for (size_t k = bucket_index; k < old_size; k += stride) {
                    bucket.reset();
                }
                for (size_t k = newbucket_index; k < old_size; k += stride) {
                    new_bucket.reset();
                }

                /*
                 * ??????local_depth < global_depth???bucket???address_table_???slot????????????????????????????????????address_table_???
                 * ???????????????slot?????????bucket
                 * local_depth == global_depth???bucket?????????????????????????????????????????????
                 *     for( size_t j = ... ; j < adress_table_.size(); .... )??????
                 */
                for (size_t i = 0; i < old_size; i++) {
                    if (address_table_[i]) {
                        int tmp_step = 1 << address_table_[i]->local_depth;
                        for (size_t j = i + tmp_step; j < address_table_.size(); j += tmp_step) {
                            if (!address_table_[j])
                                address_table_[j] = address_table_[i];
                        }
                    }
                }

            }// end if (local_depth <= global_depth) {} else {}
        }//end if (bucket->records.size() > bucket_size_)
    }

/*
 * class specification
 */
    template
    class ExtendibleHash<page_id_t, Page *>;

    template
    class ExtendibleHash<Page *, std::list<Page *>::iterator>;

// test purpose
    template
    class ExtendibleHash<int, std::string>;

    template
    class ExtendibleHash<int, std::list<int>::iterator>;

    template
    class ExtendibleHash<int, int>;
} // namespace cmudb
