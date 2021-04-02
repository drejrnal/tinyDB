/**
 * LRU implementation
 */
#include <cassert>
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

    template<typename T>
    LRUReplacer<T>::LRUReplacer() {}

    template<typename T>
    LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert @value into LRU
 * 首先根据 @value 查看page_list_directory看buffer pool中是否已经存在该 @value
 * 若存在，则改变该页指针在page pointer list中的位置，erase->push_front
 * 若不存在，直接调用push_front
 * 更新directory，插入的值对应的pointer永远是begin，表示最近访问的页面的指针
 */
    template<typename T>
    void LRUReplacer<T>::Insert(const T &value) {
        auto pos = page_list_directory_.find(value); //return pair<T, list<T>::iterator>
        if (pos != page_list_directory_.end()) {
            pages_pointer_.erase(pos->second);
        }
        pages_pointer_.push_front(value);
        page_list_directory_.insert({value, pages_pointer_.begin()});
    }

/* If LRU is non-empty, pop the tail member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
    template<typename T>
    bool LRUReplacer<T>::Victim(T &value) {
        if (pages_pointer_.empty())
            return false;
        auto tail = pages_pointer_.end();
        value = *(std::prev(tail, 1));
        //pages_pointer_.erase(std::prev(tail,1));
        pages_pointer_.pop_back();
        page_list_directory_.erase(value);
        return true;
    }

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false: directory中没有该页指针对应的元素
 *
 */
    template<typename T>
    bool LRUReplacer<T>::Erase(const T &value) {
        if (page_list_directory_.find(value) == page_list_directory_.end())
            return false;
        auto pos = page_list_directory_.find(value);
        page_list_directory_.erase(pos);
        pages_pointer_.erase(pos->second);
        return true;
    }

    template<typename T>
    size_t LRUReplacer<T>::Size() { return pages_pointer_.size(); }

    template
    class LRUReplacer<Page *>;

// test only
    template
    class LRUReplacer<int>;

} // namespace cmudb
