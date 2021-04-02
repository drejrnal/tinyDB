/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *page, int index, BufferPoolManager *bufferPoolManager):
leafPage_(page), index_(index),bufferPoolManager_(bufferPoolManager)
{}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    bufferPoolManager_->UnpinPage(leafPage_->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
    return (leafPage_ == nullptr) ||
                        ((index_ == leafPage_->GetSize()) && (leafPage_->GetNextPageId() == INVALID_PAGE_ID));
}

INDEX_TEMPLATE_ARGUMENTS
const std::pair<KeyType, ValueType> & INDEXITERATOR_TYPE::operator*() {
    if (isEnd()){
        throw std::out_of_range("IndexIterator: out of range");
    }
    return leafPage_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
IndexIterator<KeyType, ValueType, KeyComparator> & INDEXITERATOR_TYPE::operator++() {
    index_++;
    if ((index_ == leafPage_->GetSize()) && (leafPage_->GetNextPageId() != INVALID_PAGE_ID) ) {
        int next_id = leafPage_->GetNextPageId();
        bufferPoolManager_->UnpinPage(leafPage_->GetPageId(), true);
        auto sibling_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>
                (bufferPoolManager_->FetchPage(next_id)->GetData());
        leafPage_ = sibling_page;
        index_ = 0;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
