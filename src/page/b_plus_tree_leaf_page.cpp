/*
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>
#include <include/page/b_plus_tree_internal_page.h>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetSize(0);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetNextPageId(INVALID_PAGE_ID);
    SetMaxSize(((PAGE_SIZE - sizeof(B_PLUS_TREE_LEAF_PAGE_TYPE) )
                                / sizeof(MappingType)) - 2 );
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找 确定key在数组中的位置pos array[pos]>=key
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
    int low = 0, high = GetSize()-1;
    while(low <= high){
        int mid = low + (high - low)/2;
        int flag = comparator(key, array[mid].first);
        if(flag > 0){
            low = mid+1;
        }else if(flag < 0){
            high = mid-1;
        } else
            return mid;
    }
    return low;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key = array[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}


/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
    int pos = KeyIndex(key, comparator);
    std::copy_backward(array+pos, array+GetSize(), array+GetSize()+1);
    array[pos] = {key, value};
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    int half_size = GetSize()/2;
    recipient->CopyHalfFrom(array+half_size, GetSize()-half_size);
    recipient->SetNextPageId(GetNextPageId());
    SetNextPageId(recipient->GetPageId());
    IncreaseSize( -1 * (GetSize() - half_size) );
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
    assert(GetSize() == 0);
    std::copy(items, items+size, array);
    IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
    int pos = KeyIndex(key, comparator);
    if (pos < GetSize() && comparator(key, array[pos].first) == 0){
        value = array[pos].second;
        return true;
    }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
    int index = KeyIndex(key, comparator);
    /*
     * 下述代码刚开始并未加入"判断待删除元素是否仍然在页中"的逻辑( 即 index < GetSize() )
     * 直到并发删除的测试失败，发现了这个bug
     */
    if(index < GetSize() && (comparator(key, array[index].first) == 0) ) {
        std::copy(array + index + 1, array + GetSize(), array + index);
        IncreaseSize(-1);
    }
    return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int index_in_parent, BufferPoolManager *buffer_pool_manager) {
    auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),KeyComparator> *>
                        (buffer_pool_manager->FetchPage(GetParentPageId())->GetData());
    recipient->CopyAllFrom(array, GetSize());
    recipient->SetNextPageId(GetNextPageId());

    SetNextPageId(INVALID_PAGE_ID);
    SetSize(0);


    buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
    std::copy(items, items+size, array+GetSize());
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {

    MappingType move_pair = { array[0].first, array[0].second };
    std::copy(array+1, array+GetSize(), array);//当前page元素均向左移动一格
    /*
     * 更新recipient page的信息
     */
    recipient->CopyLastFrom(move_pair);
    IncreaseSize(-1);

    auto parent_page = reinterpret_cast< BPlusTreeInternalPage<KeyType, decltype(GetPageId()),KeyComparator> *>
                             (buffer_pool_manager->FetchPage(GetParentPageId())->GetData());
    assert(parent_page->ValueIndex(GetPageId()) == 1);
    parent_page->SetKeyAt(1, KeyAt(0));
    buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {

    array[GetSize()] = item;
    IncreaseSize(1);

}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {

    MappingType move_pair = { array[GetSize()-1].first, array[GetSize()-1].second };

    /*
     * 更新recipient page的信息
     */
    recipient->CopyFirstFrom(move_pair, parentIndex, buffer_pool_manager);
    IncreaseSize(-1);


}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {

    std::copy_backward(array, array+GetSize(), array+GetSize()+1);//当前page元素均向右移动一格
    array[0] = item;
    IncreaseSize(1);

    auto parent_page = reinterpret_cast< BPlusTreeInternalPage<KeyType, decltype(GetPageId()),KeyComparator> *>
                         (buffer_pool_manager->FetchPage(GetParentPageId())->GetData());
    parent_page->SetKeyAt(parentIndex, item.first);

    buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
