/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
    SetParentPageId(parent_id);
    SetPageId(page_id);
    SetMaxSize( ( (PAGE_SIZE - sizeof(B_PLUS_TREE_INTERNAL_PAGE_TYPE) )
                          / sizeof(MappingType) ) - 2 );
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key = array[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    for (int i = 0; i < GetSize(); ++i) {
        if (array[i].second == value)
            return i;
    }
  return GetSize();
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
        array[index].second = value;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const
{ return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {

    int low =1, high =GetSize()-1;
    while(low <= high){
        int mid = low + (high - low)/2;
        if(comparator(key, array[mid].first) > 0)
            low = mid+1;
        else if(comparator(key, array[mid].first) < 0)
            high = mid - 1;
        else{
            return array[mid].second;
        }
    }

    return array[high].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    SetValueAt(0, old_value);

    SetKeyAt(1, new_key);
    SetValueAt(1,new_value);
    IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    int position = ValueIndex(old_value);
    if (position >= 0){
        /*int size = GetSize();
        for (int i = size-1; i >= position ; --i) {
            array[i+1] = array[i];
        }
        array[position] = {new_key, new_value};*/
        std::copy_backward(array+position+1, array+GetSize(), array+GetSize()+1);
        array[position+1] = {new_key, new_value};
        IncreaseSize(1);
    }
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    int half_size =(int) GetSize() /2;
    //将this所指的page的后一半mappingType送至recipient中
    recipient->CopyHalfFrom(array+half_size, GetSize() - half_size, buffer_pool_manager);
    for(int i = half_size; i < GetSize(); i++){
        auto page = buffer_pool_manager->FetchPage(array[i].second);
        auto child_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
        child_page->SetParentPageId(recipient->GetPageId());
        buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
    }
    IncreaseSize(-1 * (GetSize() - half_size ));
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    std::copy(items, items+size, array);
    IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    std::copy(array+index+1,array+GetSize(), array+index);
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {

  return INVALID_PAGE_ID;

}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
    auto parent_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>
                        (buffer_pool_manager->FetchPage(GetParentPageId())->GetData());

    for(int i = 0; i< GetSize(); i++){
        auto page = buffer_pool_manager->FetchPage(array[i].second);
        auto child_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
        child_page->SetParentPageId(recipient->GetPageId());
        buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
    }


    int prior_size = recipient->GetSize();
    /*
     * *this 页的内容拷贝进recipient内部
     * 首先考虑特殊情况：不能将将*this->array[0].first(invalide)直接拷贝到recipient中，而是将
     * 小于*this->array[0].second的关键字拷贝（该key在parent_page的 @index_in_parent 处）
     * 然后，将 [*this->array + 1， *this->array + *this.getSize() )拷贝进recipient->array内
     */
    recipient->SetKeyAt(prior_size, parent_page->KeyAt(index_in_parent));
    recipient->SetValueAt(prior_size, ValueAt(0));
    recipient->IncreaseSize(1); //注意别忘了更新

    recipient->CopyAllFrom(array+1, GetSize()-1, buffer_pool_manager);
    SetSize(0);
    buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    std::copy(items, items+size, array+GetSize());
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 * redistribute 涉及两节点间元素移动，下面两种分别适用最左子节点和其他位置的子节点
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {

    MappingType move_pair = { array[1].first, array[0].second };
    page_id_t child_id = array[0].second;
    SetValueAt(0, ValueAt(1));
    std::copy(array+2, array+GetSize(), array+1);//当前page元素均向左移动一格
    /*
     * 更新recipient page的信息
     */
    recipient->CopyLastFrom(move_pair, buffer_pool_manager);
    auto child_page = reinterpret_cast<BPlusTreePage *>
                (buffer_pool_manager->FetchPage(child_id)->GetData());
    child_page->SetParentPageId(recipient->GetPageId());
    IncreaseSize(-1);
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {

    /*
     * @pair.first移动到父节点的index处的key，index处的value不变
     */
    auto parent_page =reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>
                        (buffer_pool_manager->FetchPage(GetParentPageId())->GetData());
    int index = parent_page->ValueIndex(GetPageId());
    assert(index == 0); //说明moveFirstToEndOf仅限于最左子节点同其右侧兄弟节点间元素移动
    IncreaseSize(1);
    //更新 recipient 节点的末端
    SetKeyAt(GetSize()-1, parent_page->KeyAt(index+1));
    SetValueAt(GetSize()-1, pair.second);
    //更新 parent_page
    parent_page->SetKeyAt(1, pair.first);

    buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);

}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    auto parent_page =reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>
                    (buffer_pool_manager->FetchPage(GetParentPageId())->GetData());
    MappingType moved_pair = {parent_page->KeyAt(parent_index), ValueAt(GetSize()-1)};
    parent_page->SetKeyAt(parent_index, KeyAt(GetSize()-1));
    recipient->CopyFirstFrom(moved_pair, parent_index, buffer_pool_manager);
    IncreaseSize(-1);

    buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {

    std::copy_backward(array+1, array+GetSize(), array+GetSize()+1);
    SetKeyAt(1, pair.first);
    SetValueAt(1, ValueAt(0));
    SetValueAt(0, pair.second);
    auto child_page =reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>
            (buffer_pool_manager->FetchPage(pair.second)->GetData());
    child_page->SetParentPageId(GetPageId());
    IncreaseSize(1);

    buffer_pool_manager->UnpinPage(pair.second, true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace cmudb
