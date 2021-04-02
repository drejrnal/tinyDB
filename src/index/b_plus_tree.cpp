/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>
#include <memory>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

    INDEX_TEMPLATE_ARGUMENTS
    BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                              BufferPoolManager *buffer_pool_manager,
                              const KeyComparator &comparator,
                              page_id_t root_page_id)
            : index_name_(name), root_page_id_(root_page_id),
              buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {

    }

/*
 * Helper function to decide whether current b+tree is empty
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::IsEmpty() const {
        return root_page_id_ == INVALID_PAGE_ID;
    }

    INDEX_TEMPLATE_ARGUMENTS
    thread_local int BPlusTree<KeyType, ValueType, KeyComparator>::rootLockedCnt = 0;
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::GetValue( const KeyType &key,
                                  std::vector<ValueType> &result,
                                  Transaction *transaction ) {
        auto leaf_page = FindLeafPage(key, OpType::READ, false, transaction);
        if ( leaf_page != nullptr ) {
            ValueType value;
            bool ret = leaf_page->Lookup(key, value, comparator_);
            if (ret) result.push_back(value);
            //buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
            ReleasePageInTransaction(false, transaction, leaf_page->GetPageId());
            return ret;
        }
        return false;
    }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                Transaction *transaction) {
        LockRootPageId(true);
        if (IsEmpty()) {
            StartNewTree(key, value);
            TryUnlockRootPageId(true);
            return true;
        } else {
            TryUnlockRootPageId(true);
            return InsertIntoLeaf(key, value, transaction);
        }
    }
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
        Page *new_page = buffer_pool_manager_->NewPage(root_page_id_); //树的根节点page_id由buffer_pool_manager分配
        if (new_page == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX, "no free pages to allocate");
        auto root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(new_page->GetData());

        root->Init(root_page_id_, INVALID_PAGE_ID); //根节点父节点无效
        root->Insert(key, value, comparator_);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                        Transaction *transaction) {

        B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key, OpType::INSERT, false, transaction);
        ValueType old_value;
        if ( leaf_page->Lookup(key, old_value, comparator_) ) {
            ReleasePageInTransaction(true, transaction);
            return false;
        }
        /*
         * 此时leaf_page持有锁
         */
        leaf_page->Insert(key, value, comparator_);
        if ( leaf_page->GetSize() > leaf_page->GetMaxSize() ) {
            /*
             * leaf_page及其父节点均持有写锁
             */
            auto alloc_page =
                    Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> >(leaf_page, transaction);
            InsertIntoParent(leaf_page,
                             alloc_page->KeyAt(0), alloc_page, transaction);
        }
        ReleasePageInTransaction(true, transaction);
        return true;
    }


/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
        page_id_t alloc_page_id;
        auto alloc_page = buffer_pool_manager_->NewPage(alloc_page_id);
        if (alloc_page == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX, "no free pages to allocate");

        /*
         * for concurrency control and transaction management
         * 非lab2组成部分
         */
        alloc_page->WLatch();
        transaction->AddIntoPageSet(alloc_page);


        N *index_page = reinterpret_cast<N *>(alloc_page->GetData());
        index_page->Init(alloc_page_id);
        node->MoveHalfTo(index_page, buffer_pool_manager_);

        return index_page;
    }

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key           new_node->array[0].first
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                          const KeyType &key,
                                          BPlusTreePage *new_node,
                                          Transaction *transaction) {
        if (old_node->IsRootPage()) {
            page_id_t root_id;
            auto new_page = buffer_pool_manager_->NewPage(root_id);
            auto root_page =
                    reinterpret_cast< BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_page->GetData());
            root_page->Init(root_id);
            root_page_id_ = root_id;
            old_node->SetParentPageId(root_id);
            new_node->SetParentPageId(root_id);
            root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
            UpdateRootPageId(false);
            buffer_pool_manager_->UnpinPage(root_page_id_, true);
            return;
        }
        else {
            auto page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
            auto parent_page =
                    reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
            /*
             * 由于每页预留有空间放置额外的键值对，因此先执行插入操作，
             * 在执行分裂，注意递归过程中页的Unpin操作
             * 最终函数返回到InsertIntoLeafPage函数时，注意执行unpin leaf page操作
             */
            parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
            new_node->SetParentPageId(parent_page->GetPageId());
            if (parent_page->GetSize() > parent_page->GetMaxSize()) {
                auto alloc_internal_page =
                        Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> >(parent_page, transaction);
                InsertIntoParent(parent_page, alloc_internal_page->KeyAt(0),
                                 alloc_internal_page, transaction);
            }
        }

    }
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
        /*if (IsEmpty()) {
            return;
        }*/
        auto leaf_page = FindLeafPage(key, OpType::DELETE, false, transaction);
        leaf_page->RemoveAndDeleteRecord(key, comparator_);
        if ( leaf_page->GetSize() < leaf_page->GetMinSize() )
            CoalesceOrRedistribute(leaf_page, transaction);
        ReleasePageInTransaction(true, transaction);

    }

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
        if (node->IsRootPage()) {
            bool ret = AdjustRoot(node);
            if (ret) transaction->AddIntoDeletedPageSet(node->GetPageId());
            return ret;
        } else {
            int parent_id = node->GetParentPageId();
            auto page = reinterpret_cast
                    <BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>
            (buffer_pool_manager_->FetchPage(parent_id)->GetData());
            int index = page->ValueIndex(node->GetPageId());
            assert(index >= 0 && index < page->GetSize());
            int sibling_index;
            if (index == 0)
                sibling_index = index + 1;
            else
                sibling_index = index - 1;
            auto sibling_page = reinterpret_cast<N*>(
                    LockCrabbingIter(page->ValueAt(sibling_index),OpType::DELETE,-1,transaction));

            if (sibling_page->GetSize() + node->GetSize() <= sibling_page->GetMaxSize()) {
                //coalesce
                if (index == 0) {
                    Coalesce(node, sibling_page, page, index + 1, transaction);
                } else {
                    Coalesce(sibling_page, node, page, index, transaction);
                }
                buffer_pool_manager_->UnpinPage(parent_id, true);
                return true;
            } else {
                Redistribute(sibling_page, node, index);
                buffer_pool_manager_->UnpinPage(parent_id, true);
                return false;
            }
        }
    }

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    bool BPLUSTREE_TYPE::Coalesce(
            N *&neighbor_node, N *&node,
            BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
            int index, Transaction *transaction) {

        node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
        transaction->AddIntoDeletedPageSet(node->GetPageId());
        parent->Remove(index);
        if (parent->GetSize() < parent->GetMinSize())
            return CoalesceOrRedistribute(parent, transaction);
        return false;
    }

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
        if (index == 0)
            neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
        else
            neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
    }
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
        if (old_root_node->IsLeafPage()) {
            int num = old_root_node->GetSize();
            if (num == 0) {
                root_page_id_ = INVALID_PAGE_ID;
                UpdateRootPageId(false);
                return true;
            }
            return false;
        } else {
            if (old_root_node->GetSize() == 1) {
                auto old_root_page = reinterpret_cast
                        <BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>
                (old_root_node);

                page_id_t child_pageId = old_root_page->ValueAt(0);
                root_page_id_ = child_pageId;

                auto child_page = buffer_pool_manager_->FetchPage(child_pageId);
                auto child_page_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
                child_page_node->SetParentPageId(INVALID_PAGE_ID);
                UpdateRootPageId(false);

                buffer_pool_manager_->UnpinPage(child_pageId, true);
                return true;
            }
        }
        return false;
    }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
        auto leaf_page = FindLeafPage(KeyType(), OpType::READ, true, nullptr);
        TryUnlockRootPageId(false);
        return INDEXITERATOR_TYPE(leaf_page, 0, buffer_pool_manager_);
    }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
        auto leaf_page = FindLeafPage(key, OpType::READ, false, nullptr);
        TryUnlockRootPageId(false);
        int index = leaf_page->KeyIndex(key, comparator_);
        return INDEXITERATOR_TYPE(leaf_page, index, buffer_pool_manager_);
    }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
    INDEX_TEMPLATE_ARGUMENTS
    B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, OpType opType,
                                                             bool leftMost, Transaction *transaction) {
        bool exclusive = (opType != OpType::READ);
        LockRootPageId(exclusive);
        if (IsEmpty()) {
            TryUnlockRootPageId(exclusive);
            return nullptr;
        }
        /*auto page = buffer_pool_manager_->FetchPage(root_page_id_);
        BPlusTreePage *traverse = reinterpret_cast<BPlusTreePage *>(page->GetData());*/
        auto traverse = LockCrabbingIter(root_page_id_, opType, -1, transaction);
        page_id_t previous_page_id = root_page_id_;
        while (!traverse->IsLeafPage()) {
            auto internal
                    = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * >(traverse);
            page_id_t child_page_id;
            if (!leftMost)
                child_page_id = internal->Lookup(key, comparator_);
            else
                child_page_id = internal->ValueAt(0);
            traverse = LockCrabbingIter(child_page_id, opType, previous_page_id, transaction);
            previous_page_id = child_page_id;
        }
        return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(traverse);
    }

    INDEX_TEMPLATE_ARGUMENTS
    BPlusTreePage *
    BPLUSTREE_TYPE::LockCrabbingIter(page_id_t pageId, OpType opType, page_id_t parent, Transaction *transaction) {
        bool exclusive = (opType != OpType::READ);
        auto page = buffer_pool_manager_->FetchPage(pageId);
        Lock(exclusive, page);

        auto treePage = reinterpret_cast<BPlusTreePage *>(page->GetData());

        /*
         * TODO:: 感觉有bug
         * if oPType == OPTYPE::DELETE
         *      then 如果兄弟节点（treePage）是safe的，说明兄弟节点可以移动一个节点到待删除元素所在节点,但即便如此
         *      也牵涉到父节点，两兄弟节点的改变，因此，这三个节点应继续持有写锁，父节点以上的所有父节点释放写锁
         *           如果兄弟节点也不满足safe，则兄弟节点会与待删除元素所在节点合并，父节点进而进行删除操作，所以
         *           会继续持有任何父节点的写锁
         */
        if (parent > 0 && (!exclusive || treePage->IsSafe(opType)))
            ReleasePageInTransaction(exclusive, transaction, parent);
            //TODO::根据注释，该是ReleasePageInTransaction(exclusive, transaction, parent.parent);
        if (transaction != nullptr)
            transaction->AddIntoPageSet(page);
        return treePage;

    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::ReleasePageInTransaction(bool exclusive, Transaction *transaction,
                                                  page_id_t parent) {
        TryUnlockRootPageId(exclusive);
        if (transaction == nullptr) {
            Unlock(false, parent);
            buffer_pool_manager_->UnpinPage(parent, true);
            return;
        }
        for (Page *page: *(transaction->GetPageSet())) {
            page_id_t pageId = page->GetPageId();
            Unlock(exclusive, page);
            buffer_pool_manager_->UnpinPage(pageId, true);
            if (transaction->GetDeletedPageSet()->find(pageId) !=
                transaction->GetDeletedPageSet()->end()) {
                buffer_pool_manager_->DeletePage(pageId);
                transaction->GetDeletedPageSet()->erase(pageId);
            }
        }
        transaction->GetPageSet()->clear();
    }
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
        HeaderPage *header_page = static_cast<HeaderPage *>(
                buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
        if (insert_record)
            // create a new record<index_name + root_page_id> in header_page
            header_page->InsertRecord(index_name_, root_page_id_);
        else
            // update root_page_id in header_page
            header_page->UpdateRecord(index_name_, root_page_id_);
        buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
    }

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
    INDEX_TEMPLATE_ARGUMENTS
    std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                        Transaction *transaction) {
        int64_t key;
        std::ifstream input(file_name);
        while (input) {
            input >> key;

            KeyType index_key;
            index_key.SetFromInteger(key);
            RID rid(key);
            Insert(index_key, rid, transaction);
        }
    }
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                        Transaction *transaction) {
        int64_t key;
        std::ifstream input(file_name);
        while (input) {
            input >> key;
            KeyType index_key;
            index_key.SetFromInteger(key);
            Remove(index_key, transaction);
        }
    }

    template
    class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

    template
    class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

    template
    class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

    template
    class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

    template
    class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
