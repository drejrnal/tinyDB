/**
 * lock_manager.cpp
 */
#include <algorithm>
#include "concurrency/lock_manager.h"

namespace cmudb {

    bool LockManager::LockShared(Transaction *txn, const RID &rid) {
        return LockTemplate(txn, rid, LockMode::SHARED);
    }

    bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
        return LockTemplate( txn, rid, LockMode::EXCLUSIVE );
    }

    bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
        return LockTemplate( txn, rid, LockMode::UPGRADING );
    }


    bool LockManager::LockTemplate(Transaction *txn, const RID &rid, LockMode lockMode) {
        /*
         * 验证两阶段提交：判断事务是否处于GROWING阶段
         * 若不满足2PL约束，则该事务应Abort
         */
        if (txn->GetState() != TransactionState::GROWING) {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }

        std::unique_lock<std::mutex> table_latch(mutex_);
        RequestQueue &requestQueue = lock_table_[rid];
        std::unique_lock<std::mutex> item_latch(requestQueue.mutex_);
        table_latch.unlock();

        if (lockMode == LockMode::UPGRADING) {
            /*
             * 等待队列中不能有其他请求是Upgrading状态
             * 如果该事务之前以LOCKMODE::SHARED加入请求队列，则从队列中删除
             * 找到处于shared状态的请求，从队列中删除，并在最后重新以Upgrade状态重新加入队列中
             */
            if (requestQueue.has_upgrading) {
                txn->SetState(TransactionState::ABORTED);
                return false;
            }
            auto it = std::find_if(requestQueue.req_queue_.begin(), requestQueue.req_queue_.end(),
                                   [txn](const Request &req) { return txn->GetTransactionId() == req.tid_; });
            if (it == requestQueue.req_queue_.end() || it->mode_ != LockMode::SHARED || !it->is_granted_) {
                txn->SetState(TransactionState::ABORTED);
                return false;
            }

            requestQueue.req_queue_.erase(it);
            txn->GetSharedLockSet()->erase(rid);
        }

        bool can_granted = requestQueue.canGranted(lockMode);
        requestQueue.insert_into_queue(txn, rid, lockMode, can_granted, &item_latch);
        return true;

    }


    bool LockManager::Unlock(Transaction *txn, const RID &rid) {
        //std::cout<<"Unlock request by trasaction: "<<txn->GetTransactionId()<<std::endl;

        if( strict_2PL_ ){
            //当txn对rid持有互斥锁的时候，事务只有在COMMIT或ABORT状态，unlock()操作成功;其余情况皆操作成功
            if( txn->GetExclusiveLockSet()->count( rid ) != 0 ){
                if( txn->GetState() != TransactionState::COMMITTED
                        && txn->GetState() != TransactionState::ABORTED ) {
                    return false;
                }
            }
        }

        if( txn->GetState() == TransactionState::GROWING )
            txn->SetState( TransactionState::SHRINKING );

        std::unique_lock<std::mutex> table_latch(mutex_);
        auto &request_queue = lock_table_[rid];
        std::unique_lock<std::mutex> item_latch(request_queue.mutex_);

        auto it = std::find_if(request_queue.req_queue_.begin(), request_queue.req_queue_.end(),
                               [txn](const Request &req) { return txn->GetTransactionId() == req.tid_; });
        //断言： 发出unlock message的事务必须在data item所在的请求队列中
        assert(it != request_queue.req_queue_.end());

        auto lockSet = (it->mode_ == LockMode::SHARED) ? txn->GetSharedLockSet() : txn->GetExclusiveLockSet();
        lockSet->erase(rid);
        request_queue.req_queue_.erase(it);
        //如果当前RID对应的请求队列为空，则从locktable中删除
        if (request_queue.req_queue_.empty()) {
            lock_table_.erase(rid);
            return true;
        }
        table_latch.unlock();

        auto &first_req = request_queue.req_queue_.front();

        /* 从队列头开始遍历，如果已是获取状态，则说明是SHARED模式，break循坏
         * 如果未获取，且获取到的状态是EXCLUSIVE 则break循坏
         * 如果未获取，且获取是SHARED, 则遍历直到某个request不是SHARED mode
         */
        if ( !first_req.is_granted_ && first_req.mode_ == LockMode::EXCLUSIVE )
            first_req.granted();
        else if ( !first_req.is_granted_ ) {
            for ( Request &ele : request_queue.req_queue_ ) {
                if ( ele.mode_ == LockMode::UPGRADING ) {
                    request_queue.has_upgrading = false;
                    ele.mode_ = LockMode::EXCLUSIVE;
                }
                if ( ele.mode_ == LockMode::EXCLUSIVE ) {
                    ele.granted();
                    break;
                }
                ele.granted();
            }
        }
        return true;
    }
} // namespace cmudb