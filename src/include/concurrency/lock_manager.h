/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {
    enum class LockMode {
        SHARED = 0, UPGRADING, EXCLUSIVE
    };

    struct Request {

        //Request(Request &) = delete;

        Request(txn_id_t tid, LockMode mode, bool granted) : tid_(tid),
                                                             mode_(mode), is_granted_(granted) { ; }

        void wait() {
            std::unique_lock<std::mutex> lk(cv_m);
            cv_.wait(lk, [this] { return this->is_granted_; });
        }

        void granted() {
            std::unique_lock<std::mutex> lk(cv_m);
            this->is_granted_ = true;
            cv_.notify_one();
        }

        txn_id_t tid_;
        LockMode mode_;
        bool is_granted_;
        std::condition_variable cv_;
        std::mutex cv_m;

    };

    struct RequestQueue {

        //RequestQueue(RequestQueue &) = delete;

        bool canGranted(LockMode lockMode) {
            /*
            if ( req_queue_.empty() )
                return true;
            auto &last_request = req_queue_.back();
            if (last_request.is_granted_)
                return lockMode == LockMode::SHARED && last_request.mode_ == LockMode::SHARED;
            return false;*/

            if( req_queue_.empty() )
                return true;
            Request &request = req_queue_.back();
            if( request.is_granted_ ){
                return request.mode_ == LockMode::SHARED && lockMode == LockMode::SHARED;
            }
            return false;
        }

        void insert_into_queue(Transaction *txn, const RID &rid, LockMode lockMode, bool granted,
                               std::unique_lock<std::mutex> *lock) {

            req_queue_.emplace_back(txn->GetTransactionId(), lockMode, granted );
            Request &request = req_queue_.back();
            if (!request.is_granted_) {
                lock->unlock();
                request.wait();
            }
            if (lockMode == LockMode::SHARED)
                txn->GetSharedLockSet()->insert(rid);
            else
                txn->GetExclusiveLockSet()->insert(rid);
        }

        bool has_upgrading;
        std::list<Request> req_queue_;
        std::mutex mutex_;
    };

    class LockManager {

    public:
        /*
         * Strict 2PL: 保证如果事务持有互斥锁，则直到事务提交才释放该锁
         */
        LockManager(bool strict_2PL) : strict_2PL_(strict_2PL) {};

        /*** below are APIs need to implement ***/
        // lock:
        // return false if transaction is aborted
        // it should be blocked on waiting and should return true when granted
        // note the behavior of trying to lock locked rids by same txn is undefined
        // it is transaction's job to keep track of its current locks
        bool LockShared(Transaction *txn, const RID &rid);

        bool LockExclusive(Transaction *txn, const RID &rid);

        bool LockUpgrade(Transaction *txn, const RID &rid);

        bool LockTemplate(Transaction *txn, const RID &rid, LockMode lockMode);

        // unlock:
        // release the lock hold by the txn
        bool Unlock(Transaction *txn, const RID &rid);
        /*** END OF APIs ***/

    private:
        std::unordered_map< RID, RequestQueue > lock_table_;
        std::mutex mutex_;
        bool strict_2PL_;
    };

} // namespace cmudb
