//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager_v2.h"

#include "common/config.h"
#include "concurrency/transaction_2pl.h"
#include "concurrency/transaction_manager.h"

#include "common/logger.h"

namespace cmudb {

// helper function to determine whether current transaction can take the lock based on the txn isolation level, throw exception if not allowed to grant lock
    void LockManagerV2::ValidateLockMode(TransactionWith2PL *txn, LockMode lock_mode) {
        if (txn->GetIsolationLevel() == IsolationLevelWith2PL::READ_UNCOMMITTED &&
            ((lock_mode == LockMode::SHARED) || (lock_mode == LockMode::INTENTION_SHARED))) {
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
        }
        if (txn->GetState() == TransactionStateWith2PL::SHRINKING) {
            if ((txn->GetIsolationLevel() == IsolationLevelWith2PL::READ_COMMITTED) &&
                (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED)) {
                return;
            }
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
    }

    auto LockManagerV2::ValidateLock(LockRequestQueue *lock_queue, LockMode lock_mode) -> bool {
        for (const auto &traverse_lock_request: lock_queue->request_queue_) {
            if (traverse_lock_request->granted_ &&
                (lock_compatible_table_[traverse_lock_request->lock_mode_].find(lock_mode) ==
                 lock_compatible_table_[traverse_lock_request->lock_mode_].end())) {
                LOG_INFO("Check compatibility with lock mode %d, %s", lock_mode, "false");
                return false;
            }
        }
        return true;
    }

    auto LockManagerV2::LockTable(TransactionWith2PL *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
        //if lock validation of the txn fails, throw exception
        ValidateLockMode(txn, lock_mode);
        // construct a lock request based on txn
        auto *lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
        // get the lock request queue of table oid, if the queue is empty, construct a new one, and add the lock request to
        // the queue
        std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
        if (table_lock_map_.find(oid) == table_lock_map_.end()) {
            auto lock_queue = new LockRequestQueue();
            table_lock_map_[oid] = std::shared_ptr<LockRequestQueue>(lock_queue);
            std::unique_lock<std::mutex> queue_lock(lock_queue->latch_);
            lock_request->granted_ = true;
            lock_queue->request_queue_.push_back(std::shared_ptr<LockRequest>(lock_request));
            lock_queue->txn_lock_request_map_[txn->GetTransactionId()] = std::prev(lock_queue->request_queue_.end());
            // TODO 向事务txn的shared_lock_set或exclusive_lock_set添加oid
            txn->SetState(TransactionStateWith2PL::GROWING);
            return true;
        }  // get the lock mode of the first lock request in the queue and check whether lock_mode is compatible with it
        auto lock_queue = table_lock_map_[oid];
        table_lock.unlock();
        std::unique_lock<std::mutex> queue_lock(lock_queue->latch_);
        if (lock_queue->txn_lock_request_map_.find(txn->GetTransactionId()) !=
            lock_queue->txn_lock_request_map_.end()) {
            // lock upgrading because the lock request is already in the queue
            return UpgradeLockTable(txn, lock_mode, oid, lock_queue.get());
        }
        if (!ValidateLock(lock_queue.get(), lock_mode)) {
            // wait until the lock is compatible with the existing lock mode in the queue and consider there will be a concurrent transaction which will be granted the lock whern resource is unlocked
            lock_queue->cv_.wait(queue_lock, [&lock_queue, &lock_mode, &txn, this]() {
                LOG_INFO("Txn %d waiting for lock for current mode %d", txn->GetTransactionId(), lock_mode);
                for (const auto &traverse_lock_request: lock_queue->request_queue_) {
                    if (traverse_lock_request->granted_ &&
                        (lock_compatible_table_[traverse_lock_request->lock_mode_].find(lock_mode) ==
                         lock_compatible_table_[traverse_lock_request->lock_mode_].end())) {
                        return false;
                    }
                }
                return lock_queue->upgrading_ == INVALID_TXN_ID || lock_queue->upgrading_ == txn->GetTransactionId();
            });
            LOG_INFO("Txn %d is notified by unlock for mode %d", txn->GetTransactionId(), lock_mode);
        }
        // now this thread is notified by another thread or is compatible with the existing lock mode, so add the lock
        // request to the queue
        lock_request->granted_ = true;
        lock_queue->request_queue_.push_back(std::shared_ptr<LockRequest>(lock_request));
        lock_queue->txn_lock_request_map_[txn->GetTransactionId()] = std::prev(lock_queue->request_queue_.end());
        txn->SetState(TransactionStateWith2PL::GROWING);
        return true;
    }

    auto LockManagerV2::UpgradeLockTable(TransactionWith2PL *txn, LockMode lock_mode, const table_oid_t &oid,
                                         LockRequestQueue *request_queue) -> bool {
        /**
         * Attention!!! request_queue has been already locked in the caller function, so no need to lock it again
         * Otherwise it will cause deadlock because there is no reentrant lock in C++ mutex
         */
        auto current_lock_request = request_queue->txn_lock_request_map_[txn->GetTransactionId()];
        // if txn does not hold the lock or the current lock mode is the same as the target lock mode, return false
        if ( !((*current_lock_request)->granted_) || ((*current_lock_request)->lock_mode_ == lock_mode)) {
            return false;
        }
        if (!CanLockUpgrade((*current_lock_request)->lock_mode_, lock_mode)) {
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::IMCOMPATIBLE_UPGRADE);
        }
        request_queue->upgrading_ = txn->GetTransactionId();
        LOG_INFO("Txn %d is upgrading lock mode to %d, drop current lock %d", txn->GetTransactionId(), lock_mode, (*current_lock_request)->lock_mode_);
        //DROP the current lock
        request_queue->request_queue_.erase(current_lock_request);
        request_queue->txn_lock_request_map_.erase(txn->GetTransactionId());
        request_queue->cv_.notify_all();
        //Wait to get the new lock
        ValidateLockMode(txn, lock_mode);
        auto *lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
        lock_request->granted_ = true;
        request_queue->request_queue_.push_back(std::shared_ptr<LockRequest>(lock_request));
        request_queue->txn_lock_request_map_[txn->GetTransactionId()] = std::prev(request_queue->request_queue_.end());
        txn->SetState(TransactionStateWith2PL::GROWING);
        request_queue->upgrading_ = INVALID_TXN_ID;
        return true;
    }

    auto LockManagerV2::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
        if (lock_upgrade_table_.find(curr_lock_mode) == lock_upgrade_table_.end() ||
            lock_upgrade_table_[curr_lock_mode].find(requested_lock_mode) ==
            lock_upgrade_table_[curr_lock_mode].end()) {
            return false;
        }
        return true;
    }

    auto LockManagerV2::UnlockTable(TransactionWith2PL *txn, const table_oid_t &oid) -> bool {
        // find the lock request queue of table oid, and remove the lock request of txn from the queue
        std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
        if (table_lock_map_.find(oid) == table_lock_map_.end()) {
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_NOT_EXIST);
        } else {
            auto lock_queue = table_lock_map_[oid];
            table_lock.unlock();
            std::unique_lock<std::mutex> queue_lock(lock_queue->latch_);
            if (lock_queue->request_queue_.empty()) {
                // todo throw exception
                return false;
            }
            auto current_lock_request = lock_queue->txn_lock_request_map_[txn->GetTransactionId()];
            if (!(*current_lock_request)->granted_) {
                // todo throw exception
                return false;
            }
            LOG_INFO("Txn %d unlock table", txn->GetTransactionId());
            lock_queue->request_queue_.erase(current_lock_request);
            lock_queue->txn_lock_request_map_.erase(txn->GetTransactionId());
            lock_queue->cv_.notify_all();
            txn->SetState(TransactionStateWith2PL::SHRINKING);
            // TODO 从事务txn的shared_lock_set或exclusive_lock_set删除oid
            return true;
        }
    }

    auto LockManagerV2::LockRow(TransactionWith2PL *txn, LockMode lock_mode, const table_oid_t &oid,
                                const RID &rid) -> bool {
        return true;
    }

    auto LockManagerV2::UnlockRow(TransactionWith2PL *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
        return true;
    }

    void LockManagerV2::UnlockAll() {
        // You probably want to unlock all table and txn locks here.
    }

    void LockManagerV2::AddEdge(txn_id_t t1, txn_id_t t2) {}

    void LockManagerV2::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

    auto LockManagerV2::HasCycle(txn_id_t *txn_id) -> bool { return false; }

    auto LockManagerV2::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
        std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
        return edges;
    }

    void LockManagerV2::RunCycleDetection() {
        while (enable_cycle_detection_) {
            //TODO sleep for configurable seconds
            //sleep for 1 millisecond
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            {  // TODO(students): detect deadlock
            }
        }
    }

}  // namespace bustub
