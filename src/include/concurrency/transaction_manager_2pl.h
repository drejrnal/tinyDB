//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.h
//
// Identification: src/include/concurrency/transaction_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "common/config.h"
#include "common/rwmutex.h"
#include "concurrency/lock_manager_v2.h"
#include "concurrency/transaction_2pl.h"

namespace cmudb {
class LockManagerV2;

/**
 * TransactionManager keeps track of all the transactions running in the system.
 */
class TransactionManagerWith2PL {
 public:
  explicit TransactionManagerWith2PL(LockManagerV2 *lock_manager)
      : lock_manager_(lock_manager) {}

  ~TransactionManagerWith2PL() = default;

  /**
   * Begins a new transaction.
   * @param txn an optional transaction object to be initialized, otherwise a new transaction is created.
   * @param isolation_level an optional isolation level of the transaction.
   * @return an initialized transaction
   */
  auto Begin(TransactionWith2PL *txn = nullptr, IsolationLevelWith2PL isolation_level = IsolationLevelWith2PL::REPEATABLE_READ)
      -> TransactionWith2PL *;

  /**
   * Commits a transaction.
   * @param txn the transaction to commit
   */
  void Commit(TransactionWith2PL *txn);

  /**
   * Aborts a transaction
   * @param txn the transaction to abort
   */
  void Abort(TransactionWith2PL *txn);

  /**
   * Global list of running transactions
   */

  /** The transaction map is a global list of all the running transactions in the system. */
  std::unordered_map<txn_id_t, TransactionWith2PL *> txn_map;
  RWMutex txn_map_mutex;

  /**
   * Locates and returns the transaction with the given transaction ID.
   * @param txn_id the id of the transaction to be found, it must exist!
   * @return the transaction with the given transaction id
   */
  auto GetTransaction(txn_id_t txn_id) -> TransactionWith2PL * {
    txn_map_mutex.RLock();
    assert(txn_map.find(txn_id) != txn_map.end());
    auto *res = txn_map[txn_id];
    assert(res != nullptr);
    txn_map_mutex.RUnlock();
    return res;
  }

  /** Prevents all transactions from performing operations, used for checkpointing. */
  void BlockAllTransactions();

  /** Resumes all transactions, used for checkpointing. */
  void ResumeTransactions();

 private:
  /**
   * Releases all the locks held by the given transaction.
   * @param txn the transaction whose locks should be released
   */
  void ReleaseLocks(TransactionWith2PL *txn);

  std::atomic<txn_id_t> next_txn_id_{0};
  LockManagerV2 *lock_manager_ __attribute__((__unused__));

  /** The global transaction latch is used for checkpointing. */
  RWMutex global_txn_latch_;
};

}  // namespace bustub
