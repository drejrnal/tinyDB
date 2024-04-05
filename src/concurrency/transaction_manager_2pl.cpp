//
// Created by luoxiYun on 2024/2/15.
//

#include "concurrency/transaction_manager_2pl.h"

namespace cmudb {

    auto TransactionManagerWith2PL::Begin(TransactionWith2PL *txn,
                                          IsolationLevelWith2PL isolation_level) -> TransactionWith2PL * {
        txn_map_mutex.WLock();
        auto txn_id = next_txn_id_++;
        if (txn == nullptr) {
            txn = new TransactionWith2PL(txn_id, isolation_level);
        } else {
            txn->SetIsolationLevel(isolation_level);
        }
        txn_map.insert(std::make_pair(txn_id, txn));
        txn_map_mutex.WUnlock();
        return txn;
    }

    void TransactionManagerWith2PL::Commit(cmudb::TransactionWith2PL *txn) {
        txn_map_mutex.WLock();
        txn->SetState(TransactionStateWith2PL::COMMITTED);
        txn_map.erase(txn->GetTransactionId());
        txn_map_mutex.WUnlock();
    }

    void TransactionManagerWith2PL::Abort(cmudb::TransactionWith2PL *txn) {
        txn_map_mutex.WLock();
        txn->SetState(TransactionStateWith2PL::ABORTED);
        txn_map.erase(txn->GetTransactionId());
        txn_map_mutex.WUnlock();
    }

    void TransactionManagerWith2PL::BlockAllTransactions() {}

    void TransactionManagerWith2PL::ResumeTransactions() {}


}  // namespace bustub