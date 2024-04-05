//
// Created by luoxiYun on 2024/4/4.
//
#include <thread>
#include "concurrency/transaction_manager_2pl.h"
#include "concurrency/lock_manager_v2.h"
#include "gtest/gtest.h"
#include "common/logger.h"

namespace cmudb {

    // --- Helper functions ---
    void CheckGrowing(TransactionWith2PL *txn) { EXPECT_EQ(txn->GetState(), TransactionStateWith2PL::GROWING); }

    void CheckShrinking(TransactionWith2PL *txn) { EXPECT_EQ(txn->GetState(), TransactionStateWith2PL::SHRINKING); }

    void CheckCommitted(TransactionWith2PL *txn) { EXPECT_EQ(txn->GetState(), TransactionStateWith2PL::COMMITTED); }

    void LockTableTest() {
        LockManagerV2 lock_mgr;
        TransactionManagerWith2PL txn_mgr{&lock_mgr};

        table_oid_t oid{static_cast<uint32_t>(0)};
        TransactionWith2PL *txn = txn_mgr.Begin();
        //lock table
        lock_mgr.LockTable(txn, LockManagerV2::LockMode::SHARED, oid);
        CheckGrowing(txn);
        //unlock table
        lock_mgr.UnlockTable(txn, oid);
        CheckShrinking(txn);
        //commit transaction
        txn_mgr.Commit(txn);
        CheckCommitted(txn);
        delete txn;
    }

    void multithreadLockTableSharedTest(){
        LockManagerV2 lock_mgr;
        TransactionManagerWith2PL txn_mgr{&lock_mgr};

        auto txn1 = txn_mgr.Begin();
        auto txn2 = txn_mgr.Begin();

        table_oid_t oid{static_cast<uint32_t>(1)};
        bool lock_res;
        auto task = [&](){
            bool res;
            res = lock_mgr.LockTable(txn2, LockManagerV2::LockMode::EXCLUSIVE, oid);
            EXPECT_TRUE(res);
            CheckGrowing(txn2);
            //sleep for 300 ms
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            lock_mgr.UnlockTable(txn2, oid);
            CheckShrinking(txn2);
            txn_mgr.Commit(txn2);
            CheckCommitted(txn2);
        };
        std::thread t(task);
        lock_res = lock_mgr.LockTable(txn1, LockManagerV2::LockMode::SHARED, oid);
        EXPECT_TRUE(lock_res);
        CheckGrowing(txn1);
        //sleep for 100 ms
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        lock_mgr.UnlockTable(txn1, oid);
        CheckShrinking(txn1);
        txn_mgr.Commit(txn1);
        CheckCommitted(txn1);
        t.join();

        auto requests = lock_mgr.getLockQueueSize(oid);
        EXPECT_EQ(requests, 0);
        delete txn1;
        delete txn2;
    }

    void multithreadLockUpgradeTest(){
        LockManagerV2 lock_mgr;
        TransactionManagerWith2PL txn_mgr{&lock_mgr};

        auto txn1 = txn_mgr.Begin();
        auto txn2 = txn_mgr.Begin();

        table_oid_t oid{static_cast<uint32_t>(1)};
        bool lock_res;
        auto task = [&](){
            bool res;
            //transaction 0 do something before hold lock
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            res = lock_mgr.LockTable(txn1, LockManagerV2::LockMode::SHARED, oid);
            EXPECT_TRUE(res);
            CheckGrowing(txn1);
            lock_mgr.UnlockTable(txn1, oid);
            CheckShrinking(txn1);
            txn_mgr.Commit(txn1);
            CheckCommitted(txn1);
        };
        std::thread t(task);
        lock_res = lock_mgr.LockTable(txn2, LockManagerV2::LockMode::INTENTION_EXCLUSIVE, oid);
        EXPECT_TRUE(lock_res);
        CheckGrowing(txn2);
        //sleep for 100 ms
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        lock_res = lock_mgr.LockTable(txn2, LockManagerV2::LockMode::EXCLUSIVE, oid);
        EXPECT_TRUE(lock_res);
        CheckGrowing(txn2);
        //sleep for 300 ms
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        lock_mgr.UnlockTable(txn2, oid);
        CheckShrinking(txn2);
        txn_mgr.Commit(txn2);
        CheckCommitted(txn2);
        t.join();

        auto requests = lock_mgr.getLockQueueSize(oid);
        EXPECT_EQ(requests, 0);
        delete txn1;
        delete txn2;
    }

    TEST(LockTableTest, SerializialTest) {
        LockTableTest();
    }

    TEST(SharedExclusiveLockTest, MultithreadTest) {
        multithreadLockTableSharedTest();
    }

    TEST(LockUpgradionTest, MultithreadTest) {
        multithreadLockUpgradeTest();
    }



}