/**
 * lock_manager_test.cpp
 */

#include <thread>

#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace cmudb {


/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */
    //TEST1
    TEST(LockManagerTest, BasicTest) {
        LockManager lock_mgr{false};
        TransactionManager txn_mgr{&lock_mgr};
        RID rid{0, 0};

        std::thread t0([&] {
            Transaction txn(0);
            bool res = lock_mgr.LockShared(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
            txn_mgr.Commit(&txn);
            EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
        });

        std::thread t1([&] {
            Transaction txn(1);
            bool res = lock_mgr.LockShared(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
            txn_mgr.Commit(&txn);
            EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
        });

        t0.join();
        t1.join();
    }

    //TEST2
    TEST(LockManagerTest, LockSharedTest) {
        LockManager lock_mgr{false};
        TransactionManager txn_mgr{&lock_mgr};
        RID rid{0, 0};

        Transaction *txns[10];
        for (int i = 0; i < 10; i++) {
            txns[i] = txn_mgr.Begin();
            EXPECT_TRUE(lock_mgr.LockShared(txns[i], rid));
            EXPECT_EQ(TransactionState::GROWING, txns[i]->GetState());
        }
        for (auto &txn : txns) {
            txn_mgr.Commit(txn);
            EXPECT_EQ(TransactionState::COMMITTED, txn->GetState());
            delete txn;
        }
    }

    //TEST3
    TEST(LockManagerTest, BasicExclusiveTest) {
        LockManager lock_mgr{false};
        TransactionManager txn_mgr{&lock_mgr};
        RID rid{0, 0};

        std::promise<void> go, p0, p1, p2;
        std::shared_future<void> ready(go.get_future());

        std::thread t0([&, ready] {
            Transaction txn(5);
            bool res = lock_mgr.LockExclusive(&txn, rid);

            p0.set_value();
            ready.wait();

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        std::thread t1([&, ready] {
            Transaction txn(3);

            p1.set_value();
            ready.wait();

            bool res = lock_mgr.LockShared(&txn, rid);

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            res = lock_mgr.Unlock(&txn, rid);

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);

        });

        std::thread t2([&, ready] {
            Transaction txn(1);

            p2.set_value();
            ready.wait();

            // wait for t1
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            bool res = lock_mgr.LockShared(&txn, rid);

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            res = lock_mgr.Unlock(&txn, rid);

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);

        });

        p0.get_future().wait();
        p1.get_future().wait();
        p2.get_future().wait();

        go.set_value();

        t0.join();
        t1.join();
        t2.join();
    }

    //TEST4
    TEST(LockManagerTest, LockExclusiveTest) {
        LockManager lock_mgr{false};
        TransactionManager txn_mgr{&lock_mgr};
        RID rid{0, 0};

        {
            std::mutex mu;
            Transaction txn1{1};
            EXPECT_TRUE(lock_mgr.LockShared(&txn1, rid));
            EXPECT_EQ(TransactionState::GROWING, txn1.GetState());

            std::thread t( [&] {
                Transaction txn0{0};
                EXPECT_TRUE(lock_mgr.LockExclusive(&txn0, rid));
                EXPECT_EQ(TransactionState::GROWING, txn0.GetState());
                {
                    std::lock_guard<std::mutex> lock{mu};

                    EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
                }
                txn_mgr.Commit(&txn0);
                EXPECT_EQ(TransactionState::COMMITTED, txn0.GetState());
            } );

            Transaction txn2{2};
            RID rid1{0, 1};
            EXPECT_TRUE(lock_mgr.LockExclusive(&txn2, rid1));
            EXPECT_EQ(TransactionState::GROWING, txn2.GetState());
            txn_mgr.Commit(&txn2);
            //std::cout<<"trasaction: "<<txn2.GetTransactionId()<<"committed"<<(txn2.GetState()==TransactionState::COMMITTED)<<std::endl;
            EXPECT_EQ(TransactionState::COMMITTED, txn2.GetState());
            {
                std::lock_guard<std::mutex> lock{mu};
                txn_mgr.Commit(&txn1);
                EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
            }
            t.join();
        }

        {
            std::mutex mu;
            Transaction txn1{1};
            EXPECT_TRUE(lock_mgr.LockExclusive(&txn1, rid));
            EXPECT_EQ(TransactionState::GROWING, txn1.GetState());

            std::thread t([&] {
                Transaction txn0{0};
                EXPECT_TRUE(lock_mgr.LockShared(&txn0, rid));
                EXPECT_EQ(TransactionState::GROWING, txn0.GetState());
                {
                    std::lock_guard<std::mutex> lock{mu};
                    EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
                }
                txn_mgr.Commit(&txn1);
                EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
            });

            {
                std::lock_guard<std::mutex> lock{mu};
                txn_mgr.Commit(&txn1);
                EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
            }
            t.join();
        }
    }

    //TEST5
    TEST(LockManagerTest, LockUpgradeTest) {
        LockManager lock_mgr{false};
        TransactionManager txn_mgr{&lock_mgr};
        RID rid{0, 0};

        {
            Transaction txn{0};
            EXPECT_FALSE(lock_mgr.LockUpgrade(&txn, rid));
            EXPECT_EQ(TransactionState::ABORTED, txn.GetState());
            txn_mgr.Abort(&txn);
        }

        {
            Transaction txn{0};
            EXPECT_TRUE(lock_mgr.LockExclusive(&txn, rid));
            EXPECT_FALSE(lock_mgr.LockUpgrade(&txn, rid));
            EXPECT_EQ(TransactionState::ABORTED, txn.GetState());
            txn_mgr.Abort(&txn);
        }

        {
            Transaction txn{0};
            EXPECT_TRUE(lock_mgr.LockShared(&txn, rid));
            EXPECT_TRUE(lock_mgr.LockUpgrade(&txn, rid));
            txn_mgr.Commit(&txn);
        }

        {
            std::mutex mu;
            Transaction txn0{0};
            Transaction txn1{1};
            EXPECT_TRUE(lock_mgr.LockShared(&txn1, rid));

            std::thread t([&] {
                EXPECT_TRUE(lock_mgr.LockShared(&txn0, rid));
                EXPECT_TRUE(lock_mgr.LockUpgrade(&txn0, rid));
                {
                    std::lock_guard<std::mutex> lock{mu};
                    EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
                }
                txn_mgr.Commit(&txn0);
                EXPECT_EQ(TransactionState::COMMITTED, txn0.GetState());
            });

            {
                std::lock_guard<std::mutex> lock{mu};
                txn_mgr.Commit(&txn1);
                EXPECT_EQ(TransactionState::COMMITTED, txn1.GetState());
            }
            t.join();
        }
    }

    //TEST6
    TEST(LockManagerTest, BasicUpdateTest) {
        LockManager lock_mgr{false};
        TransactionManager txn_mgr{&lock_mgr};
        RID rid{0, 0};

        std::promise<void> go, p0, p1, p2, p3;
        std::shared_future<void> ready(go.get_future());

        std::thread t0([&, ready] {
            Transaction txn(0);
            bool res = lock_mgr.LockShared(&txn, rid);

            p0.set_value();
            ready.wait();

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            // update
            res = lock_mgr.LockUpgrade(&txn, rid);

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        std::thread t1([&, ready] {
            Transaction txn(1);

            bool res = lock_mgr.LockShared(&txn, rid);

            p1.set_value();
            ready.wait();

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        std::thread t2([&, ready] {
            Transaction txn(2);
            bool res = lock_mgr.LockShared(&txn, rid);

            p2.set_value();
            ready.wait();

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        std::thread t3([&, ready] {
            Transaction txn(3);
            bool res = lock_mgr.LockShared(&txn, rid);

            p3.set_value();
            ready.wait();

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        p0.get_future().wait();
        p1.get_future().wait();
        p2.get_future().wait();
        p3.get_future().wait();

        go.set_value();

        t0.join();
        t1.join();
        t2.join();
        t3.join();
    }

    TEST(LockManagerTest, 2plTest) {
        LockManager lock_mgr{false};
        Transaction txn{0};
        RID rid0{0}, rid1{1};

        EXPECT_TRUE(lock_mgr.LockShared(&txn, rid0));
        EXPECT_TRUE(lock_mgr.Unlock(&txn, rid0));
        EXPECT_EQ(TransactionState::SHRINKING, txn.GetState());
        EXPECT_FALSE(lock_mgr.LockShared(&txn, rid1));
        EXPECT_EQ(TransactionState::ABORTED, txn.GetState());
    }

    TEST(LockManagerTest, S2plTest) {
        LockManager lock_mgr{true};
        RID rid{0};

        {
            Transaction txn{0};
            EXPECT_TRUE(lock_mgr.LockShared(&txn, rid));
            EXPECT_TRUE(lock_mgr.Unlock(&txn, rid));
            EXPECT_EQ(TransactionState::SHRINKING, txn.GetState());
        }

        {
            Transaction txn{0};
            EXPECT_TRUE(lock_mgr.LockShared(&txn, rid));
            txn.SetState(TransactionState::COMMITTED);
            EXPECT_TRUE(lock_mgr.Unlock(&txn, rid));
        }
    }

    TEST(LockManagerTest, BasicTest1) {
        LockManager lock_mgr{false};
        TransactionManager txn_mgr{&lock_mgr};
        RID rid{0, 0};

        std::promise<void> go, t0, t1, t2;
        std::shared_future<void> ready(go.get_future());

        std::thread thread0([&, ready] {
            Transaction txn(2);

            // will block and can wait
            bool res = lock_mgr.LockShared(&txn, rid);

            t0.set_value();
            ready.wait();

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // unlock
            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        std::thread thread1([&, ready] {
            Transaction txn(1);

            // will block and can wait
            bool res = lock_mgr.LockShared(&txn, rid);

            t1.set_value();
            ready.wait();

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // unlock
            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        std::thread thread2([&, ready] {
            Transaction txn(0);

            t2.set_value();
            ready.wait();

            // can wait and will block
            bool res = lock_mgr.LockExclusive(&txn, rid);

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        t0.get_future().wait();
        t1.get_future().wait();
        t2.get_future().wait();

        // go!
        go.set_value();

        thread0.join();
        thread1.join();
        thread2.join();
    }

    TEST(LockManagerTest, BasicTest2) {
        LockManager lock_mgr{false};
        TransactionManager txn_mgr{&lock_mgr};
        RID rid{0, 0};

        std::promise<void> go, t0, t1, t2;
        std::shared_future<void> ready(go.get_future());

        std::thread thread0([&, ready] {
            Transaction txn(0);

            t0.set_value();
            ready.wait();

            // let thread1 try to acquire shared lock first
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // will block and can wait
            bool res = lock_mgr.LockShared(&txn, rid);

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            // unlock
            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        std::thread thread1([&, ready] {
            Transaction txn(1);

            t1.set_value();
            ready.wait();

            // will block and can wait
            bool res = lock_mgr.LockShared(&txn, rid);

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);


            // unlock
            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        std::thread thread2([&, ready] {
            Transaction txn(2);

            // can wait and will block
            bool res = lock_mgr.LockExclusive(&txn, rid);

            t2.set_value();
            ready.wait();

            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

            std::this_thread::sleep_for(std::chrono::milliseconds(200));

            res = lock_mgr.Unlock(&txn, rid);
            EXPECT_EQ(res, true);
            EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
        });

        t0.get_future().wait();
        t1.get_future().wait();
        t2.get_future().wait();

        // go!
        go.set_value();

        thread0.join();
        thread1.join();
        thread2.join();
    }
} // namespace cmudb