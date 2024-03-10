#include <chrono>
#include <cstdio>
#include <cstdlib>

#include "logging/common.h"
#include "logging/log_recovery.h"
#include "vtable/virtual_table.h"
#include "gtest/gtest.h"

namespace cmudb {

    TEST(LogManagerTest, BasicLogging) {
        StorageEngine *storage_engine = new StorageEngine("test.db");

        EXPECT_FALSE(ENABLE_LOGGING);
        LOG_DEBUG("Skip system recovering...");

        storage_engine->log_manager_->RunFlushThread();
        EXPECT_TRUE(ENABLE_LOGGING);
        LOG_DEBUG("System logging thread running...");

        LOG_DEBUG("Create a test table");
        Transaction *txn = storage_engine->transaction_manager_->Begin();
        TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                              storage_engine->lock_manager_,
                                              storage_engine->log_manager_, txn);
        LOG_DEBUG("Insert and delete a random tuple");

        std::string createStmt =
                "a varchar, b smallint, c bigint, d bool, e varchar(16)";
        Schema *schema = ParseCreateStatement(createStmt);
        RID rid;
        Tuple tuple = ConstructTuple(schema);
        EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
        EXPECT_TRUE(test_table->MarkDelete(rid, txn));
        LOG_DEBUG("Before Commit txn");
        storage_engine->transaction_manager_->Commit(txn);
        LOG_DEBUG("After Commit txn");

        storage_engine->log_manager_->StopFlushThread();
        EXPECT_FALSE(ENABLE_LOGGING);
        LOG_DEBUG("Turning off flushing thread");

        LOG_DEBUG("After Commit Log persistent lsn %d", storage_engine->log_manager_->GetPersistentLSN());

        // some basic manually checking here
        char buffer[PAGE_SIZE];
        storage_engine->disk_manager_->ReadLog(buffer, PAGE_SIZE, 0);
        int32_t size = *reinterpret_cast<int32_t *>(buffer);
        LOG_DEBUG("size  = %d", size);
        size = *reinterpret_cast<int32_t *>(buffer + 20);
        LOG_DEBUG("size  = %d", size);
        size = *reinterpret_cast<int32_t *>(buffer + 48);
        LOG_DEBUG("size  = %d", size);

        delete txn;
        delete storage_engine;
        delete test_table;
        delete schema;
        LOG_DEBUG("Teared down the system");
        remove("test.db");
        remove("test.log");
    }

    void task_transaction_begin(StorageEngine *storage_engine, TableHeap *table){
        Transaction *txn = storage_engine->transaction_manager_->Begin();
        std::cout<<std::this_thread::get_id()<<std::endl;
        LOG_DEBUG("Insert and delete random tuple");

        std::string create_stat = "a varchar, b smallint, c bigint, d bool, e varchar(16)";
        Schema *schema = ParseCreateStatement(create_stat);
        RID rid;
        Tuple tuple = ConstructTuple(schema);
        EXPECT_TRUE(table->InsertTuple(tuple, rid, txn));
        EXPECT_TRUE(table->MarkDelete( rid, txn));
        storage_engine->transaction_manager_->Commit(txn);

        delete txn;
    }
    void task_transaction_batch_opt(StorageEngine *storage_engine, TableHeap *table){
        Transaction *txn = storage_engine->transaction_manager_->Begin();
        LOG_DEBUG("txn %d, Insert random tuples", txn->GetTransactionId());

        std::string create_stat = "a varchar, b smallint, c bigint, d bool, e varchar(16)";
        Schema *schema = ParseCreateStatement(create_stat);
        RID rid;
        for( int i = 0; i < 9; i++ ) {
            Tuple tuple = ConstructTuple(schema);
            EXPECT_TRUE(table->InsertTuple(tuple, rid, txn));
        }

        storage_engine->transaction_manager_->Commit(txn);
        LOG_DEBUG("After txn %d Commit", txn->GetTransactionId());
        delete txn;

    }
    TEST(LogManagerTest, LoggingWithGroupCommit) {
        StorageEngine *storage_engine = new StorageEngine("test.db");

        EXPECT_FALSE(ENABLE_LOGGING);
        LOG_DEBUG("Skip system recovering...");

        storage_engine->log_manager_->RunFlushThread();
        EXPECT_TRUE(ENABLE_LOGGING);
        /*
         * begin txn事务 以开始创建table
         */
        Transaction *txn = storage_engine->transaction_manager_->Begin();
        TableHeap *test_table = new TableHeap( storage_engine->buffer_pool_manager_,
                                               storage_engine->lock_manager_,
                                               storage_engine->log_manager_, txn);
        std::string create_stat = "a varchar, b smallint, c bigint, d bool, e varchar(16)";
        Schema *schema = ParseCreateStatement(create_stat);
        RID rid;
        Tuple tuple = ConstructTuple(schema);
        EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
        EXPECT_TRUE(test_table->MarkDelete( rid, txn));
        storage_engine->transaction_manager_->Commit(txn);
        delete txn;

        std::future<void> f1 = std::async(std::launch::async, task_transaction_begin, storage_engine, test_table );
        std::future<void> f2 = std::async(std::launch::async, task_transaction_begin, storage_engine, test_table );
        std::future<void> f3 = std::async(std::launch::async, task_transaction_begin, storage_engine, test_table );

        f1.wait();
        f2.wait();
        f3.wait();

        LOG_DEBUG("After Commit Log persistent lsn %d", storage_engine->log_manager_->GetPersistentLSN());
        storage_engine->log_manager_->StopFlushThread();
        EXPECT_FALSE(ENABLE_LOGGING);
        LOG_DEBUG("Turning off flushing thread");

        char buffer[PAGE_SIZE];
        storage_engine->disk_manager_->ReadLog(buffer, PAGE_SIZE, 0);
        int32_t size = *reinterpret_cast<int32_t *>(buffer);
        LOG_DEBUG("size  = %d", size);
        size = *reinterpret_cast<int32_t *>(buffer + 20);
        LOG_DEBUG("size  = %d", size);
        size = *reinterpret_cast<int32_t *>(buffer + 48);
        LOG_DEBUG("size  = %d", size);

        delete storage_engine;
        delete test_table;
        delete schema;

        LOG_DEBUG("Teared down the system");
        remove("test.db");
        remove("test.log");
    }

    TEST(LogManagerTest, SingleLoggingWithBufferFull) {

        StorageEngine *storage_engine = new StorageEngine("test.db");
        EXPECT_FALSE(ENABLE_LOGGING);
        LOG_DEBUG("Skip system recovering...");

        storage_engine->log_manager_->RunFlushThread();
        EXPECT_TRUE(ENABLE_LOGGING);
        LOG_DEBUG("System logging thread running...");

        LOG_DEBUG("Create a test table");
        Transaction *txn = storage_engine->transaction_manager_->Begin();
        TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                              storage_engine->lock_manager_,
                                              storage_engine->log_manager_, txn);
        LOG_DEBUG("Insert random tuples");

        std::string createStmt =
                "a varchar, b smallint, c bigint, d bool, e varchar(16)";
        Schema *schema = ParseCreateStatement(createStmt);
        RID rid;

        for (int i = 0; i < 9; i++)
        {
            Tuple tuple = ConstructTuple(schema);
            EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
        }
        LOG_DEBUG("Before Commit Log persistent lsn %d", storage_engine->log_manager_->GetPersistentLSN());
        LOG_DEBUG("Before Commit txn %d", txn->GetTransactionId());
        storage_engine->transaction_manager_->Commit(txn);
        LOG_DEBUG("After Commit Log persistent lsn %d", storage_engine->log_manager_->GetPersistentLSN());
        delete txn;

        storage_engine->log_manager_->StopFlushThread();
        EXPECT_FALSE(ENABLE_LOGGING);
        LOG_DEBUG("Turning off flushing thread");
        LOG_DEBUG("num of flushes = %d", storage_engine->disk_manager_->GetNumFlushes());

        // some basic manually checking here
        char buffer[PAGE_SIZE];
        storage_engine->disk_manager_->ReadLog(buffer, PAGE_SIZE, 0);
        int32_t size = *reinterpret_cast<int32_t *>(buffer);
        LOG_DEBUG("size  = %d", size);
        size = *reinterpret_cast<int32_t *>(buffer + 20);
        LOG_DEBUG("size  = %d", size);
        size = *reinterpret_cast<int32_t *>(buffer + 48);
        LOG_DEBUG("size  = %d", size);

        delete storage_engine;
        LOG_DEBUG("Teared down the system");
        remove("test.db");
        remove("test.log");
    }

    TEST(LogManagerTest, MultiLoggingWithBufferFull){
        StorageEngine *storage_engine = new StorageEngine("test.db");
        EXPECT_FALSE(ENABLE_LOGGING);
        LOG_DEBUG("Skip system recovering...");

        storage_engine->log_manager_->RunFlushThread();
        EXPECT_TRUE(ENABLE_LOGGING);
        LOG_DEBUG("System logging thread running...");

        LOG_DEBUG("Create a test table");
        Transaction *txn = storage_engine->transaction_manager_->Begin();
        TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                              storage_engine->lock_manager_,
                                              storage_engine->log_manager_, txn);
        LOG_DEBUG("Insert random tuples");

        std::string createStmt =
                "a varchar, b smallint, c bigint, d bool, e varchar(16)";
        Schema *schema = ParseCreateStatement(createStmt);
        RID rid;

        for (int i = 0; i < 9; i++)
        {
            Tuple tuple = ConstructTuple(schema);
            EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
        }
        std::future<void> f1 = std::async(std::launch::async, task_transaction_batch_opt, storage_engine, test_table );
        std::future<void> f2 = std::async(std::launch::async, task_transaction_batch_opt, storage_engine, test_table );

        LOG_DEBUG("Before Commit Log persistent lsn %d", storage_engine->log_manager_->GetPersistentLSN());
        LOG_DEBUG("Commit txn %d", txn->GetTransactionId());

        storage_engine->transaction_manager_->Commit(txn);
        LOG_DEBUG("After Commit Log persistent lsn %d", storage_engine->log_manager_->GetPersistentLSN());
        delete txn;

        LOG_DEBUG("Before waiting");

        f1.wait();
        f2.wait();

        storage_engine->log_manager_->StopFlushThread();
        EXPECT_FALSE(ENABLE_LOGGING);
        LOG_DEBUG("Turning off flushing thread");
        LOG_DEBUG("num of flushes = %d", storage_engine->disk_manager_->GetNumFlushes());

        // some basic manually checking here
        char buffer[PAGE_SIZE];
        storage_engine->disk_manager_->ReadLog(buffer, PAGE_SIZE, 0);
        int32_t size = *reinterpret_cast<int32_t *>(buffer);
        LOG_DEBUG("size  = %d", size);
        size = *reinterpret_cast<int32_t *>(buffer + 20);
        LOG_DEBUG("size  = %d", size);
        size = *reinterpret_cast<int32_t *>(buffer + 48);
        LOG_DEBUG("size  = %d", size);

        delete storage_engine;
        LOG_DEBUG("Teared down the system");
        remove("test.db");
        remove("test.log");
    }

// actually LogRecovery
    /*TEST(LogManagerTest, RedoTestWithOneTxn) {
        StorageEngine *storage_engine = new StorageEngine("test.db");

        EXPECT_FALSE(ENABLE_LOGGING);
        LOG_DEBUG("Skip system recovering...");

        storage_engine->log_manager_->RunFlushThread();
        EXPECT_TRUE(ENABLE_LOGGING);
        LOG_DEBUG("System logging thread running...");

        LOG_DEBUG("Create a test table");
        Transaction *txn = storage_engine->transaction_manager_->Begin();
        TableHeap *test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                              storage_engine->lock_manager_,
                                              storage_engine->log_manager_, txn);
        page_id_t first_page_id = test_table->GetFirstPageId();

        std::string createStmt =
                "a varchar, b smallint, c bigint, d bool, e varchar(16)";
        Schema *schema = ParseCreateStatement(createStmt);

        RID rid;
        Tuple tuple = ConstructTuple(schema);
        std::cout << "Tuple: " << tuple.ToString(schema) << "\n";
        Tuple tuple1 = ConstructTuple(schema);
        std::cout << "Tuple1: " << tuple1.ToString(schema) << "\n";

        auto val = tuple.GetValue(schema, 4);
        EXPECT_TRUE(test_table->InsertTuple(tuple, rid, txn));
        storage_engine->transaction_manager_->Commit(txn);
        delete txn;
        delete test_table;
        LOG_DEBUG("Commit txn");

        LOG_DEBUG("SLEEPING for 2s");
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // shutdown System
        delete storage_engine;

        // restart system
        storage_engine = new StorageEngine("test.db");
        LogRecovery *log_recovery = new LogRecovery(
                storage_engine->disk_manager_, storage_engine->buffer_pool_manager_);

        log_recovery->Redo();
        log_recovery->Undo();

        Tuple old_tuple;
        txn = storage_engine->transaction_manager_->Begin();
        test_table = new TableHeap(storage_engine->buffer_pool_manager_,
                                   storage_engine->lock_manager_,
                                   storage_engine->log_manager_, first_page_id);
        EXPECT_EQ(test_table->GetTuple(rid, old_tuple, txn), 1);
        storage_engine->transaction_manager_->Commit(txn);
        delete txn;
        delete test_table;

        EXPECT_EQ(old_tuple.GetValue(schema, 4).CompareEquals(val), 1);

        delete storage_engine;
        LOG_DEBUG("Teared down the system");
        remove("test.db");
        remove("test.log");
    }*/

} // namespace cmudb
