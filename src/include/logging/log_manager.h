/**
 * log_manager.h
 * log manager maintain a separate thread that is awaken when the log buffer is
 * full or time out(every X second) to write log buffer's content into disk log
 * file.
 */

#pragma once

#include <algorithm>
#include <condition_variable>
#include <future>
#include <mutex>

#include "disk/disk_manager.h"
#include "logging/log_record.h"

namespace cmudb {

    class LogManager {
    public:
        LogManager(DiskManager *disk_manager)
                : next_lsn_(0), persistent_lsn_(INVALID_LSN),
                  disk_manager_(disk_manager) {
            // 一定初始化flushBufferSize、writePosition为具体的值
            writePosition = 0;
            flushBufferSize = 0;
            needFlush_ = false;
            log_buffer_ = new char[LOG_BUFFER_SIZE];
            flush_buffer_ = new char[LOG_BUFFER_SIZE];
        }

        ~LogManager() {
            delete[] log_buffer_;
            delete[] flush_buffer_;
            log_buffer_ = nullptr;
            flush_buffer_ = nullptr;
        }

        // spawn a separate thread to wake up periodically to flush
        void RunFlushThread();

        void StopFlushThread();

        // append a log record into log buffer
        lsn_t AppendLogRecord(LogRecord &log_record);

        // get/set helper functions
        inline lsn_t GetPersistentLSN() { return persistent_lsn_; }

        inline size_t GetWritePosition() { return writePosition; }

        inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }

        inline char *GetLogBuffer() { return log_buffer_; }

        //控制log buffer
        void flushLogToDisk( bool force );

    private:

        // also remember to change constructor accordingly
        //下一次写起始的位置
        size_t writePosition;
        std::atomic<bool> needFlush_; //条件变量中的状态变量
        std::condition_variable notFull;

        // atomic counter, record the next log sequence number
        std::atomic<lsn_t> next_lsn_;
        // log records before & include persistent_lsn_ have been written to disk
        std::atomic<lsn_t> persistent_lsn_;
        //log buffer中最后一条log record的lsn
        std::atomic<lsn_t> last_lsn_;
        /* log buffer related
           log buffer环形缓冲区
           当log buffer满的情况下，需要唤醒flush thread线程，将log buffer的内容持久化到硬盘中
         */
        char *log_buffer_;

        size_t flushBufferSize;
        char *flush_buffer_;
        // latch to protect shared member variables
        std::mutex latch_;
        // flush thread
        std::thread *flush_thread_;
        // for notifying flush thread
        std::condition_variable cv_;
        // disk manager
        DiskManager *disk_manager_;
    };

} // namespace cmudb
