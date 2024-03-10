/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"
#include "common/logger.h"
namespace cmudb {
/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
    void LogManager::RunFlushThread() {

        ENABLE_LOGGING.store(true);
        flush_thread_ = new std::thread([&] {
            //buffer pool force flush时，启动该线程
            while ( ENABLE_LOGGING.load() ) {
                std::unique_lock<std::mutex> cvlock(latch_);
                bool time_out = cv_.wait_for(cvlock, LOG_TIMEOUT, [&] {
                    return needFlush_;
                });
                LOG_DEBUG("Begin to flush log buffer TIME_OUT[%d] Write position %d", time_out, (int)writePosition);
                //如果超时，则将log buffer的内容持久到盘中。
                if (writePosition > 0) {
                    std::swap(log_buffer_, flush_buffer_);
                    std::swap(writePosition, flushBufferSize);
                    //fsync()调用 写盘
                    disk_manager_->WriteLog(flush_buffer_, flushBufferSize);

                    flushBufferSize = 0;
                    SetPersistentLSN(last_lsn_);
                }
                //此时log buffer缓冲区可继续写，通知AppendRecord线程，继续写log
                needFlush_ = false;
                notFull.notify_all();
            }

        });

    }

/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
    void LogManager::StopFlushThread() {

        ENABLE_LOGGING.store( false );
        flushLogToDisk( true );
        LOG_DEBUG( " Signal flushing thread " );
        flush_thread_->join();
        assert(flushBufferSize == 0 && writePosition == 0 );
        delete flush_thread_;

    }

    /*
     * txn commit/abort 或者 buffer pool evict page时调用该函数，
     * group commit 控制log buffer内的内容是否同步到磁盘中
     */
    void LogManager::flushLogToDisk(bool force) {
        /*
         * 在log buffer内容刷到磁盘时，停止append线程
         * 同其余append thread竞争，若拿到锁，则Append线程阻塞
         */
        std::unique_lock<std::mutex> sync(latch_);
        if (force) {
            /*
             * 该函数被buffer pool manager calling thread调用，立即唤醒flush thread写日志，
             */
            needFlush_ = true;
            cv_.notify_one();
            /*
             * 此处调用notifu_one()前未调用sync.unlock()，因为后续需要访问needFlush_
             * 需要sync上锁，故一直保持sync持有锁的状态，当调用wait()调用时再释放sync锁
             */
            if (ENABLE_LOGGING)
                notFull.wait(sync, [&] { return !needFlush_; });
        } else {
            /*
             * 等待flush thread直到
             * LOG_TIMEOUT flush thread 写日志，完成后唤醒calling thread
             * 出于 group commit 考虑：
             * whenever you call Commit or Abort method, you need to make sure
             * your log records are permanently stored on disk file before release
             * the locks. But instead of forcing flush, you need to wait for LOG_TIMEOUT
             * or other operations to implicitly trigger the flush operations
             */
            notFull.wait(sync);
        }
    }

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
    lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
        std::unique_lock<std::mutex> bufferLatch(latch_);
        /*
         * 判断该log buffer剩余空间是否够存放log_record
         *  如果空间不够，则挂起当前append线程，唤醒flush线程
         *  否则，直接从offset位置开始写log record
         */
        if (writePosition + log_record.GetSize() >= LOG_BUFFER_SIZE) {
            LOG_DEBUG("Buffer overflow force log buffer flush");
            needFlush_ = true;
            cv_.notify_one();
            notFull.wait(bufferLatch, [&] {
                return writePosition + log_record.GetSize() < LOG_BUFFER_SIZE;
            });
        }

        log_record.lsn_ = next_lsn_++;

        memcpy(log_buffer_ + writePosition, &log_record, LogRecord::HEADER_SIZE);
        int pos = writePosition + 20;

        if (log_record.log_record_type_ == LogRecordType::INSERT) {
            memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
            pos += sizeof(RID);
            log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
        } else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE ||
                   log_record.log_record_type_ == LogRecordType::MARKDELETE) {
            memcpy(log_buffer_ + pos, &log_record.delete_rid_, sizeof(RID));
            pos += sizeof(RID);
            log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
        } else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
            memcpy(log_buffer_ + pos, &log_record.update_rid_, sizeof(RID));
            pos += sizeof(RID);
            log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
            //这里在deserialize过程内将old_tuple长度序列化，所以需要额外加上sizeof(int32_t)
            pos += (log_record.old_tuple_.GetLength() + sizeof(int32_t));
            log_record.new_tuple_.SerializeTo(log_buffer_ + pos);
        } else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
            memcpy(log_buffer_ + pos, &log_record.prev_page_id_, sizeof(page_id_t));
            pos +=sizeof(page_id_t);
            memcpy( log_buffer_ + pos, &log_record.page_id_, sizeof( page_id_t ));
        }

        //写完log_record后更新log file的偏移量
        writePosition += log_record.GetSize();
        last_lsn_.store(log_record.lsn_);
        return log_record.lsn_;
    }

} // namespace cmudb