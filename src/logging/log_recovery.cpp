/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
    bool LogRecovery::DeserializeLogRecord( const char *data,
                                           LogRecord &log_record ) {

        if( data + LogRecord::HEADER_SIZE >= log_buffer_ + LOG_BUFFER_SIZE )
            return false;
        memcpy(&log_record, data, LogRecord::HEADER_SIZE);
        data += LogRecord::HEADER_SIZE;
        //丢弃不完整的log record
        if( data + log_record.size_ > log_buffer_ + LOG_BUFFER_SIZE )
            return false;
        switch (log_record.log_record_type_) {
            case LogRecordType::INSERT:
                log_record.insert_rid_ = *(reinterpret_cast<const RID *>(data));
                log_record.insert_tuple_.DeserializeFrom(data+sizeof(RID));
                break;
            case LogRecordType::MARKDELETE:
            case LogRecordType::APPLYDELETE:
            case LogRecordType::ROLLBACKDELETE:
                log_record.delete_rid_ = *(reinterpret_cast<const RID *>(data));
                log_record.delete_tuple_.DeserializeFrom( data + sizeof(RID));
                break;
            case LogRecordType::UPDATE:
                //注意data内数据成员的偏移
                log_record.update_rid_ = *(reinterpret_cast<const RID *>(data));
                log_record.old_tuple_.DeserializeFrom( data + sizeof( RID ));
                data += (log_record.old_tuple_.GetLength() +2* sizeof(RID));
                log_record.new_tuple_.DeserializeFrom( data );
                break;
            case LogRecordType::NEWPAGE:
                log_record.prev_page_id_ = *(reinterpret_cast<const page_id_t *>(data));
                log_record.page_id_ = *(reinterpret_cast<const page_id_t *>(data + sizeof(page_id_t)));
                break;
            case LogRecordType::BEGIN:
            case LogRecordType::COMMIT:
            case LogRecordType::ABORT:
                break;
            default:
                return false;
        }

        return true;
    }

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
    void LogRecovery::Redo() {

        //读取log file
        disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, 0);
        LogRecord logRecord;
        while (DeserializeLogRecord(log_buffer_ + offset_, logRecord)) {
            //构建lsn_mapping_和active_txn_表
            lsn_mapping_[logRecord.GetLSN()] = offset_;
            active_txn_[logRecord.GetTxnId()] = logRecord.GetLSN(); //每个txn对应其最大的lsn
            offset_ += logRecord.size_;
            if (logRecord.log_record_type_ == LogRecordType::COMMIT ||
                logRecord.log_record_type_ == LogRecordType::ABORT) {
                active_txn_.erase(logRecord.txn_id_);
                continue;
            }
            if (logRecord.log_record_type_ == LogRecordType::NEWPAGE) {
                auto page = static_cast<TablePage *>(
                        buffer_pool_manager_->FetchPage(logRecord.page_id_));

                //新申请的一页， 初始化信息，并赋给log record的lsn
                page->Init(logRecord.page_id_, PAGE_SIZE, logRecord.prev_page_id_, nullptr, nullptr);
                page->setPageLSN(logRecord.lsn_);
                //修改prev page的信息
                TablePage *prevPage = static_cast<TablePage *>(
                        buffer_pool_manager_->FetchPage(logRecord.prev_page_id_));
                //如果prev page有next page 则修改next page的头部信息

                prevPage->SetNextPageId(logRecord.page_id_);
                buffer_pool_manager_->UnpinPage( logRecord.prev_page_id_, true );
                buffer_pool_manager_->UnpinPage(logRecord.page_id_, true);
                continue;
            }
            //由log record获取record rid，再由record ID获取page ID
            RID tupleRid = logRecord.log_record_type_ == LogRecordType::INSERT ? logRecord.insert_rid_
                                                                               : logRecord.log_record_type_ ==
                                                                                 LogRecordType::UPDATE
                                                                                 ? logRecord.update_rid_
                                                                                 : logRecord.delete_rid_;
            TablePage *tablePage = static_cast<TablePage *>(
                    buffer_pool_manager_->FetchPage(tupleRid.GetPageId()));
            bool need_redo = logRecord.GetLSN() > tablePage->GetPageLSN();
            if (need_redo) {
                if (logRecord.log_record_type_ == LogRecordType::INSERT) {
                    tablePage->InsertTuple(logRecord.insert_tuple_, tupleRid,
                                           nullptr, nullptr, nullptr);
                } else if (logRecord.log_record_type_ == LogRecordType::UPDATE) {
                    tablePage->UpdateTuple(logRecord.old_tuple_, logRecord.new_tuple_, tupleRid,
                                           nullptr, nullptr, nullptr);
                } else if (logRecord.log_record_type_ == LogRecordType::MARKDELETE) {
                    tablePage->MarkDelete(tupleRid, nullptr, nullptr, nullptr);
                } else if (logRecord.log_record_type_ == LogRecordType::APPLYDELETE) {
                    tablePage->ApplyDelete(tupleRid, nullptr, nullptr);
                } else if (logRecord.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
                    tablePage->RollbackDelete(tupleRid, nullptr, nullptr);
                }
            }
            tablePage->setPageLSN(logRecord.GetLSN());
            buffer_pool_manager_->UnpinPage(tablePage->GetPageId(), need_redo);
        }
        //是否继续读取log file?
    }

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
    void LogRecovery::Undo() {
        for( auto &mapped_value : active_txn_ ){
            int travLsn = mapped_value.second;
            //每一个事务初始的lsn的prev lsn为INVALID
            while ( travLsn != INVALID_LSN ){
                //定位log buffer中的log record
                int log_off = lsn_mapping_[travLsn];
                LogRecord currentLogRecord;
                DeserializeLogRecord( log_buffer_ + log_off, currentLogRecord );//获取log record
                travLsn = currentLogRecord.GetPrevLSN();
                /*
                 * 执行Undo，事务处于未提交或者未回滚状态
                 * 从NEW PAGE状态开始Undo
                 */
                if( currentLogRecord.log_record_type_ == LogRecordType::NEWPAGE ){
                    //
                    buffer_pool_manager_->DeletePage( currentLogRecord.page_id_ );
                    disk_manager_->DeallocatePage( currentLogRecord.page_id_ );
                    if( currentLogRecord.prev_page_id_ != INVALID_PAGE_ID ) {
                        //修改prev page的信息
                        TablePage *prevPage = static_cast<TablePage *>(
                                buffer_pool_manager_->FetchPage(currentLogRecord.prev_page_id_));
                        //修改prev page的next page信息
                        prevPage->SetNextPageId(INVALID_PAGE_ID);
                        buffer_pool_manager_->UnpinPage(currentLogRecord.prev_page_id_, true);
                    }
                    continue;
                }
                RID tupleRid = currentLogRecord.log_record_type_ == LogRecordType::INSERT ? currentLogRecord.insert_rid_
                        : currentLogRecord.log_record_type_ == LogRecordType::UPDATE ? currentLogRecord.update_rid_
                        : currentLogRecord.delete_rid_;
                TablePage *tablePage = static_cast<TablePage *>(
                        buffer_pool_manager_->FetchPage(tupleRid.GetPageId()));
                if (currentLogRecord.log_record_type_ == LogRecordType::INSERT) {
                    tablePage->ApplyDelete( tupleRid, nullptr, nullptr );
                } else if (currentLogRecord.log_record_type_ == LogRecordType::UPDATE) {
                    tablePage->UpdateTuple(currentLogRecord.new_tuple_, currentLogRecord.old_tuple_, tupleRid,
                                           nullptr, nullptr, nullptr);
                } else if (currentLogRecord.log_record_type_ == LogRecordType::MARKDELETE) {
                    tablePage->RollbackDelete( tupleRid, nullptr, nullptr );
                } else if (currentLogRecord.log_record_type_ == LogRecordType::APPLYDELETE) {
                    tablePage->InsertTuple( currentLogRecord.delete_tuple_, tupleRid,
                                            nullptr, nullptr, nullptr );
                } else if (currentLogRecord.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
                    tablePage->MarkDelete( tupleRid, nullptr, nullptr, nullptr );
                }
                buffer_pool_manager_->UnpinPage(tablePage->GetPageId(), true );
            }
        }
        active_txn_.clear();
        lsn_mapping_.clear();
    }

} // namespace cmudb
