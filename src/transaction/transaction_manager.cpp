/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针

    if (txn == nullptr) {
        txn = new Transaction(txn_id_t(txn_map.size()));
    }

    std::unique_lock<std::mutex> lock(latch_);
    txn_map[txn->get_transaction_id()] = txn;
    lock.unlock();

    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    // 1. 如果存在未提交的写操作，提交所有的写操作
    if (txn->get_write_set()->size() > 0) {
        // for (auto write_record : *(txn->get_write_set())) {
        //     auto fh_ = sm_manager_->fhs_.at(write_record->GetTableName()).get();
        //     // auto ih_ = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(write_record->GetTableName(), write_record->GetIndexColNames())).get();
        //     // fh_->insert_record(write_record->GetRid(), write_record->GetRecord().data);

        //     if (write_record->GetWriteType() == WType::INSERT_TUPLE) {
        //         fh_->insert_record(write_record->GetRid(), write_record->GetRecord().data);
        //         // insert index
        //         for (auto &index : sm_manager_->db_.get_table(write_record->GetTableName()).indexes) {
        //             auto index_name = sm_manager_->get_ix_manager()->get_index_name(write_record->GetTableName(), index.cols);
        //             auto ih = sm_manager_->ihs_.at(index_name).get();
        //             char *key = new char[index.col_tot_len];
        //             for (int i = 0; i < index.col_num; i++) {
        //                 auto col = sm_manager_->db_.get_table(write_record->GetTableName()).get_col(index.cols[i].name);
        //                 memcpy(key + index.cols[i].offset, write_record->GetRecord().data + col->offset, col->len);
        //             }
        //             ih->insert_entry(key, write_record->GetRid(), nullptr);
        //             delete[] key;
        //         }

        //     } else if (write_record->GetWriteType() == WType::DELETE_TUPLE) {
        //         fh_->delete_record(write_record->GetRid(), nullptr);
        //         // delete index
        //         for (auto &index : sm_manager_->db_.get_table(write_record->GetTableName()).indexes) {
        //             auto index_name = sm_manager_->get_ix_manager()->get_index_name(write_record->GetTableName(), index.cols);
        //             auto ih = sm_manager_->ihs_.at(index_name).get();
        //             char *key = new char[index.col_tot_len];
        //             for (int i = 0; i < index.col_num; i++) {
        //                 auto col = sm_manager_->db_.get_table(write_record->GetTableName()).get_col(index.cols[i].name);
        //                 memcpy(key + index.cols[i].offset, write_record->GetRecord().data + col->offset, col->len);
        //             }
        //             ih->delete_entry(key, nullptr);
        //             delete[] key;
        //         }
        //     } else if (write_record->GetWriteType() == WType::UPDATE_TUPLE) {
        //         fh_->update_record(write_record->GetRid(), write_record->GetRecord().data, nullptr);
        //         // update index
        //         for (auto &index : sm_manager_->db_.get_table(write_record->GetTableName()).indexes) {
        //             auto index_name = sm_manager_->get_ix_manager()->get_index_name(write_record->GetTableName(), index.cols);
        //             auto ih = sm_manager_->ihs_.at(index_name).get();
        //             char *key_old = new char[index.col_tot_len];
        //             char *key_new = new char[index.col_tot_len];
        //             size_t offset = 0;
        //             for (int i = 0; i < index.col_num; i++) {
        //                 auto col = sm_manager_->db_.get_table(write_record->GetTableName()).get_col(index.cols[i].name);
        //                 memcpy(key_old + offset, write_record->GetOldRecord().data + col->offset, col->len);
        //                 memcpy(key_new + offset, write_record->GetRecord().data + col->offset, col->len);
        //                 offset += col->len;
        //             }

        //             if (memcmp(key_old, key_new, index.col_tot_len) == 0) {
        //                 // 如果old_key和new_key相同，说明没有修改索引列
        //                 delete[] key_old;
        //                 delete[] key_new;
        //                 continue;
        //             }

        //             ih->delete_entry(key_old, nullptr);
        //             ih->insert_entry(key_new, write_record->GetRid(), nullptr);
        //             delete[] key_old;
        //             delete[] key_new;
        //         }
        //     }
        // }
    }

    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (auto lock_data_id : *lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }

    // 3. 释放事务相关资源，eg.锁集
    txn->get_lock_set()->clear();

    // 4. 把事务日志刷入磁盘中
    log_manager->flush_log_to_disk();

    // 5. 更新事务状态
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    // 1. 回滚所有写操作
    if (txn->get_write_set()->size() > 0) {
        // for (auto write_record : *(txn->get_write_set())) {
            // 倒序
        for (auto write_record_ = txn->get_write_set()->rbegin(); write_record_ != txn->get_write_set()->rend(); write_record_++) {
            auto write_record = *write_record_;
            auto fh_ = sm_manager_->fhs_.at(write_record->GetTableName()).get();

            if (write_record->GetWriteType() == WType::INSERT_TUPLE) {
                auto record = fh_->get_record(write_record->GetRid(), nullptr);

                // delete index
                for (auto &index : sm_manager_->db_.get_table(write_record->GetTableName()).indexes) {
                    auto index_name = sm_manager_->get_ix_manager()->get_index_name(write_record->GetTableName(), index.cols);
                    auto ih = sm_manager_->ihs_.at(index_name).get();
                    char *key = new char[index.col_tot_len];
                    int offset = 0;
                    for (int i = 0; i < index.col_num; i++) {
                        auto col = sm_manager_->db_.get_table(write_record->GetTableName()).get_col(index.cols[i].name);
                        memcpy(key + offset, record->data + col->offset, col->len);
                        offset += col->len;
                    }
                    ih->delete_entry(key, nullptr);
                    delete[] key;
                }

                // delete
                fh_->delete_record(write_record->GetRid(), nullptr);
            } else if (write_record->GetWriteType() == WType::DELETE_TUPLE) {
                // insert
                fh_->insert_record(write_record->GetRid(), write_record->GetRecord().data);
                // insert index
                for (auto &index : sm_manager_->db_.get_table(write_record->GetTableName()).indexes) {
                    auto index_name = sm_manager_->get_ix_manager()->get_index_name(write_record->GetTableName(), index.cols);
                    auto ih = sm_manager_->ihs_.at(index_name).get();
                    char *key = new char[index.col_tot_len];
                    int offset = 0;
                    for (int i = 0; i < index.col_num; i++) {
                        auto col = sm_manager_->db_.get_table(write_record->GetTableName()).get_col(index.cols[i].name);
                        memcpy(key + offset, write_record->GetRecord().data + col->offset, col->len);
                        offset += col->len;
                    }
                    ih->insert_entry(key, write_record->GetRid(), nullptr);
                    delete[] key;
                }
            } else if (write_record->GetWriteType() == WType::UPDATE_TUPLE) {
                // update
                fh_->update_record(write_record->GetRid(), write_record->GetOldRecord().data, nullptr);
                // update index
                for (auto &index : sm_manager_->db_.get_table(write_record->GetTableName()).indexes) {
                    auto index_name = sm_manager_->get_ix_manager()->get_index_name(write_record->GetTableName(), index.cols);
                    auto ih = sm_manager_->ihs_.at(index_name).get();
                    char *key_old = new char[index.col_tot_len];
                    char *key_new = new char[index.col_tot_len];
                    size_t offset = 0;
                    for (int i = 0; i < index.col_num; i++) {
                        auto col = sm_manager_->db_.get_table(write_record->GetTableName()).get_col(index.cols[i].name);
                        memcpy(key_old + offset, write_record->GetOldRecord().data + col->offset, col->len);
                        memcpy(key_new + offset, write_record->GetRecord().data + col->offset, col->len);
                        offset += col->len;
                    }

                    if (memcmp(key_old, key_new, index.col_tot_len) == 0) {
                        // 如果old_key和new_key相同，说明没有修改索引列
                        delete[] key_old;
                        delete[] key_new;
                        continue;
                    }

                    ih->delete_entry(key_new, nullptr);
                    ih->insert_entry(key_old, write_record->GetRid(), nullptr);
                    delete[] key_old;
                    delete[] key_new;
                }
            }
        }
    }

    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();

    for (auto lock_data_id : *lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }

    // 3. 清空事务相关资源，eg.锁集
    txn->get_lock_set()->clear();
    txn->get_write_set()->clear();
    txn->get_index_deleted_page_set()->clear();
    txn->get_index_latch_page_set()->clear();

    // 4. 把事务日志刷入磁盘中
    log_manager->flush_log_to_disk();

    // 5. 更新事务状态
    txn->set_state(TransactionState::ABORTED);
    
}