/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    RmFileHandle *ih_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses);
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        int record_size = fh_->get_file_hdr().record_size;
        // auto buf = std::make_unique<char[]>(record_size);
        // std::vector<std::unique_ptr<char[]>> bufs;
        // NOTE: 按照 MySQL，这里本应当是一个事务，因为需要检测唯一索引是否有重复的记录。现在的实现没有考虑在检测到重复的时候回滚，而是直接抛出异常，原有的数据不会被修改回去。

        for (const auto &rid: rids_) {
            auto buf = std::make_unique<char[]>(record_size);
            auto record = fh_->get_record(rid, context_);
            memcpy(buf.get(), record->data, record_size);
            for (auto &clause: set_clauses_) {
                auto col = tab_.get_col(clause.lhs.col_name);
                clause.rhs.init_raw(col->len);
                memcpy(buf.get() + col->offset, clause.rhs.raw->data, col->len);
            }
            
            // Update index
            std::vector<std::unique_ptr<RmRecord>> old_keys;
            std::vector<std::unique_ptr<RmRecord>> new_keys;
            for (auto &index: tab_.indexes) {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char *key_old = new char[index.col_tot_len];
                char *key_new = new char[index.col_tot_len];
                size_t offset = 0;
                for (int i = 0; i < index.col_num; i++) {
                    auto col = tab_.get_col(index.cols[i].name);
                    memcpy(key_old + offset, record->data + col->offset, col->len);
                    memcpy(key_new + offset, buf.get() + col->offset, col->len);
                    offset += col->len;
                }

                // check duplicate
                std::vector<Rid> _ret;
                if (ih->get_value(key_new, &_ret, context_->txn_)) {
                    throw IndexKeyDuplicateError();
                }

                old_keys.emplace_back(std::make_unique<RmRecord>(index.col_tot_len, key_old));
                new_keys.emplace_back(std::make_unique<RmRecord>(index.col_tot_len, key_new));
            }

            for (int i = 0; i < tab_.indexes.size(); i++) {
                auto &index = tab_.indexes[i];
                auto &old_key = old_keys[i];
                auto &new_key = new_keys[i];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                ih->delete_entry(old_key->data, context_->txn_);
                ih->insert_entry(new_key->data, rid, context_->txn_);
            }

            fh_->update_record(rid, buf.get(), context_);

            // bufs.emplace_back(std::move(buf));
        }

        // for (int i = 0; i < rids_.size(); i++) {
        //     auto &rid = rids_[i];
        //     auto &buf = bufs[i];

        //     // insert index
        //     for (int j = 0; j < tab_.indexes.size(); j++) {
        //         auto &index = tab_.indexes[j];
        //         auto &old_key = old_keys[i*tab_.indexes.size() + j];
        //         auto &new_key = new_keys[i*tab_.indexes.size() + j];
        //         auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        //         ih->delete_entry(old_key->data, context_->txn_);
        //         ih->insert_entry(new_key->data, rid, context_->txn_);
        //     }

        //     fh_->update_record(rid, buf.get(), context_);
        // }

        // free memory
        // for (auto &buf: bufs) {
        //     buf.reset();
        // }
        // for (auto &key: old_keys) {
        //     key.reset();
        // }
        // for (auto &key: new_keys) {
        //     key.reset();
        // }

        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    ExecutorType getType() override { return ExecutorType::UPDATE_EXECUTOR; }
};