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

class DeleteExecutor : public AbstractExecutor {
  private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Condition> conds_; // delete的条件
    RmFileHandle *fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::string tab_name_;         // 表名称
    SmManager *sm_manager_;

  public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        for (const Rid &rid : rids_) {

            // Update index
            for (auto &index : tab_.indexes) {
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char *key = new char[index.col_tot_len];
                auto record = fh_->get_record(rid, context_);
                int offset = 0;
                for (int i = 0; i < index.col_num; i++) {
                    auto col = tab_.get_col(index.cols[i].name);
                    memcpy(key + offset, record->data + col->offset, col->len);
                    offset += col->len;
                }
                ih->delete_entry(key, context_->txn_);
                delete[] key;
            }

            // Operate Transaction
            if (context_->txn_->get_txn_mode()) {
                // auto write_record = WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *fh_->get_record(rid,
                // context_)); context_->txn_->append_write_record(&write_record);
                WriteRecord *write_record =
                    new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *fh_->get_record(rid, context_));
                context_->txn_->append_write_record(write_record);
            }

            fh_->delete_record(rid, context_);
        }
        return nullptr;
    }

    Rid &rid() override {
        return _abstract_rid;
    }

    ExecutorType getType() override {
        return ExecutorType::DELETE_EXECUTOR;
    }
};
