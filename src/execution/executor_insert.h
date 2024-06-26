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

class InsertExecutor : public AbstractExecutor {
  private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle *fh_;          // 表的数据文件句柄
    std::string tab_name_;      // 表名称
    Rid rid_; // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

  public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type != val.type && !colTypeCanHold(col.type, val.type)) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            // 尝试转换类型, 目前是 int -> float or float -> int
            val.try_cast_to(col.type);
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        // Insert into index
        std::vector<std::unique_ptr<RmRecord>> recs;
        for (auto &index : tab_.indexes) {
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char *key = new char[index.col_tot_len];
            int offset = 0;
            for (size_t i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }

            // check duplicate
            std::vector<Rid> _ret;
            if (ih->get_value(key, &_ret, context_->txn_)) {
                throw IndexKeyDuplicateError();
            }

            // ih->insert_entry(key, rid_, context_->txn_);
            recs.emplace_back(std::make_unique<RmRecord>(index.col_tot_len, key));
        }

        // Insert into record file
        rid_ = fh_->insert_record(rec.data, context_);

        // Insert into index
        for (int i = 0; i < recs.size(); ++i) {
            auto &rec = recs[i];
            auto &index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            ih->insert_entry(rec->data, rid_, context_->txn_);
        }

        // Operate Transaction
        if (context_->txn_->get_txn_mode()) {
            // auto write_record = WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_);
            // context_->txn_->append_write_record(write_record);
            WriteRecord *write_record = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_);
            context_->txn_->append_write_record(write_record);
        }

        return nullptr;
    }
    Rid &rid() override {
        return rid_;
    }

    ExecutorType getType() override {
        return ExecutorType::INSERT_EXECUTOR;
    }
};