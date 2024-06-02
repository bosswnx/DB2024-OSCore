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

class SeqScanExecutor : public AbstractExecutor {
private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_{};
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    [[nodiscard]] size_t tupleLen() const override{
        return len_;
    }

    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_;
    };


    void beginTuple() override {
        scan_ = std::make_unique<RmScan>(fh_);
        // 当前记录未消费，可能需要
        while (!is_end() && !evalConditions()) {      // 滑过不满足条件的记录
            scan_->next();
        }
    }

    void nextTuple() override {
        // 当前记录已经消费完了
        do {
            scan_->next();  // 滑过不满足条件的记录
        } while (!is_end() && !evalConditions());
    }

    bool evalConditions() {
        auto handle = fh_->get_record(scan_->rid(), context_);
        char *base = handle->data;
        // 逻辑不短路，目前只实现逻辑与
        return std::all_of(conds_.begin(), conds_.end(), [base, this](const Condition& cond) {
            auto value = Value::col2Value(base, get_col_offset(cond.lhs_col));
            return cond.eval_with_rvalue(value);
        });
    }

    ColMeta get_col_offset(const TabCol &target) override {
        auto it = std::find_if(cols_.begin(), cols_.end(), [&target](const ColMeta &col) {
            return col.name == target.col_name;
        });
        assert(it != cols_.end());
        return *it;
    }

    [[nodiscard]] bool is_end() const override {
        return scan_->is_end();
    }

    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(scan_->rid(), context_);
    }

    Rid &rid() override {
        rid_ = scan_->rid();    // TODO：没必要维护一个`rid_`跟踪`RmScan.rid_`的变化，目前删掉`rid_`需要改动接口，未来可以删掉`rid_`
        return rid_;
    }

    ExecutorType getType() override {
        return SEQ_SCAN_EXECUTOR;
    }
};