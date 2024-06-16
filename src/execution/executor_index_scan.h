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

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    IxIndexHandle *ih_;
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同
    std::vector<Condition> index_conds_;

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;



   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        ih_ = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;

        // for (auto &cond : conds_) {
        //     if (cond.lhs_col.tab_name == tab_name_ && index_meta_.has_col(cond.lhs_col.col_name)) {
        //         index_conds_.push_back(cond);
        //     }
        // }
        for (auto &col_name : index_col_names_) {
            for (auto &cond : conds_) {
                if (cond.lhs_col.col_name == col_name) {
                    index_conds_.push_back(cond);
                }
            }
        }
    }

    void beginTuple() override {
        // 索引扫描的实现
        // 1. 根据条件找到索引的起始位置
        // 2. 从起始位置开始扫描索引，找到满足条件的记录
        // 3. 从数据文件中读取记录
        // 4. 返回记录

        auto lower_k = new char[index_meta_.col_tot_len];
        auto upper_k = new char[index_meta_.col_tot_len];
        // 根据条件填充lower_k和upper_k
        size_t offset = 0;
        for (int i=0; i<index_col_names_.size(); ++i) {
            Value val;
            if (i<index_conds_.size()) {
                // 该索引col有条件
                auto &cond = index_conds_[i];
                auto col_meta = *tab_.get_col(cond.lhs_col.col_name);
                switch (cond.op) {
                    case OP_NE:
                        // lower_k 是最小值
                        val = Value::makeEdgeValue(col_meta.type, col_meta.len, false);
                        memcpy(lower_k + offset, val.raw->data, col_meta.len);
                        // upper_k 是最大值
                        val = Value::makeEdgeValue(col_meta.type, col_meta.len, true);
                        memcpy(upper_k + offset, val.raw->data, col_meta.len);
                        break;
                    case OP_EQ:
                        memcpy(lower_k + offset, cond.rhs_val.raw->data, col_meta.len);
                        memcpy(upper_k + offset, cond.rhs_val.raw->data, col_meta.len);
                        break;
                    case OP_LE:
                    case OP_LT:
                        // lower_k 是最小值
                        val = Value::makeEdgeValue(col_meta.type, col_meta.len, false);
                        memcpy(lower_k + offset, val.raw->data, col_meta.len);
                        memcpy(upper_k + offset, cond.rhs_val.raw->data, col_meta.len);
                        break;
                    case OP_GE:
                    case OP_GT:                    
                        // upper_k 是最大值
                        val = Value::makeEdgeValue(col_meta.type, col_meta.len, true);
                        memcpy(lower_k + offset, cond.rhs_val.raw->data, col_meta.len);
                        memcpy(upper_k + offset, val.raw->data, col_meta.len);
                        break;
                    default:
                        assert(false);
                }
            } else {
                // 该索引col没有条件，直接用最小值和最大值
                auto col_meta = *tab_.get_col(index_col_names_[i]);
                val = Value::makeEdgeValue(col_meta.type, col_meta.len, false);
                memcpy(lower_k + offset, val.raw->data, col_meta.len);
                val = Value::makeEdgeValue(col_meta.type, col_meta.len, true);
                memcpy(upper_k + offset, val.raw->data, col_meta.len);
            }
            offset += tab_.get_col(index_col_names_[i])->len;
        }

        Iid lower_iid = ih_->lower_bound(lower_k);
        Iid upper_iid = ih_->upper_bound(upper_k);

        scan_ = std::make_unique<IxScan>(ih_, lower_iid, upper_iid, ih_->get_buffer_pool_manager());

        // 查看是否有符合的
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            if (evalConditions()) break;
            scan_->next();
        }
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


    void nextTuple() override {
        if (scan_->is_end()) return;
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            if (evalConditions()) break;
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_, context_);
    }

    [[nodiscard]] bool is_end() const override {
        return scan_->is_end();
    }

    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_;
    };

    Rid &rid() override { return rid_; }

    ExecutorType getType() override { return ExecutorType::INDEX_SCAN_EXECUTOR; }
};