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

class NestedLoopJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    std::unique_ptr<RmRecord> result;           // 存储当前迭代轮次的值，供`Next`取走
    std::vector<std::unique_ptr<RmRecord>> left_record;     // seqScan扫描出的左表记录
    std::vector<std::unique_ptr<RmRecord>> right_record;    // seqScan扫描出的右表记录
    std::vector<std::unique_ptr<RmRecord>>::iterator lit;
    std::vector<std::unique_ptr<RmRecord>>::iterator rit;   // `rit`和`lit`组成按照排列组合顺序遍历左右表记录的cursor
    bool isend;

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col: right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);
    }

    void beginTuple() override {
        for (left_->beginTuple(); !left_->is_end(); left_->nextTuple()) {
            left_record.push_back(left_->Next());
        }
        for (right_->beginTuple(); !right_->is_end(); right_->nextTuple()) {
            right_record.push_back(right_->Next());
        }
        lit = left_record.begin();      // `push_back`使得迭代器失效，必须放在`push_back`后面
        rit = right_record.begin();
        isend = lit == left_record.end() || rit == right_record.end();  // 避免左右表为空的情况
        while (!isend && !evalConditions()) {     // 滑过不满足条件的记录
            step();
        }
        if (isend) {      // 没有任何满足条件的记录
            return;
        }
        result = std::make_unique<RmRecord>(len_);
        memcpy(result->data, (*lit)->data, left_->tupleLen());
        memcpy(result->data + left_->tupleLen(), (*rit)->data, right_->tupleLen());
    }

    void nextTuple() override {
        assert(!is_end());
        do {         // 滑过不满足条件的记录
            step();
        } while (!isend && !evalConditions());
        if (isend) {      // 滑到末尾
            return;
        }
        // 默认内连接，将左表和右表中满足条件的进行排列组合
        assert(result == nullptr);      // 检查迭代后是否把值取出
        result = std::make_unique<RmRecord>(len_);
        memcpy(result->data, (*lit)->data, left_->tupleLen());
        memcpy(result->data + left_->tupleLen(), (*rit)->data, right_->tupleLen());

    }

    [[nodiscard]] bool is_end() const override {
        return isend;
    }

    /// 嵌套循环中移动内部cursor的帮助函数
    void step() {
        rit++;
        if (rit == right_record.end()) {      // rewind
            lit++;
            rit = right_record.begin();
            if (lit == left_record.end()) {
                isend = true;   // 迭代到达终点
                return;
            }
        }
    }

    bool evalConditions() {
        char *lbase = (*lit)->data;
        char *rbase = (*rit)->data;
        return std::all_of(fed_conds_.begin(), fed_conds_.end(), [&](Condition &cond) {
            assert(!cond.is_rhs_val);
            auto lvalue = Value::col2Value(lbase, get_col_offset_lr(left_->cols(), cond.lhs_col));
            auto rvalue = Value::col2Value(rbase, get_col_offset_lr(right_->cols(), cond.rhs_col));
            return cond.eval(lvalue, rvalue);
        });
    }

    static ColMeta get_col_offset_lr(const std::vector<ColMeta> &cols, const TabCol &target) {
        auto it = std::find_if(cols.begin(), cols.end(), [&target](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        assert(it != cols.end());
        return *it;
    }

    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(result);
    }

    Rid &rid() override { return _abstract_rid; }

    ExecutorType getType() override {
        return NESTEDLOOP_JOIN_EXECUTOR;
    }
};