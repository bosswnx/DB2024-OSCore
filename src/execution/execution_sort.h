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

class SortExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta cols_;                              // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;
    bool is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;
    std::unique_ptr<ExternalMergeSorter> sorter;
    std::unique_ptr<RmRecord> buffer;
    bool is_end_ = false;

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const TabCol& sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->get_col_offset(sel_cols);
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
        auto cmp = [](const void *a, const void *b, void *arg) {
            auto col = (ColMeta *) arg;
            auto lvalue = Value::col2Value((const char *) a, *col);
            auto rvalue = Value::col2Value((const char *) b, *col);
            if (lvalue < rvalue) {
                return -1;
            } else if (lvalue > rvalue) {
                return 1;
            } else {
                return 0;
            }
        };
        sorter = std::make_unique<ExternalMergeSorter>(1024 * 1024 * 800, prev_->tupleLen(), cmp, &cols_);
    }

    void beginTuple() override {
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            sorter->write(prev_->Next()->data);
        }
        sorter->endWrite();
        sorter->beginRead();
        nextTuple();
    }

    void nextTuple() override {
        if(sorter->is_end()){
            is_end_ = true;
            return;
        }
        assert(buffer == nullptr);      // 保证上次的记录已经被`Next`取走
        buffer = std::make_unique<RmRecord>(prev_->tupleLen());
        sorter->read(buffer->data);
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(buffer);
    }

    [[nodiscard]] bool is_end() const override{
        return is_end_;
    }

    Rid &rid() override { return _abstract_rid; }

    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return prev_->cols();
    };

    ExecutorType getType() override { return ExecutorType::SORT_EXECUTOR; }
};