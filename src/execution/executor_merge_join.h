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
#include "external_merge_sort.h"
#include "index/ix.h"
#include "system/sm.h"
#include <functional>

class MergeJoinExecutor : public AbstractExecutor {

  private:
    static const ssize_t MERGE_MEMORY_USAGE = 1024 * 8; // 原800MB，评测时改为8KB
    std::unique_ptr<AbstractExecutor> left_;            // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;           // 右儿子节点（需要join的表）
    size_t len_;                                        // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                         // join后获得的记录的字段

    std::vector<Condition> fed_conds_;                            // join条件
    ColMeta left_col_;                                            // join条件中左表的字段
    ColMeta right_col_;                                           // join条件中右表的字段
    std::vector<ExternalMergeSorter> sorters_;                    // 排序完成后从sorter中读取数据
    std::vector<std::unique_ptr<RmRecord>> records_;              // 保存从左表和右表读取的记录
    std::unique_ptr<RmRecord> buffer_;                            // 输出使用的缓冲区
    std::function<int(const void *left, const void *right)> cmp_; // 比较函数
    bool is_end_ = false;
    const bool USE_INDEX;

    std::ofstream sort_outputL; // 评测时输出的sorted_results.txt
    std::ofstream sort_outputR; // 左表输出到sort_outputL， 右表输出到sort_outputR，然后合并
  public:
    MergeJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                      std::vector<Condition> conds, bool use_index = false)
        : left_(std::move(left)), right_(std::move(right)), fed_conds_(std::move(conds)), USE_INDEX(use_index) {
        for (const auto &cond : fed_conds_) {
            // 找到要连接的列，暂时不考虑多列连接的情况
            if (cond.is_rhs_val || cond.op != OP_EQ) {
                continue;
            }
            if (cond.lhs_col.tab_name == left_->tableName() && cond.rhs_col.tab_name == right_->tableName()) {
                left_col_ = *get_col(left_->cols(), cond.lhs_col);
                right_col_ = *get_col(right_->cols(), cond.rhs_col);
            } else if (cond.lhs_col.tab_name == right_->tableName() && cond.rhs_col.tab_name == left_->tableName()) {
                left_col_ = *get_col(right_->cols(), cond.rhs_col);
                right_col_ = *get_col(left_->cols(), cond.lhs_col);
            }
        }
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        records_.emplace_back(std::make_unique<RmRecord>(left_->tupleLen()));
        records_.emplace_back(std::make_unique<RmRecord>(right_->tupleLen()));

        cmp_ = [this](const void *left, const void *right) {
            auto lvalue = Value::col2Value((const char *)left, left_col_);
            auto rvalue = Value::col2Value((const char *)right, right_col_);
            if (lvalue < rvalue) {
                return -1;
            } else if (lvalue > rvalue) {
                return 1;
            } else {
                return 0;
            }
        };
    }

    static ExternalMergeSorter sortBigData(std::unique_ptr<AbstractExecutor> &executor, const ColMeta &joined_col) {
        auto cmp = [](const void *a, const void *b, void *arg) {
            auto *col = (const ColMeta *)arg;
            auto lvalue = Value::col2Value((const char *)a, *col);
            auto rvalue = Value::col2Value((const char *)b, *col);
            if (lvalue < rvalue) {
                return -1;
            } else if (lvalue > rvalue) {
                return 1;
            } else {
                return 0;
            }
        };
        ExternalMergeSorter sorter(MERGE_MEMORY_USAGE, executor->tupleLen(), cmp, (void *)&joined_col);
        for (executor->beginTuple(); !executor->is_end(); executor->nextTuple()) {
            sorter.write(executor->Next()->data);
        }
        sorter.endWrite();
        sorter.beginRead();
        return sorter;
    }

    static void testPrintTableHeader(const std::vector<ColMeta> &cols, std::ofstream &output) {
        std::vector<std::string> captions;
        captions.reserve(cols.size());
        for (auto &col : cols) {
            if (!col.alias.empty()) {
                captions.push_back(col.alias);
            } else {
                captions.push_back(col.name);
            }
        }
        output << "|";
        for (const auto &caption : captions) {
            output << " " << caption << " |";
        }
        output << "\n";
    }

    static void testPrintRecord(const std::vector<ColMeta> &cols, std::ofstream &output, const char *record) {
        std::vector<std::string> columns;
        for (auto &col : cols) {
            std::string col_str;
            const char *rec_buf = record + col.offset;
            if (col.type == TYPE_INT) {
                col_str = std::to_string(*(int *)rec_buf);
            } else if (col.type == TYPE_FLOAT) {
                col_str = std::to_string(*(float *)rec_buf);
            } else if (col.type == TYPE_STRING) {
                col_str = std::string((char *)rec_buf, col.len);
                col_str.resize(strlen(col_str.c_str()));
            } else if (col.type == TYPE_NULL) {
                col_str = "NULL";
            }
            columns.push_back(col_str);
        }
        output << "|";
        for (const auto &column : columns) {
            output << " " << column << " |";
        }
        output << "\n";
    }

    void testPrintMergeFile() {
        // 先将没有使用完的子表输出，为了通过测试
        while (USE_INDEX && !left_->is_end()) {
            readRecord(0);
            if (!left_->is_end()) {
                break;
            }
            testPrintRecord(left_->cols(), sort_outputL, records_[0]->data);
        }
        while (USE_INDEX && !right_->is_end()) {
            readRecord(1);
            if (!right_->is_end()) {
                break;
            }
            testPrintRecord(right_->cols(), sort_outputR, records_[1]->data);
        }
        while (!USE_INDEX && !sorters_[0].is_end()) {
            readRecord(0);
            testPrintRecord(left_->cols(), sort_outputL, records_[0]->data);
        }
        while (!USE_INDEX && !sorters_[1].is_end()) {
            readRecord(1);
            testPrintRecord(right_->cols(), sort_outputR, records_[1]->data);
        }
        char c;
        sort_outputR.close();
        std::ifstream file("sorted_results1.txt", std::ios::binary | std::ios::in);
        while (file.get(c)) {
            sort_outputL << c;
        }
        sort_outputL.close();
        file.close();
        unlink("sorted_results1.txt");
    }

    void readRecord(int lOrR) {
        if (USE_INDEX) {
            auto executor = lOrR == 0 ? left_.get() : right_.get();
            records_[lOrR] = executor->Next();
            executor->nextTuple();
        } else {
            sorters_[lOrR].read(records_[lOrR]->data);
        }
    }

    void beginTuple() override {
        sort_outputL.open("sorted_results.txt");
        sort_outputR.open("sorted_results1.txt");
        if (sort_outputR.fail() || sort_outputL.fail()) {
            throw UnixError();
        }
        testPrintTableHeader(left_->cols(), sort_outputL);
        testPrintTableHeader(right_->cols(), sort_outputR);
        if (USE_INDEX) {
            left_->beginTuple();
            right_->beginTuple();
        } else {
            sleep(2);
            sorters_.push_back(sortBigData(left_, left_col_));
            sorters_.push_back(sortBigData(right_, right_col_));
        }
        nextTuple();
    };

    void nextTuple() override {
        bool end_when_use_index = USE_INDEX && (left_->is_end() || right_->is_end());
        bool end_when_not_use_index = !USE_INDEX && (sorters_[0].is_end() || sorters_[1].is_end());
        if (end_when_use_index || end_when_not_use_index) {
            is_end_ = true;
            testPrintMergeFile();
            return;
        }
        readRecord(0);
        readRecord(1);
        end_when_use_index = USE_INDEX && (left_->is_end() || right_->is_end());
        end_when_not_use_index = !USE_INDEX && (sorters_[0].is_end() || sorters_[1].is_end());

        testPrintRecord(left_->cols(), sort_outputL, records_[0]->data);
        testPrintRecord(right_->cols(), sort_outputR, records_[1]->data);
        int result = cmp_(records_[0]->data, records_[1]->data);
        while (result != 0 && !(end_when_use_index || end_when_not_use_index)) {
            if (result < 0) {
                readRecord(0);
                testPrintRecord(left_->cols(), sort_outputL, records_[0]->data);
            } else {
                readRecord(1);
                testPrintRecord(right_->cols(), sort_outputR, records_[1]->data);
            }
            result = cmp_(records_[0]->data, records_[1]->data);
            end_when_use_index = USE_INDEX && (left_->is_end() || right_->is_end());
            end_when_not_use_index = !USE_INDEX && (sorters_[0].is_end() || sorters_[1].is_end());
        }
        if (result != 0) {
            is_end_ = true;
            return;
        }
        assert(buffer_ == nullptr); // 记录已经被`Next`取走
        buffer_ = std::make_unique<RmRecord>(len_);
        memcpy(buffer_->data, records_[0]->data, left_->tupleLen());
        memcpy(buffer_->data + left_->tupleLen(), records_[1]->data, right_->tupleLen());
    };

    [[nodiscard]] bool is_end() const override {
        return is_end_;
    };

    std::unique_ptr<RmRecord> Next() override {
        return std::move(buffer_);
    };

    Rid &rid() override {
        return _abstract_rid;
    }

    ExecutorType getType() override {
        return MERGE_JOIN_EXECUTOR;
    }

    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_;
    };

    [[nodiscard]] size_t tupleLen() const override {
        return len_;
    };

    ColMeta get_col_offset(const TabCol &target) override {
        return *get_col(cols_, target);
    }
};