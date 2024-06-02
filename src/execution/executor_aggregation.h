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

class AggregationExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    size_t len_;
    std::vector<Condition> having_conds_;
    
    std::vector<ColMeta> group_cols_;

    std::vector<ColMeta> sel_cols_;
    std::vector<ColMeta> sel_cols_initial_;

    std::map<std::string, int> grouped_records_idx_;
    std::vector<std::vector<std::unique_ptr<RmRecord>>> grouped_records_;

    int curr_idx = -1; // 用于遍历    
    std::vector<std::unique_ptr<RmRecord>> curr_records;

    bool empty_table_aggr_ = false;

   public:
    AggregationExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols, 
        const std::vector<TabCol> &group_cols, const std::vector<Condition> &having_conds) {
        prev_ = std::move(prev);

        auto &prev_cols = prev_->cols();    // 获取上一个算子的列信息

        for (auto &sel_group_col : group_cols) {
            auto pos = get_col(prev_cols, sel_group_col, false);
            group_cols_.push_back(*pos);
        }

        for (auto &sel_col : sel_cols) {
            if (sel_col.aggr == ast::AGGR_TYPE_COUNT && sel_col.col_name == "*") {
                ColMeta col = make_count_star_col(sel_col);
                sel_cols_initial_.push_back(col);
                sel_cols_.push_back(col);
                continue;
            }
            auto pos = get_col(prev_cols, sel_col);
            auto col = *pos;
            col.aggr = sel_col.aggr;
            sel_cols_initial_.push_back(col);
            size_t p = pos - prev_cols.begin();
            if (col.aggr != ast::NO_AGGR) {
                if (sel_col.aggr == ast::AGGR_TYPE_COUNT) {
                    col.type = TYPE_INT;
                    col.len = sizeof(int);
                } else if(sel_col.aggr == ast::AGGR_TYPE_SUM) {
                    if (col.type == TYPE_STRING) {
                        col.type = TYPE_INT;
                        col.len = sizeof(int);
                    }
                }
            }

            sel_cols_.push_back(col);
        }

        size_t offset = 0;
        for (auto &col : sel_cols_) {
            col.offset = offset;
            offset += col.len;
        }
        len_ = offset;
        
        having_conds_ = having_conds;
    }

    void store_group(std::unique_ptr<RmRecord> record) {
        /*
        * 计算 group by 用到的 key，并存储到 grouped_records_ 中
        */
        std::string key = "";
        for (auto group_col : group_cols_) {
            key += std::string(record->data + group_col.offset, group_col.len);
        }
        if (grouped_records_idx_.find(key) == grouped_records_idx_.end()) {
            grouped_records_idx_[key] = grouped_records_.size();
            grouped_records_.push_back(std::vector<std::unique_ptr<RmRecord>>());
        }
        grouped_records_[grouped_records_idx_[key]].push_back(std::move(record));
    }

    void beginTuple() override {
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto record = prev_->Next();
            store_group(std::move(record));
        }
        nextTuple();
    }

    void nextTuple() override {
        do {
            if (grouped_records_.empty() && group_cols_.empty()) {
                if (!empty_table_aggr_) {
                    empty_table_aggr_ = true;
                    return;
                }
            }
            ++curr_idx;
            if (is_end()) return;
            // curr_records = std::move(grouped_records_[curr_idx]);
            auto &grouped_records = grouped_records_[curr_idx];
            curr_records.clear();
            for (auto &record : grouped_records) {
                curr_records.push_back(std::move(record));
            }
        } while (!evalConditions());
    }

    bool evalConditions() {
        std::vector<int> to_delete;
        int i = 0;
        for (auto &record : curr_records) {
            auto handle = record.get();
            char *base = handle->data;
            // 逻辑不短路，目前只实现逻辑与
            if (!std::all_of(having_conds_.begin(), having_conds_.end(), [base, this](const Condition& cond) {
                if(cond.lhs_col.aggr == ast::NO_AGGR) {
                    auto value = Value::col2Value(base, *get_col(sel_cols_initial_, cond.lhs_col, true));
                    return cond.eval_with_rvalue(value);
                } else {
                    ColMeta col_meta;
                    if (cond.lhs_col.aggr == ast::AGGR_TYPE_COUNT && cond.lhs_col.col_name == "*") {
                        col_meta = make_count_star_col(cond.lhs_col);
                    } else {
                        col_meta = *get_col(sel_cols_initial_, cond.lhs_col, true);
                    }
                    auto value = aggregate_value(col_meta);
                    return cond.eval_with_rvalue(value);
                }
            })) {
                to_delete.push_back(i);
            }
            ++i;
        }
        if (to_delete.size() == curr_records.size()) return false;
        for (int i = to_delete.size() - 1; i >= 0; --i) {
            curr_records.erase(curr_records.begin() + to_delete[i]);
        }
        return true;
    }

    ColMeta make_count_star_col(TabCol c) {
        ColMeta col;
        col.name = "*";
        col.tab_name = "";
        col.alias = c.alias;
        col.type = TYPE_INT;
        col.len = sizeof(int);
        col.offset = 0;
        col.aggr = ast::AGGR_TYPE_COUNT;
        return col;
    }

    Value aggregate_value(ColMeta sel_col) {
        Value val;
        // 处理空表的情况
        if (curr_records.empty()) {
            val.type = sel_col.type;
            val.init_raw(sel_col.len);
            val.type = TYPE_NULL;
            return val;
        }
        switch (sel_col.aggr) {
            case ast::NO_AGGR:
                val = Value::col2Value(curr_records[0]->data, sel_col);
                val.init_raw(sel_col.len);
                break;
            case ast::AGGR_TYPE_COUNT:
                val.type = TYPE_INT;
                val.set_int(curr_records.size());
                val.init_raw(sizeof(int));
                break;
            case ast::AGGR_TYPE_MAX:
                val.type = sel_col.type;
                val = Value::col2Value(curr_records[0]->data, sel_col);
                for (auto &record : curr_records) {
                    Value tmp = Value::col2Value(record->data, sel_col);
                    if (tmp > val) val = tmp;
                }
                val.init_raw(sel_col.len);
                break;
            case ast::AGGR_TYPE_MIN:
                val.type = sel_col.type;
                val = Value::col2Value(curr_records[0]->data, sel_col);
                for (auto &record : curr_records) {
                    Value tmp = Value::col2Value(record->data, sel_col);
                    if (tmp < val) val = tmp;
                }
                val.init_raw(sel_col.len);
                break;
            case ast::AGGR_TYPE_SUM:
                val.type = sel_col.type;
                if (sel_col.type == TYPE_INT) {
                    int sum = 0;
                    for (auto &record : curr_records) {
                        sum += *(int *)(record->data + sel_col.offset);
                    }
                    val.set_int(sum);
                    val.init_raw(sizeof(int));
                } else if (sel_col.type == TYPE_FLOAT) {
                    float sum = 0;
                    for (auto &record : curr_records) {
                        sum += *(float *)(record->data + sel_col.offset);
                    }
                    val.set_float(sum);
                    val.init_raw(sizeof(float));
                } else if (sel_col.type == TYPE_STRING) {
                    // 字符串的sum
                    val.type = TYPE_INT;
                    std::string sum;
                    float sum_float = 0;
                    bool is_float = false;

                    for (auto &record : curr_records) {
                        const char* base = record->data + sel_col.offset;
                        sum = "";
                        for (size_t i=0; i<sel_col.len; ++i) {
                            if (base[i] >= '0' && base[i] <= '9') sum += base[i];
                            else if (base[i] == '.'){
                                sum += base[i];
                                is_float = true;
                            }
                            else break;
                        }
                        if (sum.size() == 0) continue;
                        sum_float += std::stof(sum);
                    }
                    if (is_float) {
                        val.set_float(sum_float);
                        val.type = TYPE_FLOAT;
                        val.init_raw(sizeof(float));
                    } else {
                        val.set_int((int)sum_float);
                        val.init_raw(sizeof(int));
                    }
                } else {
                    throw InternalError("Unknown AggrType");
                }
                break;
            
            default:
                throw InternalError("Unknown AggrType");
        }
        return val;
    }

    [[nodiscard]] bool is_end() const override{
        return curr_idx >= (int)grouped_records_.size();
    }

    [[nodiscard]] const std::vector<ColMeta> &cols() const override{
        return sel_cols_;
    }

    std::unique_ptr<RmRecord> Next() override {
        std::vector<Value> values;

        int idx = 0;
        for (auto sel_col : sel_cols_initial_) {
            Value val = aggregate_value(sel_col);
            if (val.type == TYPE_NULL) {
                // 将 sel_cols 中对应的TYPE改成NULl
                sel_cols_[idx].type = TYPE_NULL; // 为了能够正常识别到。
            }
            values.push_back(val);
            ++idx;
        }

        auto data = std::make_unique<char[]>(len_);
        // 拷贝到 data 中
        size_t offset = 0;
        size_t i = 0;
        for (auto col : sel_cols_) {
            memcpy(data.get() + offset, values[i].raw->data, values[i].raw->size);
            ++i;
            offset += col.len;
        }
        return std::make_unique<RmRecord>(len_, data.get());
    }

    Rid &rid() override { return _abstract_rid; }

    ExecutorType getType() override {
        return ExecutorType::AGGREGATION_EXECUTOR;
    }
};