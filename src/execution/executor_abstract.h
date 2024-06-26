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

#include "common/common.h"
#include "execution_defs.h"
#include "index/ix.h"
#include "system/sm.h"

enum ExecutorType {
    AGGREGATION_EXECUTOR,
    DELETE_EXECUTOR,
    PROJECTION_EXECUTOR,
    SEQ_SCAN_EXECUTOR,
    UPDATE_EXECUTOR,
    NESTEDLOOP_JOIN_EXECUTOR,
    MERGE_JOIN_EXECUTOR,
    SORT_EXECUTOR,
    INSERT_EXECUTOR,
    INDEX_SCAN_EXECUTOR,
};

class AbstractExecutor {
  public:
    Rid _abstract_rid;

    Context *context_;

    virtual ~AbstractExecutor() = default;

    [[nodiscard]] virtual size_t tupleLen() const {
        throw InternalError("virtual member function not implemented");
    };

    [[nodiscard]] virtual const std::vector<ColMeta> &cols() const {
        throw InternalError("virtual member function not implemented");
    };

    virtual ExecutorType getType() {
        throw InternalError("virtual member function not implemented");
    };

    virtual void beginTuple() {
        throw InternalError("virtual member function not implemented");
    };

    virtual void nextTuple() {
        throw InternalError("virtual member function not implemented");
    };

    [[nodiscard]] virtual bool is_end() const {
        throw InternalError("virtual member function not implemented");
    };

    [[nodiscard]] virtual std::string tableName() const {
        throw InternalError("virtual member function not implemented");
    };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) {
        throw InternalError("virtual member function not implemented");
    }

    static std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target,
                                                        bool aggr = false) {
        /*
        aggr: 是否需要考虑聚合函数。如果需要考虑聚合函数，则需要同时匹配聚合函数类型
        */
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&target, aggr](const ColMeta &col) {
            if (!aggr) {
                return col.tab_name == target.tab_name && col.name == target.col_name;
            } else {
                return col.tab_name == target.tab_name && col.name == target.col_name && col.aggr == target.aggr;
            }
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }
};