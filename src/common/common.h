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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "defs.h"
#include "record/rm_defs.h"


struct TabCol {
    std::string tab_name;
    std::string col_name;

    friend bool operator<(const TabCol &x, const TabCol &y) {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }
};

struct Value {
    ColType type;  // type of value
    union {
        int int_val;      // int value
        float float_val;  // float value
    };
    std::string str_val;  // string value

    std::shared_ptr<RmRecord> raw;  // raw record buffer

    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void init_raw(int len) {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT) {
            assert(len == sizeof(int));
            *(int *) (raw->data) = int_val;
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(float));
            *(float *) (raw->data) = float_val;
        } else if (type == TYPE_STRING) {
            if (len < (int) str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);  // 空余bit填充零，如果没有空余bit就没有tailing-zero
            memcpy(raw->data, str_val.c_str(), str_val.size());
        }
    }

    bool operator==(const Value &rhs) const {
        // 字符串不能和数字类型比较
        if ((this->type == TYPE_STRING && rhs.type != TYPE_STRING) ||
            (rhs.type == TYPE_STRING && this->type != TYPE_STRING)) {
            throw InternalError("cannot compare numeric type with string type");
        }
        // int和float可以比较，int强制转为float参与比较。如果float强转为int，损失精度后果更严重
        // eg: 1.2 -- 强制转为int --> 1     ===>   1 == 1
        if(this->type == TYPE_INT && rhs.type == TYPE_FLOAT){
            return (float )this->int_val == rhs.float_val;
        }else if(this->type == TYPE_FLOAT && rhs.type == TYPE_INT){
            return this->float_val == (float )rhs.int_val;
        }
        // 同类型相互比较
        switch (this->type) {
            case TYPE_INT:
                return this->int_val == rhs.int_val;
            case TYPE_FLOAT:
                return this->float_val == rhs.float_val;
            case TYPE_STRING:
                return this->str_val == rhs.str_val;
            default:
                throw InternalError("not implemented");
        }
    }

    bool operator!=(const Value &rhs) const {
        return !this->operator==(rhs);
    }

    bool operator>(const Value &rhs) const {
        // 字符串不能和数字类型比较
        if ((this->type == TYPE_STRING && rhs.type != TYPE_STRING) ||
            (rhs.type == TYPE_STRING && this->type != TYPE_STRING)) {
            throw InternalError("cannot compare numeric type with string type");
        }
        // int和float可以比较
        if(this->type == TYPE_INT && rhs.type == TYPE_FLOAT){
            return (float )this->int_val > rhs.float_val;
        }else if(this->type == TYPE_FLOAT && rhs.type == TYPE_INT){
            return this->float_val > (float )rhs.int_val;
        }
        // 同类型相互比较
        switch (this->type) {
            case TYPE_INT:
                return this->int_val > rhs.int_val;
            case TYPE_FLOAT:
                return this->float_val > rhs.float_val;
            case TYPE_STRING:
                return this->str_val > rhs.str_val;     // 比较字符串字典序
            default:
                throw InternalError("not implemented");
        }
    }

    bool operator<(const Value &rhs) const {
        return !(this->operator==(rhs) && this->operator>(rhs));
    }

    bool operator>=(const Value &rhs) const {
        return operator==(rhs) && this->operator>(rhs);
    }
    bool operator<=(const Value &rhs) const {
        return !this->operator>(rhs);
    }
};

//             =       !=    <       >      <=     >=
enum CompOp {
    OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE
};

struct Condition {
    TabCol lhs_col;   // left-hand side column
    CompOp op;        // comparison operator
    bool is_rhs_val;  // true if right-hand side is a value (not a column)
    TabCol rhs_col;   // right-hand side column
    Value rhs_val;    // right-hand side value

    [[nodiscard]] bool eval(const Value &lhs) const {
        assert(is_rhs_val);
        switch (op) {
            case OP_EQ:
                return lhs == rhs_val;
            case OP_NE:
                return lhs != rhs_val;
            case OP_LT:
                return lhs < rhs_val;
            case OP_GT:
                return lhs > rhs_val;
            case OP_LE:
                return lhs <= rhs_val;
            case OP_GE:
                return lhs >= rhs_val;
            default:
                throw InternalError("not implemented");
        }
    }
};

struct SetClause {
    TabCol lhs;
    Value rhs;
};