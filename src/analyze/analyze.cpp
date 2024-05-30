/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query 
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse))
    {
        // 处理表名
        query->tables = std::move(x->tabs);
        /* 检查表是否存在 */
        for(auto& table_name : query->tables){
            if (!sm_manager_->db_.is_table(table_name)){
                throw TableNotFoundError(table_name);
            }
        }

        // 处理target list，再target list中添加上表名，例如 a.id
        for (auto &sv_sel_col : x->cols) {
            /** TODO: 检查所 select 的 cols 的 alias 是否冲突？ */
            TabCol sel_col = {
                .tab_name = sv_sel_col->tab_name,
                .col_name = sv_sel_col->col_name,
                .alias = sv_sel_col->alias,
                .aggr = sv_sel_col->aggr_type
            };
            query->cols.push_back(sel_col);
        }

        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        if (query->cols.empty()) {
            // select all columns
            for (auto &col : all_cols) {
                TabCol sel_col = {
                    .tab_name = col.tab_name,
                    .col_name = col.name,
                    .alias = col.alias,
                    .aggr = ast::NO_AGGR
                };
                query->cols.push_back(sel_col);
            }
        } else {
            // infer table name from column name
            for (auto &sel_col : query->cols) {
                sel_col = check_column(all_cols, sel_col);  // 列元数据校验
            }
        }
        //处理where条件
        get_clause(x->conds, query->conds);
        check_where_clause(query->tables, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        // update语句只有一个表
        query->tables.push_back(x->tab_name);
        // 检查表是否存在
        if (!sm_manager_->db_.is_table(x->tab_name)){
            throw TableNotFoundError(x->tab_name);
        }
        get_clause(x->conds, query->conds);
        // 检查where子句的语义
        check_where_clause(query->tables, query->conds);
        // 从语法树中提取set子句
        for(const auto& clause : x->set_clauses){
            query->set_clauses.push_back({
                // 补全表名
                .lhs={.tab_name = query->tables.at(0),.col_name= clause->col_name},
                .rhs=convert_sv_value(clause->val)
            });
        }
        check_set_clause(query->tables.at(0), query->set_clauses);
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        // delete语句只有一个表
        query->tables.push_back(x->tab_name);
        // 检查表是否存在
        if (!sm_manager_->db_.is_table(x->tab_name)){
            throw TableNotFoundError(x->tab_name);
        }
        //处理where条件
        get_clause(x->conds, query->conds);
        check_where_clause({x->tab_name}, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        // 处理insert 的values值
        for (auto &sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
    } else {
        // do nothing
    }
    query->parse = std::move(parse);
    return query;
}

/// 检查`set score = 90`语句中，`score`列名是否存在，`score`和`90`类型是否相容,并强制转换数据类型
void Analyze::check_set_clause(const std::string &tab_name, std::vector<SetClause>& clauses){
    TabMeta table = sm_manager_->db_.get_table(tab_name);
    for(auto& clause: clauses){
        table.is_col(clause.lhs.col_name);  // 检查列名是否存在
        ColType lhs_type = table.get_col(clause.lhs.col_name)->type;
        ColType rhs_type = clause.rhs.type;
        if(!colTypeCanHold(lhs_type, rhs_type)){    // 检查set语句两边的类型是否相容
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
        if(lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT){
            clause.rhs.float2int();
        }else if(lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT){
            clause.rhs.int2float();
        }
    }
}

/// 当表名省略时，自动推导出表名(无法推导则抛出歧义异常），然后为`target`补全表名
TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        /** Make sure target column exists */
        TabMeta table = sm_manager_->db_.get_table(target.tab_name);
        if(!table.is_col(target.col_name)){
            throw ColumnNotFoundError(target.col_name);
        }
    }
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

/// 从语法树中提取出where语句
void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;
        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
        }
        conds.push_back(cond);
    }
}

/// where子句语义检查，包括检查是否存在模糊的字段名，操作符两侧的列名是否存在，两侧类型是否支持操作符
void Analyze::check_where_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {     // 如果右手边也是列，也需要检查列的合法性
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        if(!colTypeCanHold(lhs_type, rhs_type)){
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}


Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_int(int_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->val);
    } else {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    static const std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}
