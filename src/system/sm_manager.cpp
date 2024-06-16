/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    if(chdir(db_name.c_str()) < 0){
        throw UnixError();
    }
    std::ifstream ifs(DB_META_NAME);
    ifs >> db_;
    for(auto& [table_name,table_meta] : db_.tabs_){
        fhs_[table_name] = rm_manager_->open_file(table_name);
    }
    // open index
    for(auto& [table_name, table_meta] : db_.tabs_){
        for (auto& index_meta : table_meta.indexes){
            ihs_[ix_manager_->get_index_name(table_name, index_meta.cols)] = ix_manager_->open_index(table_name, index_meta.cols);
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .alias = "",
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    auto it1 = fhs_.find(tab_name);
    if(it1 == fhs_.end()){
        throw TableNotFoundError(tab_name);
    }
    rm_manager_->close_file(it1->second.get());
    rm_manager_->destroy_file(tab_name);
//    TODO: 删除索引
    drop_index(tab_name, db_.get_table(tab_name).cols, context);
    fhs_.erase(tab_name);
    db_.tabs_.erase(tab_name);
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    if (ix_manager_->exists(tab_name, col_names)) throw IndexExistsError(tab_name, col_names);
    
    std::vector<ColMeta> cols;
    size_t col_tot_len = 0;
    for (auto &col : col_names) {
        auto col_meta = db_.get_table(tab_name).get_col(col);
        col_tot_len += col_meta->len;
        cols.push_back(*col_meta);
    }

    auto index_meta = IndexMeta {
        .tab_name    = tab_name,
        .col_tot_len = col_tot_len,
        .col_num     = cols.size(),
        .cols        = cols
    };

    // 插入数据到索引文件
    ix_manager_->create_index(tab_name, cols);
    auto ix_handler = ix_manager_->open_index(tab_name, cols);
    auto file_handler = fhs_.at(tab_name).get();
    auto txn = nullptr ? nullptr : context->txn_;
    RmScan rm_scan(file_handler);

    for (; !rm_scan.is_end(); rm_scan.next()) {
        auto record = file_handler->get_record(rm_scan.rid(), context);

        auto projected_record = std::make_unique<RmRecord>(index_meta.col_tot_len);
        size_t offset = 0;
        for (const auto &col : cols) {
            memcpy(projected_record->data + offset, record->data + col.offset, col.len);
            offset += col.len;
        }

        try {
            ix_handler->insert_entry(projected_record->data, rm_scan.rid(), txn);
        } catch (const IndexKeyDuplicateError &e) {
            
        }
    }
    
    // 更新元数据
    ihs_.emplace(ix_manager_->get_index_name(tab_name, col_names), std::move(ix_handler));
    db_.get_table(tab_name).indexes.emplace_back(index_meta);
    flush_meta();    
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    if (!ix_manager_->exists(tab_name, col_names)) throw IndexNotFoundError(tab_name, col_names);
    
    // 删除索引
    auto index_name = ix_manager_->get_index_name(tab_name, col_names);

    bool in_ihs = ihs_.find(index_name) != ihs_.end();

    if (in_ihs){
        ix_manager_->close_index(ihs_.at(index_name).get());
    }

    ix_manager_->destroy_index(tab_name, col_names);

    if (in_ihs) {
        // 删除page
        for (int i = 0; i < ihs_.at(index_name)->get_page_cnt(); ++i) {
            PageId page_id = {ihs_.at(index_name)->get_fd(), i};
            buffer_pool_manager_->unpin_page(page_id, true);
            buffer_pool_manager_->delete_page(page_id);
        }
        // 更新元数据
        ihs_.erase(index_name);
    }
    
    auto &tab_meta = db_.tabs_.at(tab_name);
    tab_meta.indexes.erase(tab_meta.get_index_meta(col_names));
    flush_meta();
}

void SmManager::show_index(const std::string& tab_name, Context* context) {


    TabMeta &tab = db_.get_table(tab_name);
    if (tab.indexes.empty()) return;

    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    RecordPrinter printer(3);
    for (auto &index : tab.indexes) {
        std::vector<std::string> index_info = {tab_name, "unique", ""};

        std::string cols = "(";
        for (int i = 0; i < index.col_num; ++i) {
            cols += index.cols[i].name;
            if (i != index.col_num - 1) cols += ",";
            else cols += ")";
        }

        index_info[2] = cols;
        printer.print_record(index_info, context);
        outfile << "| " << tab_name << " | unique | " << cols << " |\n";
    }

    outfile.close();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    std::vector<std::string> col_names;
    for (auto &col : cols) col_names.emplace_back(col.name);
    drop_index(tab_name, col_names, context);
}