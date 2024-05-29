/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <algorithm>
#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）

    // 一个page的状态有全满，半满，全空，链表记录的是非全满的page
    // 需要寻找非全空的记录中page_no最小的一个
    // 1. ■ ■ ◧ □           page_no = 1
    //        ^ first_free_page_no
    // 2. ◧ ■ □ ■
    //    ^ first_free_page_no
    // 3. □ ■ ◧ ■
    //    ^ first_free_page_no
    auto hdr = file_handle_->file_hdr_;
    page_id_t page_no = -1;
    int slot_no = -1;

    // 链表对寻找非全空无帮助，遍历page
    int num_slot = hdr.num_records_per_page;
    for (page_no = 1; page_no < hdr.num_pages; ++page_no) {
        auto page_handle = file_handle->fetch_page_handle(page_no);
        int first_one =  Bitmap::first_bit(true, page_handle.bitmap, num_slot);
        file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        if(first_one < num_slot){   // 此页非全空
            slot_no = first_one;
            break;
        }
    }
    if(slot_no == -1){  // 所有页面全空，到达终点
        page_no = -1;
    }
    rid_ = Rid{page_no, slot_no};
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

    // 1. page内部查bitmap找到下一个记录
    // 2. 整个page内后面为空(或已经在page末尾），前往下一个非全空页
    auto hdr = file_handle_->file_hdr_;
    int num_slot = hdr.num_records_per_page;
    int curr = rid_.slot_no;
    assert(!is_end());      // 迭代器失效后不能再迭代

    for (int page_no = rid_.page_no; page_no < hdr.num_pages; ++page_no) {
        // 找到此page内第一个记录
        auto page_handle = file_handle_->fetch_page_handle(page_no);
        int first_one = Bitmap::next_bit(true, page_handle.bitmap, num_slot, curr);
        curr = -1;   // 先搜索当前页后面，再搜索后面的页的全部
        file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        if (first_one < num_slot){
            rid_ = {page_no, first_one};
            return;
        }
    }
    rid_ = {-1, -1};
    // 到达终点
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return rid_.page_no == -1 && rid_.slot_no == -1;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}