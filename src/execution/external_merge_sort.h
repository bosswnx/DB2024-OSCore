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

#include <sys/mman.h>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

#include <cstdlib>
#include <algorithm>
#include <cmath>


class ExternalMergeSorter {
private:
    const ssize_t TOTAL_MEM;                            // 指示排序算法最多能使用的内存(近似)，此参数影响文件大小和缓冲区大小
    const ssize_t RECORD_SIZE;
    std::vector<std::string> filenames_;                // 大表分隔后存储在多个文件中
    std::vector<std::ifstream> opened_files;            // 保存打开的文件
    std::vector<std::unique_ptr<char[]>> buffer_list;   // 分配个每个ifstream对象的缓冲区
    std::vector<std::unique_ptr<char[]>> record_list;   // 每个文件“第一个”记录
    std::vector<ssize_t> heap;                          // 堆模拟败者树

    int (*cmp_)(const void *, const void *, void *);    // 比较函数

    void *arg_;                                         // 比较函数传入的额外参数，用于比较记录的任意属性，传递给`qsort_r`
    int total_record = 0;                               // 记录总数，用于判断是否读完

// 以下只在write函数中使用
    ssize_t index = 0;                                  // 当前插入记录在文件中的偏移
    char *data = nullptr;                               // 写入时文件映射在此处
    bool isFull = true;                                 // 当前使用的文件是否已满
public:
    ExternalMergeSorter(ssize_t total_mem, ssize_t record_size,
                        int (*cmp)(const void *, const void *, void *), void *arg = nullptr)
    // 保证每个文件大小是记录大小的整数倍
            : TOTAL_MEM(total_mem - total_mem % record_size), RECORD_SIZE(record_size), cmp_(cmp), arg_(arg) {}

    ExternalMergeSorter(ExternalMergeSorter &&sorter) noexcept: TOTAL_MEM(sorter.TOTAL_MEM),
                                                                RECORD_SIZE(sorter.RECORD_SIZE) {
        filenames_ = std::move(sorter.filenames_);
        opened_files = std::move(sorter.opened_files);
        buffer_list = std::move(sorter.buffer_list);
        record_list = std::move(sorter.record_list);
        heap = std::move(sorter.heap);
        cmp_ = sorter.cmp_;
        arg_ = sorter.arg_;
        total_record = sorter.total_record;
        index = sorter.index;
        data = sorter.data;
        isFull = sorter.isFull;
    }

    /// 上层函数可能不读完所有记录
    ~ExternalMergeSorter() {
        for (size_t i = 0; i < opened_files.size(); ++i) {
            if (opened_files[i].is_open()) {
                opened_files[i].close();
                unlink(filenames_[i].c_str());
            }
        }
    }


    void write(const char *record) {
        if (isFull) {
            if (data != nullptr) {
                ::qsort_r(data, TOTAL_MEM / RECORD_SIZE, RECORD_SIZE, cmp_, arg_);
                munmap(data, TOTAL_MEM);
            }
            char filename[] = "auxiliary_sort_fileXXXXXX";
            int fd = mkstemp(filename);
            filenames_.emplace_back(filename);
            if (fd == -1) {
                throw UnixError();
            }
            ftruncate(fd, TOTAL_MEM);
            data = (char *) mmap(nullptr, TOTAL_MEM, PROT_READ | PROT_WRITE,
                                 MAP_SHARED, fd, 0);
            if (data == (void *) -1) {
                throw UnixError();
            }
            isFull = false;
            index = 0;
            close(fd);
        }
        memcpy(data + index * RECORD_SIZE, record, RECORD_SIZE);
        index++;
        total_record++;
        if (index == TOTAL_MEM / RECORD_SIZE) {
            isFull = true;
        }
    }

    void endWrite() {
        if (data != nullptr) {
            ::qsort_r(data, index, RECORD_SIZE, cmp_, arg_);
            munmap(data, TOTAL_MEM);
            int fd = open(filenames_.back().c_str(), O_RDWR);
            if (fd == -1) {
                throw UnixError();
            }
            ftruncate(fd, index * RECORD_SIZE);
            close(fd);
        }
        if (filenames_.empty()) {
            return;
        }
    }

    void beginRead() {
        ssize_t _buffer_size = TOTAL_MEM / filenames_.size();
        const ssize_t buffer_size = _buffer_size - _buffer_size % RECORD_SIZE;   // 尽量使用更多的内存
        for (const auto &filename: filenames_) {
            std::ifstream file(filename, std::ios::binary);
            if (file.fail()) {
                throw UnixError();
            }
            auto buffer = new char[buffer_size];
            file.rdbuf()->pubsetbuf(buffer, buffer_size);
            buffer_list.emplace_back(buffer);
            char *record = new char[RECORD_SIZE];
            file.read(record, RECORD_SIZE);
            record_list.emplace_back(record);     // 读取每个文件的第一个记录
            opened_files.push_back(std::move(file));
        }
        int height = std::ceil(std::log2(filenames_.size()));
        heap.resize(1 << (height + 1));   // 败者树，数大为败者
        for (ssize_t i = 0; i < filenames_.size(); i++) {
            heap[(1 << height) + i] = i;
        }
        for (size_t i = filenames_.size(); i < (1 << height); i++) {
            heap[(1 << height) + i] = -1;       // 哑节点
        }
        auto winners = std::vector<ssize_t>(1 << (height + 1));  // 在自底向上构建败者树时，记录每一轮的胜者
        for (size_t i = 0; i < filenames_.size(); i++) {
            winners[i + (1 << height)] = i;
        }
        for (size_t i = filenames_.size(); i < (1 << height); i++) {
            winners[i + (1 << height)] = -1;
        }
        for (ssize_t i = (1 << height) - 1; i >= 1; i--) {
            ssize_t left = i << 1;
            ssize_t right = i << 1 ^ 1;
            if (winners[left] != -1 &&
                (winners[right] == -1 ||
                 cmp_(record_list[winners[left]].get(), record_list[winners[right]].get(), arg_) <= 0)) {
                // 左节点获胜
                winners[i] = winners[left];
                heap[i] = winners[right];
            } else {
                // 右节点获胜
                winners[i] = winners[right];
                heap[i] = winners[left];
            }
        }
        heap[0] = winners[1];
    }

    /// 取出最小的记录后，调整败者树
    void adjust() {
        ssize_t file_index = heap[0];      // 此记录已经使用完了
        opened_files[file_index].read(record_list[file_index].get(), RECORD_SIZE);
        size_t height = std::ceil(std::log2(filenames_.size()));
        ssize_t cur = file_index + (1 << height);
        ssize_t winner = file_index;
        if (opened_files[file_index].eof()) {
            heap[cur] = -1;     // 文件读完后，变为哑节点
            winner = -1;
            opened_files[file_index].close();
            unlink(filenames_[file_index].c_str());
        }
        while (cur != 1) {   // 使用新值重新参与比赛
            ssize_t parent = cur >> 1;      // heap[parent]是上次比赛中的败者
            // 父节点保存了左右子树两胜者中的次胜者(败者)
            if (winner != -1 &&
                (heap[parent] == -1 || cmp_(record_list[winner].get(), record_list[heap[parent]].get(), arg_) <= 0)) {
                // winner参赛并取胜，败者不变，winner继续参与下一轮比赛
                cur = parent;
            } else {
                // winner败北，父节点保存的上一轮败者成为这一轮的胜者，参与下次比赛
                std::swap(heap[parent], winner);
                cur = parent;           // 迭代，继续向上调整
            }
        }
        heap[0] = winner;
    }

    void read(char *record) {
        memcpy(record, record_list[heap[0]].get(), RECORD_SIZE);
        adjust();
        total_record--;
    }

    [[nodiscard]] bool is_end() const {
        return total_record == 0;
    }
};