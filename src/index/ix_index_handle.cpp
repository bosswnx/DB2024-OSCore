/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"
#include "ix_scan.h"
#include <mutex>
#include <cstring>

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int size = page_hdr->num_key;
    for (int i=0; i<size; ++i) {
        if (ix_compare(get_key(i), target, file_hdr->col_types_, file_hdr->col_lens_) >= 0) {
            return i;
        }
    }
    return -1;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式：顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int size = page_hdr->num_key;
    for (int i=0; i<size; ++i) {
        if (ix_compare(get_key(i), target, file_hdr->col_types_, file_hdr->col_lens_) > 0) {
            return i;
        }
    }
    return -1;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    // Todo:
    // 1. 在叶子节点中获取目标key所在位置
    // 2. 判断目标key是否存在
    // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
    // 提示：可以调用lower_bound()和get_rid()函数。
    
    // if (get_key_pos() == -1) return 

    auto pos = lower_bound(key);
    if (pos == -1) return false;
    
    // 可能有大于的情况，这里要做double-check
    if (ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) != 0)
        return false;
    
    *value = get_rid(pos);
    return true;
}

int IxNodeHandle::get_key_pos(const char *key) {
    auto pos = upper_bound(key) - 1;
    if (pos < 0) pos = 0;
    return pos;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::internal_lookup(const char *key) {
    // Todo:
    // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    // 2. 获取该孩子节点（子树）所在页面的编号
    // 3. 返回页面编号
    int pos = get_key_pos(key);
    return get_rid(pos)->page_no;
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    // Todo:
    // 1. 判断pos的合法性
    // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // 4. 更新当前节点的键数量

    // if (pos < 0 || pos > get_size()) return
    assert(pos >= 0 && pos <= get_size()); // pos = get_size() 的情况是插入末尾。
    auto key_insert_start = get_key(pos);
    auto rid_insert_start = get_rid(pos);
    std::move(key_insert_start, get_key(get_size()), get_key(pos + n));
    std::memcpy(key_insert_start, key, n*file_hdr->col_tot_len_);
    std::move(rid_insert_start, get_rid(get_size()), get_rid(pos + n));
    std::memcpy(rid_insert_start, rid, n*sizeof(Rid));
    page_hdr->num_key += n;
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxNodeHandle::insert(const char *key, const Rid &value) {
    // Todo:
    // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
    // 2. 如果key重复则不插入
    // 3. 如果key不重复则插入键值对
    // 4. 返回完成插入操作之后的键值对数量

    auto pos = lower_bound(key);
    if (pos == -1) pos = get_size(); // 加在末尾

    if(pos != get_size() && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        // duplicate
        throw IndexKeyDuplicateError();
    } else {
        insert_pair(pos, key, value);
    }

    return get_size();
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    // Todo:
    // 1. 删除该位置的key
    // 2. 删除该位置的rid
    // 3. 更新结点的键值对数量
    assert(pos >= 0 && pos < get_size());

    if (pos == get_size() - 1) {
        // 尾部直接清空
        memset(get_key(pos), 0, file_hdr->col_tot_len_);
        memset(get_rid(pos), 0, sizeof(Rid));
    } else {
        std::move(get_key(pos+1), get_key(get_size()), get_key(pos));
        std::move(get_rid(pos+1), get_rid(get_size()), get_rid(pos));
    }
    --page_hdr->num_key;

}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
int IxNodeHandle::remove(const char *key) {
    // Todo:
    // 1. 查找要删除键值对的位置
    // 2. 如果要删除的键值对存在，删除键值对
    // 3. 返回完成删除操作后的键值对数量

    auto pos = lower_bound(key);
    if (pos == -1) return get_size();

    if(ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        erase_pair(pos);
    }

    return get_size();
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    char* buf = new char[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);
    
    // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                            Transaction *transaction, bool find_first) {
    // Todo:
    // 1. 获取根节点
    // 2. 从根节点开始不断向下查找目标key
    // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点

    page_id_t page_id;
    auto cur = fetch_node(file_hdr_->root_page_);
    while (!cur->is_leaf_page()) {
        page_id = cur->internal_lookup(key);
        buffer_pool_manager_->unpin_page(cur->get_page_id(), false);
        cur = fetch_node(page_id);
    }

    return std::make_pair(cur, false);
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    // Todo:
    // 1. 获取目标key值所在的叶子结点
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    // 3. 把rid存入result参数中
    // 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁

    std::scoped_lock lock{root_latch_};

    // 1. 获取目标key值所在的叶子结点
    auto leaf_node = find_leaf_page(key, Operation::FIND, transaction).first;
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    Rid *rid = nullptr;
    bool ok = leaf_node->leaf_lookup(key, &rid);
    if (ok) result->emplace_back(*rid);
    
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);

    return ok;
}

/**
 * @brief  将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    // Todo:
    // 1. 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
    //    需要初始化新节点的page_hdr内容
    // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
    //    为新节点分配键值对，更新旧节点的键值对数记录
    // 3. 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())

    auto new_node = create_node();
    int pos = node->get_size() >> 1;
    new_node->page_hdr->next_free_page_no = node->page_hdr->next_free_page_no;
    new_node->page_hdr->is_leaf = node->page_hdr->is_leaf;
    new_node->page_hdr->parent = node->page_hdr->parent;

    new_node->insert_pairs(0, node->get_key(pos), node->get_rid(pos), node->get_size() - pos);
    node->set_size(pos);

    if (new_node->is_leaf_page()) {
        // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
        new_node->page_hdr->prev_leaf = node->get_page_no();
        new_node->page_hdr->next_leaf = node->page_hdr->next_leaf;

        auto new_node_next = fetch_node(node->page_hdr->next_leaf); // TODO: 这里可能会报错的。 
        new_node_next->page_hdr->prev_leaf = new_node->get_page_no();

        node->page_hdr->next_leaf = new_node->get_page_no();

        buffer_pool_manager_->unpin_page(new_node_next->get_page_id(), true);

    } else {
        for (int i=0; i<new_node->get_size(); ++i) 
            maintain_child(new_node, i);
    }
    
    return new_node;
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                     Transaction *transaction) {
    // Todo:
    // 1. 分裂前的结点（原结点, old_node）是否为根结点，如果为根结点需要分配新的root
    // 2. 获取原结点（old_node）的父亲结点
    // 3. 获取key对应的rid，并将(key, rid)插入到父亲结点
    // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
    // 提示：记得unpin page
    
    if (old_node->is_root_page()) {
        IxNodeHandle *root;
        root = create_node();
        root->page_hdr->is_leaf = false;
        
        update_root_page_no(root->get_page_no());

        Rid ot, nt;
        ot.page_no = old_node->get_page_no();
        ot.slot_no = -1;
        nt.page_no = new_node->get_page_no();
        nt.slot_no = -1;

        root->set_size(0);
        root->set_parent_page_no(INVALID_PAGE_ID);
        root->set_next_leaf(INVALID_PAGE_ID);
        root->set_prev_leaf(INVALID_PAGE_ID);

        root->insert_pair(0, old_node->get_key(0), ot);
        root->insert_pair(1, key, nt);

        old_node->page_hdr->parent = root->get_page_no();
        new_node->page_hdr->parent = root->get_page_no();
    } else {
        // 递归更新
        IxNodeHandle *parent;
        parent = fetch_node(old_node->get_parent_page_no()); // 2. 获取原结点（old_node）的父亲结点
        int pos = parent->find_child(old_node); // 找到 old node 在 parent 中的位置，然后将 new node 插在这个位置之后。

        // 不是叶子节点。
        Rid t;
        t.page_no = new_node->get_page_no();
        t.slot_no = -1;

        parent->insert_pair(pos + 1, key, t);
        // 如果超出了，递归更新
        if (parent->get_size() >= parent->get_max_size()) { // 达到这个就换，而不是大于
            auto new_node = split(parent); // we split the parent node.
            insert_into_parent(parent, new_node->get_key(0), new_node, transaction);
        }
    }
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 */
page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    // Todo:
    // 1. 查找key值应该插入到哪个叶子节点
    // 2. 在该叶子节点中插入键值对
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    // 提示：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁

    std::scoped_lock lock{root_latch_};
    // 1. 查找key值应该插入到哪个叶子节点
    auto leaf_node = find_leaf_page(key, Operation::INSERT, transaction).first;
    // 2. 在该叶子节点中插入键值对
    int kv_num_before = leaf_node->get_size();
    int kv_num = leaf_node->insert(key, value);

    if (kv_num_before != kv_num && kv_num == leaf_node->get_max_size()) {
        // full, we split it
        auto new_leaf_node = split(leaf_node);
        // 并把新结点的相关信息插入父节点
        insert_into_parent(leaf_node, new_leaf_node->get_key(0), new_leaf_node, transaction); // 新的节点现在只有一个kv。
        buffer_pool_manager_->unpin_page(new_leaf_node->get_page_id(), true);

        if (file_hdr_->last_leaf_ == leaf_node->get_page_no()) {
            // 更新 file_hdr
            file_hdr_->last_leaf_ == new_leaf_node->get_page_no();
        }
    }
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), kv_num_before != kv_num);
    return leaf_node->get_page_no();
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 */
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    // Todo:
    // 1. 获取该键值对所在的叶子结点
    // 2. 在该叶子结点中删除键值对
    // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁
    std::scoped_lock lock{root_latch_};

    auto tar = find_leaf_page(key, Operation::DELETE, transaction).first;
    int num = tar->get_size();
    bool ok = num != tar->remove(key); 
    if (ok) {
        bool need_delete = coalesce_or_redistribute(tar, transaction); // 是否需要删除结点
        if (need_delete) {
            
        }
    }

    buffer_pool_manager_->unpin_page(tar->get_page_id(), ok);

    return ok;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction, bool *root_is_latched) {
    // Todo:
    // 1. 判断node结点是否为根节点
    //    1.1 如果是根节点，需要调用AdjustRoot() 函数来进行处理，返回根节点是否需要被删除
    //    1.2 如果不是根节点，并且不需要执行合并或重分配操作，则直接返回false，否则执行2
    // 2. 获取node结点的父亲结点
    // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
    // 4. 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点（即node.size+neighbor.size >=
    // NodeMinSize*2)，则只需要重新分配键值对（调用Redistribute函数）
    // 5. 如果不满足上述条件，则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）

    if (node->is_root_page()) {
        return adjust_root(node);
    } else if (node->get_size() >= node->get_min_size()) {
        // 不需要执行合并或重分配操作，删除之后，需要更新父节点的信息
        maintain_parent(node);
        return false;
    }

    // 这里是合并或者redistribute
    auto node_parent = fetch_node(node->page_hdr->parent); // 找到父节点
    int node_pos = node_parent->find_child(node); // 找到 node 在 parent 中的位置
    int siblings_pos = node_pos - 1 == -1 ? node_pos + 1 : node_pos - 1; // 优先选取前驱结点进行合并
    if (siblings_pos > node_parent->get_size()) {
        throw RMDBError("coalesce_or_redistribute: No siblings found!");
    }
    auto sibling = fetch_node(node_parent->get_rid(siblings_pos)->page_no); // 获取兄弟结点
    
    bool need_delete = false;
    if (node->get_size() + sibling->get_size() >= node->get_min_size() * 2) {
        // 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点，则只需要重新分配键值对。（够用）
        redistribute(sibling, node, node_parent, siblings_pos);
    } else {
        need_delete = coalesce(&sibling, &node, &node_parent, siblings_pos, transaction, root_is_latched);
    }
    buffer_pool_manager_->unpin_page(sibling->get_page_id(), true);
    buffer_pool_manager_->unpin_page(node_parent->get_page_id(), true);

    return need_delete;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesce_or_redistribute()
 */
bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) {
    // Todo:
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    // 3. 除了上述两种情况，不需要进行操作

    if (old_root_node->is_leaf_page()) {
        // 这里的情况是：根节点是叶子结点（整个b+树只有一个节点）
        if (old_root_node->get_size() == 0) {
            release_node_handle(*old_root_node);
            update_root_page_no(2);
            return true;
        }
    } else {
        if (old_root_node->get_size() == 1) {
            auto new_root = fetch_node(file_hdr_->root_page_);
            new_root->page_hdr->parent = 0;
            update_root_page_no(new_root->get_page_no());
            buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
            return true;
        }
    }
    
    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    // Todo:
    // 1. 通过index判断neighbor_node是否为node的前驱结点
    // 2. 从neighbor_node中移动一个键值对到node结点中
    // 3. 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
    // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论

    if (index == 0) {
        // neighbor是node后继结点
        // 把 neighbor_node 的第一个键值对借过来
        node->insert_pair(node->get_size(), neighbor_node->get_key(0), *neighbor_node->get_rid(0));
        neighbor_node->erase_pair(0);
        maintain_child(node, node->get_size()-1); // 保证后面的孩子结点的父节点信息正确。
        maintain_parent(neighbor_node); // 更新父节点的信息。如果neighbor_node是node的后继节点，那么就需要保证删除neighbor_node的第一个key之后，neighbor_node后来的第一个key被更新到parent中。
    } else {
        // neighbor是node前驱结点
        node->insert_pair(0, neighbor_node->get_key(neighbor_node->get_size()-1), *neighbor_node->get_rid(neighbor_node->get_size()-1));
        neighbor_node->erase_pair(neighbor_node->get_size()-1);
        maintain_child(node, 0); // 保证后面的孩子结点的父节点信息正确。
        maintain_parent(node); // 更新父节点的信息。
    }
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction, bool *root_is_latched) {
    // Todo:
    // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
    // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
    // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
    // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf

    if (index == 0) {
        std::swap(*neighbor_node, *node);
    }

    // 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息
    (*neighbor_node)->insert_pairs((*neighbor_node)->get_size(), (*node)->get_key(0), (*node)->get_rid(0), (*node)->get_size());
    for (int i=0; i<(*node)->get_size(); ++i) {
        maintain_child(*neighbor_node, i + (*neighbor_node)->get_size());
    }

    release_node_handle(**node); // 释放node结点
    (*parent)->erase_pair(index);

    if ((*node)->is_leaf_page() && (*node)->get_page_no() == file_hdr_->last_leaf_) {
        file_hdr_->last_leaf_ = (*neighbor_node)->get_page_no();
    }

    return coalesce_or_redistribute(*parent, transaction); // 检测上层是否需要继续合并（因为parent可能也下溢了）
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size()) {
        throw IndexEntryNotFoundError();
    }
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    return *node->get_rid(iid.slot_no);
}

/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char *key) {
    auto leaf = find_leaf_page(key, Operation::FIND, nullptr).first; // 找到叶子结点
    int pos = leaf->lower_bound(key); // 找到key在叶子结点中的位置
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);  // unpin it!
    if (pos == -1 && leaf->get_page_no() != leaf->file_hdr->last_leaf_) {
        // 位于叶子节点的末尾。
        return Iid{.page_no = leaf->get_next_leaf(), .slot_no = 0};
    }
    if (pos == -1) pos = leaf->get_size();
    return Iid{.page_no = leaf->get_page_no(), .slot_no = pos};
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char *key) {
    auto leaf = find_leaf_page(key, Operation::FIND, nullptr).first; // 找到叶子结点
    int pos = leaf->upper_bound(key); // 找到key在叶子结点中的位置
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);  // unpin it!
    if (pos == -1 && leaf->get_page_no() != leaf->file_hdr->last_leaf_) {
        // 位于叶子节点的末尾。
        return Iid{.page_no = leaf->get_next_leaf(), .slot_no = 0};
    }
    if (pos == -1) pos = leaf->get_size();
    return Iid{.page_no = leaf->get_page_no(), .slot_no = pos};
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const {
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);
    
    return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::create_node() {
    IxNodeHandle *node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    node = new IxNodeHandle(file_hdr_, page);
    return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    IxNodeHandle *curr = node;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        // Load its parent
        IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
        int rank = parent->find_child(curr); // 找到当前结点在父节点中的位置
        char *parent_key = parent->get_key(rank); // 获取当前节点在父节点中的key
        char *child_first_key = curr->get_key(0); // 获取当前节点的第一个key
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) { // 如果相等，不需要更新
            assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);  // 修改了parent node
        curr = parent;

        assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->is_leaf_page());

    IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);

    IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf());  // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node) {
    file_hdr_->num_pages_--;
}

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->is_leaf_page()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
    }
}