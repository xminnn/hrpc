#pragma once

struct hashmap;

/**
 * @param init_cap 初始容量
 * @param element_size 0表示不存储数据，只对指针进行索引. 否则会在创建的时候申请内存，删除的时候释放内存
 * @param func_hashcode hash函数, 不可为空
 * @param func_equal 比较函数，不可为空。返回0表示不相等，1表示相等
 */
struct hashmap* hashmap_create(int init_cap, int element_size, int (*func_hashcode)(const void*), int (*func_equal)(const void*, const void*));

/**
 * 释放hashmap内存
 */
void hashmap_free(struct hashmap* self);

/**
 * 获得元素计数
 */
int hashmap_count(struct hashmap* self);

/**
 * 调整容量
 */
void hashmap_resize(struct hashmap* self, int capacity);

/**
 * 添加元素，⚠ 必须保证不存在
 */
void* hashmap_add(struct hashmap* self, void* data);
void* hashmap_put(struct hashmap* self, void* data);
void* hashmap_get(struct hashmap* self, const void* key);
void hashmap_del(struct hashmap* self, const void* key);

struct hashmap_itor {
    int i;
    int j;
    int where;
};
struct hashmap_itor hashmap_itor_next(struct hashmap* self, struct hashmap_itor it);
void* hashmap_itor_val(struct hashmap* self, struct hashmap_itor it);
#define _concat_impl(a, b) a##b
#define _concat(a, b) _concat_impl(a, b)
#define hashmap_foreach(type, val, map)                     \
    struct hashmap_itor _concat(__hmap_it, __LINE__) = {0}; \
    for (type val = 0; (_concat(__hmap_it, __LINE__) = hashmap_itor_next(map, _concat(__hmap_it, __LINE__))).where != 0 && (val = hashmap_itor_val(map, _concat(__hmap_it, __LINE__)));)
