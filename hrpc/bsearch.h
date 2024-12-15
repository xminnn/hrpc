#pragma once

#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

typedef int (*cmpfunc)(const void *, const void *);

int _binary_search(const void *array, int count, int element_size, cmpfunc cmp, const void *data);
void *_bsearch_get(const void *arr, int element_size, int count, cmpfunc cmp, const void *index);
int _bsearch_get_pos(const void *arr, int element_size, int count, cmpfunc cmp, const void *index);    // 返回值小于0表示没有找到， 特殊用法：pos = -pos - 1 表示可以插入的位置，常用于获取遍历的起始位置
int _bsearch_put(void *arr, int element_size, int *count, int limit, cmpfunc cmp, void *data);
int _bsearch_del(void *arr, int element_size, int *count, cmpfunc cmp, const void *index);
int _bsearch_del_pos(void *array, int element_size, int *count, int pos);

int cmp_int64(const void *a, const void *b);
int cmp_2int64(const void *a, const void *b);
int cmp_int(const void *a, const void *b);
int cmp_2int(const void *a, const void *b);

// 延迟删除时统一删除
#define remove_deleted(arr)                               \
    do {                                                  \
        int real_count = 0;                               \
        for (int i = 0; i < _concat(arr, _count_); i++) { \
            if (arr[i].deleted) {                         \
                continue;                                 \
            }                                             \
            if (i != real_count) {                        \
                arr[real_count++] = arr[i];               \
            } else {                                      \
                real_count++;                             \
            }                                             \
        }                                                 \
        _concat(arr, _count_) = real_count;               \
    } while (0);

#define foreach(name, arr)         \
    int _concat(_i, __LINE__) = 0; \
    for (typeof(arr[0]) *name; _concat(_i, __LINE__) < _concat(arr, _count_) && (name = &arr[_concat(_i, __LINE__)]); ++_concat(_i, __LINE__))

#define bsearch_put(arr, cmp, data) _bsearch_put(arr, sizeof(arr[0]), &_concat(arr, _count_), sizeof(arr) / sizeof(arr[0]), cmp, data)
#define bsearch_get(arr, cmp, index) (typeof(arr[0]) *)_bsearch_get(arr, sizeof(arr[0]), _concat(arr, _count_), cmp, index)
#define bsearch_get_pos(arr, cmp, index) _bsearch_get_pos(arr, sizeof(arr[0]), _concat(arr, _count_), cmp, index)
#define bsearch_del(arr, cmp, index) _bsearch_del(arr, sizeof(arr[0]), &_concat(arr, _count_), cmp, index)
#define bsearch_del_pos(arr, pos) _bsearch_del_pos(arr, sizeof(arr[0]), &_concat(arr, _count_), pos)