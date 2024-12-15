#include "bsearch.h"

#include <memory.h>

int _binary_search(const void *array, int count, int element_size, cmpfunc cmp, const void *data) {
    char *arr = (char *)array;
    if (count == 0) {
        return -1;
    }
    int start = 0;
    int end = count;

    int tar = 0;
    while (start < end) {
        tar = (end - start) / 2 + start;
        int rst = cmp(arr + (element_size * tar), data);
        if (rst == 0) {
            return tar;
        }
        if (rst > 0) {
            end = tar;
            tar += 1;    // 记录插入位置
        } else {
            start = tar + 1;
            tar += 2;    // 记录插入位置
        }
    }
    return -tar;
}

int _bsearch_put(void *array, int element_size, int *count, int limit, cmpfunc cmp, void *data) {
    char *arr = (char *)array;
    int pos = _binary_search(arr, *count, element_size, cmp, data);
    if (pos >= 0) {
        void *tar = arr + (element_size * pos);
        memmove(tar, data, element_size);
        return pos;
    } else {
        if ((*count) + 1 >= limit) {
            return -1;
        }
        pos = -pos - 1;
        memmove(arr + ((pos + 1) * element_size), arr + (pos * element_size), element_size * ((*count) - pos));
        memmove(arr + (pos * element_size), data, element_size);
        (*count)++;
        return pos;
    }
}

void *_bsearch_get(const void *array, int element_size, int count, cmpfunc cmp, const void *index) {
    if (count <= 0) {
        return 0;
    }
    char *arr = (char *)array;
    int pos = _binary_search(arr, count, element_size, cmp, index);
    if (pos < 0) {
        return 0;
    }
    return arr + (pos * element_size);
}

int _bsearch_get_pos(const void *array, int element_size, int count, cmpfunc cmp, const void *index) {
    if (count <= 0) {
        return -1;
    }
    char *arr = (char *)array;
    int pos = _binary_search(arr, count, element_size, cmp, index);
    return pos;
}

int _bsearch_del(void *array, int element_size, int *count, cmpfunc cmp, const void *index) {
    if ((*count) == 0) {
        return -1;
    }
    char *arr = (char *)array;
    int pos = _binary_search(arr, *count, element_size, cmp, index);
    if (pos < 0) {
        return -1;
    }
    memmove(arr + (pos * element_size), arr + ((pos + 1) * element_size), ((*count) - pos - 1) * element_size);
    (*count)--;
    return pos;
}

int _bsearch_del_pos(void *array, int element_size, int *count, int pos) {
    if ((*count) == 0) {
        return 0;
    }
    if (pos < 0) {
        return 0;
    }
    char *arr = (char *)array;
    memmove(arr + (pos * element_size), arr + ((pos + 1) * element_size), ((*count) - pos - 1) * element_size);
    (*count)--;
    return 1;
}

int cmp_int64(const void *a, const void *b) {
    long long *_a = (long long *)a;
    long long *_b = (long long *)b;
    if (*_a > *_b) {
        return 1;
    }
    if (*_a < *_b) {
        return -1;
    }
    return 0;
}

int cmp_2int64(const void *a, const void *b) {
    long long *_a = (long long *)a;
    long long *_b = (long long *)b;
    if (*_a > *_b) {
        return 1;
    }
    if (*_a < *_b) {
        return -1;
    }
    _a = _a + 1;
    _b = _b + 1;
    if (*_a > *_b) {
        return 1;
    }
    if (*_a < *_b) {
        return -1;
    }
    return 0;
}

int cmp_int(const void *a, const void *b) {
    int *_a = (int *)a;
    int *_b = (int *)b;
    if (*_a > *_b) {
        return 1;
    }
    if (*_a < *_b) {
        return -1;
    }
    return 0;
}

int cmp_2int(const void *a, const void *b) {
    int *_a = (int *)a;
    int *_b = (int *)b;
    if (*_a > *_b) {
        return 1;
    }
    if (*_a < *_b) {
        return -1;
    }
    _a = _a + 1;
    _b = _b + 1;
    if (*_a > *_b) {
        return 1;
    }
    if (*_a < *_b) {
        return -1;
    }
    return 0;
}