#include "hashmap.h"

#include <assert.h>
#include <memory.h>
#include <stdlib.h>

#define k_hash_conflict_list 8

struct hashmap_element {
    int overflow;                         // 溢出大小，溢出后向前查找
    short count;                          // datas中自身占用数量
    short borrow;                         // datas中借出去的数量
    void* datas[k_hash_conflict_list];    // 数据链表
};

struct hashmap {
    int element_size;
    int count;
    int capacity;
    int init_capacity;
    struct hashmap_element* arr;
    int (*func_hashcode)(const void*);
    int (*func_equal)(const void*, const void*);
};

struct hashmap* hashmap_create(int init_cap, int element_size, int (*func_hashcode)(const void*), int (*func_equal)(const void*, const void*)) {
    struct hashmap* self = (struct hashmap*)malloc(sizeof(struct hashmap));
    self->capacity = init_cap;
    self->count = 0;
    self->element_size = element_size;
    self->func_equal = func_equal;
    self->func_hashcode = func_hashcode;
    self->init_capacity = init_cap;
    self->arr = (struct hashmap_element*)malloc(sizeof(struct hashmap_element) * self->capacity);
    memset(self->arr, 0, sizeof(struct hashmap_element) * self->capacity);
    return self;
}

void hashmap_free(struct hashmap* self) {
    if (self->element_size) {
        hashmap_foreach(void*, val, self) {
            free(val);
        }
        self->element_size = 0;
    }
    memset(self->arr, 0, sizeof(struct hashmap_element) * self->capacity);
    free(self->arr);
    free(self);
}

int hashmap_count(struct hashmap* self) {
    return self->count;
}

void hashmap_resize(struct hashmap* self, int capacity) {
    struct hashmap_element* old = self->arr;
    int old_cap = self->capacity;
    self->capacity = capacity;
    self->arr = (struct hashmap_element*)malloc(sizeof(struct hashmap_element) * self->capacity);
    self->count = 0;
    memset(self->arr, 0, sizeof(struct hashmap_element) * self->capacity);
    for (int i = 0; i < old_cap; i++) {
        struct hashmap_element* tmp = &old[i];
        for (int j = 0; j < tmp->count; j++) {
            hashmap_add(self, tmp->datas[j]);
        }
        for (int j = 0; j < tmp->borrow; j++) {
            hashmap_add(self, tmp->datas[k_hash_conflict_list - 1 - j]);
        }
    }
    free(old);
}

#define hashmap_init_hashcode(name, data) \
    int name = self->func_hashcode(data); \
    name ^= (name >> 16);

static inline struct hashmap_element* hashmap_get_(struct hashmap* self, const void* key, int* pos) {
    hashmap_init_hashcode(hashcode, key);

    int p = hashcode % self->capacity;
    struct hashmap_element* it = &self->arr[p];
    if (it->count > 0) {
        for (int i = 0; i < it->count; i++) {
            if (self->func_equal(it->datas[i], key) == 1) {
                *pos = i;
                return it;
            }
        }
    }

    int overflow = it->overflow;
    int ep = p;
    while (overflow > 0) {
        p = p - 1;
        if (p < 0) {
            p = self->capacity - 1;
        }
        if (p == ep) {
            break;
        }
        it = &self->arr[p];
        if (it->borrow > 0) {
            for (int i = 0; i < it->borrow; i++) {
                int k = k_hash_conflict_list - 1 - i;
                hashmap_init_hashcode(ohashcode, it->datas[k]);
                if (ohashcode == hashcode) {
                    if (self->func_equal(it->datas[k], key) == 1) {
                        *pos = k;
                        return it;
                    }
                    overflow--;
                }
            }
        }
    }
    return 0;
}

void* hashmap_add(struct hashmap* self, void* data) {    // must not exist
    if (self->count > self->capacity) {
        hashmap_resize(self, self->capacity * 2);
        return hashmap_add(self, data);
    }

    if (self->element_size) {
        void* tmp = malloc(self->element_size);
        memcpy(tmp, data, self->element_size);
        data = tmp;
    }

    hashmap_init_hashcode(hashcode, data);
    int p = hashcode % self->capacity;
    struct hashmap_element* it = &self->arr[p];
    if (it->count + it->borrow < k_hash_conflict_list) {
        it->datas[it->count++] = (void*)data;
        ++self->count;
        return data;
    }

    it->overflow++;
    self->count++;
    int ep = p;
    while (1) {
        p = p - 1;
        if (p < 0) {
            p = self->capacity - 1;
        }
        if (p == ep) {
            assert(0);
            break;
        }
        it = &self->arr[p];
        if (it->count + it->borrow < k_hash_conflict_list) {
            it->datas[k_hash_conflict_list - 1 - it->borrow] = data;
            it->borrow++;
            return data;
        }
    }
    return 0;
}

void* hashmap_put(struct hashmap* self, void* data) {
    int i;
    struct hashmap_element* it = hashmap_get_(self, data, &i);
    if (it) {
        if (self->element_size) {
            void* tmp = malloc(self->element_size);
            memcpy(tmp, data, self->element_size);
            data = tmp;
        }
        it->datas[i] = data;
        return it->datas[i];
    }
    return hashmap_add(self, data);
}

void* hashmap_get(struct hashmap* self, const void* key) {
    int i;
    struct hashmap_element* it = hashmap_get_(self, key, &i);
    if (it) {
        return it->datas[i];
    }
    return 0;
}

void hashmap_del(struct hashmap* self, const void* key) {
    int pos;
    struct hashmap_element* it = hashmap_get_(self, key, &pos);
    if (!it) {
        return;
    }

    hashmap_init_hashcode(hashcode, key);
    int p = hashcode % self->capacity;
    if (self->element_size) {
        free(it->datas[pos]);
        it->datas[pos] = 0;
    }
    
    struct hashmap_element* tar = &self->arr[p];
    if (it == tar) {
        if (it->count > 0) {
            it->datas[pos] = it->datas[it->count - 1];
        }
        it->count--;
        assert(it->count >= 0);
    } else {
        if (it->borrow > 1) {
            it->datas[pos] = it->datas[k_hash_conflict_list - it->borrow];
        }
        it->borrow--;
        tar->overflow--;
        assert(it->borrow >= 0);
        assert(tar->overflow >= 0);
    }
    self->count--;
    assert(self->count >= 0);

    if (self->capacity > self->init_capacity && self->count * 3 < self->capacity) {
        hashmap_resize(self, self->capacity / 2);
    }
}

struct hashmap_itor hashmap_itor_next(struct hashmap* self, struct hashmap_itor it) {
    int i = it.i;
    int j = it.j;
    int where = it.where;
    if (it.where == 0) {
        i = 0;
        j = 0;
        where = 1;
    } else {
        j += 1;
    }
    for (; i < self->capacity; i++) {
        if (where == 1) {
            if (self->arr[i].count > 0) {
                if (j < self->arr[i].count) {
                    return (struct hashmap_itor){.i = i, .j = j, .where = where};
                }
            }
            j = 0;
            where = 2;
        }
        if (where == 2) {
            if (self->arr[i].borrow > 0) {
                if (j < self->arr[i].borrow) {
                    return (struct hashmap_itor){.i = i, .j = j, .where = where};
                }
            }
            j = 0;
            where = 1;
        }
    }
    return (struct hashmap_itor){.i = 0, .j = 0, .where = 0};
}

void* hashmap_itor_val(struct hashmap* self, struct hashmap_itor it) {
    if (it.where == 1) {
        return self->arr[it.i].datas[it.j];
    }
    if (it.where == 2) {
        return self->arr[it.i].datas[k_hash_conflict_list - it.j - 1];
    }
    return 0;
}