#include <assert.h>
#include <fcntl.h>
#include <math.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>

#define fmap_max_level 12
#define fmap_max_factor 272
#define fmap_max_files 16
#define fmap_max_idles 64

struct fmap_ptr {
    unsigned long long file : 8;    // 1 是索引文件，2以上是数据块。 0 表示空指针
    unsigned long long offset : 56;
};

#define fmap_ptr_type(type) struct fmap_ptr
#define fmap_ptr_is_null(ptr) (ptr.file == 0)
#define fmap_ptr_no_null(ptr) (ptr.file != 0)
#define fmap_ptr_eqaul(a, b) (a.file == b.file && a.offset == b.offset)
#define fmap_ptr_val(ptr) ((void*)(mp->faddr[ptr.file] + ptr.offset));

struct fmap_index {
    char key[128];
    unsigned long long val_size;
    unsigned long long size;
    fmap_ptr_type(void*) val;
    fmap_ptr_type(struct fmap_index*) next[fmap_max_level];
    fmap_ptr_type(struct fmap_index*) prev;
};

struct fmap_skiplist {
    struct fmap_index _head;
    struct fmap_ptr head;
    fmap_ptr_type(struct fmap_index*) idles[fmap_max_idles];
    int level;
    int count;
    long long fsize;      // 当前文件大小
    long long foffset;    // 下一次内存申请偏移
    char align[224];
};

struct fmap {
    char fpath[512];
    char* faddr[fmap_max_files];
    int fd[fmap_max_files];
    struct fmap_skiplist* skiplist;
};

static void fmap_element_free_(struct fmap* mp, fmap_ptr_type(struct fmap_index*) it) {
    struct fmap_index* idx = fmap_ptr_val(it);
    idx->val_size = 0;
    idx->key[0] = 0;
    idx->prev.file = 0;
    int n = log2(idx->size);
    assert(pow(2, n) == idx->size);    // 取整确保
    idx->next[0] = mp->skiplist->idles[n];
    mp->skiplist->idles[n] = it;
}

static fmap_ptr_type(struct fmap_index*) fmap_index_new_(struct fmap* mp) {
    struct fmap_ptr rst = {0};
    if (mp->skiplist->foffset + (long long)sizeof(struct fmap_index) > mp->skiplist->fsize) {
        munmap(mp->faddr[1], mp->skiplist->fsize);
        long long size = mp->skiplist->fsize * 2;
        if (ftruncate(mp->fd[1], size) == -1) {
            return rst;
        }
        char* ptr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, mp->fd[1], 0);
        if (ptr == MAP_FAILED) {
            return rst;
        }
        mp->skiplist->fsize = size;
    }
    rst.file = 1;
    rst.offset = mp->skiplist->foffset;
    mp->skiplist->foffset += sizeof(struct fmap_index);
    return rst;
}

static fmap_ptr_type(struct fmap_index*) fmap_element_malloc_val_(struct fmap* mp, unsigned int total_size) {
    if (total_size < 4096) {    // 页缓存最小单位，保证能回存
        total_size = 4096;
    }
    int n = ceil(log2(total_size));
    assert(n < fmap_max_idles);
    while (fmap_ptr_is_null(mp->skiplist->idles[n])) {
        int splited = 0;
        for (int i = n + 1; i < fmap_max_idles; i++) {
            if (fmap_ptr_no_null(mp->skiplist->idles[i])) {
                // pop 一个
                struct fmap_ptr e1p = mp->skiplist->idles[i];
                struct fmap_index* e1 = fmap_ptr_val(e1p);
                mp->skiplist->idles[i] = e1->next[0];
                // push 分割成两块
                unsigned long long size = pow(2, i - 1);
                assert(e1->size / 2 == size);
                struct fmap_ptr e2p = fmap_index_new_(mp);
                struct fmap_index* e2 = fmap_ptr_val(e2p);
                e1->size = size;
                e2->size = size;
                e2->val.file = e1->val.file;
                e2->val.offset = e1->val.offset + size;
                fmap_element_free_(mp, e1p);
                fmap_element_free_(mp, e2p);
                msync(e1, sizeof(struct fmap_index), MS_ASYNC);
                msync(e1, sizeof(struct fmap_index), MS_ASYNC);
                splited = 1;
                break;
            }
        }
        if (splited) {
            continue;
        }
        int file_created = 0;
        char path[1024];
        for (int i = 2; i < fmap_max_files; i++) {    // 0 是空，1是索引文件，数据块从2起
            path[snprintf(path, sizeof(path), "%s.%d", mp->fpath, i)] = 0;
            FILE* file = fopen(path, "r");
            if (file) {
                fclose(file);
                continue;
            }
            int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
            if (fd == -1) {
                assert(0);
            }
            unsigned long long size = pow(2, i + 27);    // 512M文件大小起
            if (ftruncate(fd, size) == -1) {
                close(fd);
                assert(0);
            }
            struct flock fl;
            fl.l_type = F_WRLCK;
            fl.l_whence = SEEK_SET;
            fl.l_start = 0;
            fl.l_len = size;
            fl.l_pid = getpid();
            if (fcntl(fd, F_SETLK, &fl) == -1) {
                close(fd);
                assert(0);
            }
            char* ptr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (ptr == MAP_FAILED) {
                close(fd);
                assert(0);
            }
            mp->faddr[i] = ptr;
            mp->fd[i] = fd;

            struct fmap_ptr iptr = fmap_index_new_(mp);
            struct fmap_index* idx = fmap_ptr_val(iptr);
            idx->size = size;
            idx->val.file = i;
            idx->val.offset = 0;
            fmap_element_free_(mp, iptr);
            msync(idx, sizeof(struct fmap_index), MS_ASYNC);    // 数据大小在申请的时候立即下发头结构
            file_created = 1;
            break;
        }
        assert(file_created);
    }

    struct fmap_ptr rptr = mp->skiplist->idles[n];
    struct fmap_index* rst = fmap_ptr_val(rptr);
    mp->skiplist->idles[n] = rst->next[0];
    memset(&rst->next, 0, sizeof(struct fmap_ptr) * (fmap_max_level + 1));
    rst->val_size = 0;
    return rptr;
}

static int random_level_() {
    int lv = 1;
    while (rand() % 1001 < fmap_max_factor && lv < fmap_max_level) {
        ++lv;
    }
    return lv;
}

static inline struct fmap_ptr select_closest_(struct fmap* mp, struct fmap_ptr search, int i, const char* key) {
    struct fmap_index* psearch = fmap_ptr_val(search);
    while (fmap_ptr_no_null(psearch->next[i])) { 
        struct fmap_index* ptr = fmap_ptr_val(psearch->next[i]);
        if (strcmp(ptr->key, key) < 0) {
            search = psearch->next[i];
            psearch = fmap_ptr_val(search);
        } else {
            break;
        }
    }
    return search;
}

struct fmap* fmap_mount(const char* fpath) {
    struct fmap* mp = malloc(sizeof(struct fmap));
    if (!mp || !fpath) {
        ("malloc fmap failed");
        return 0;
    }
    memset(mp, 0, sizeof(struct fmap));
    strcpy(mp->fpath, fpath);
    char path[1024];

    // load index
    {
        path[snprintf(path, sizeof(path), "%s.1", mp->fpath)] = 0;
        FILE* file = fopen(path, "r");
        long long size = 0;
        if (file) {
            fseek(file, 0, SEEK_END);
            size = ftell(file);
            fclose(file);
        }
        int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            free(mp);
            return 0;
        }
        if (!size) {
            size = 1024 * 1024 * 512;
            if (ftruncate(fd, size) == -1) {
                close(fd);
                return 0;
            }
        }
        struct flock fl;
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = size;
        fl.l_pid = getpid();
        if (fcntl(fd, F_SETLK, &fl) == -1) {
            close(fd);
            free(mp);
            return 0;
        }
        char* ptr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            close(fd);
            free(mp);
            return 0;
        }
        mp->faddr[1] = ptr;
        mp->fd[1] = fd;
        mp->skiplist = (struct fmap_skiplist*)ptr;
        if (mp->skiplist->fsize == 0) {
            mp->skiplist->fsize = size;
            mp->skiplist->foffset = sizeof(struct fmap_skiplist);
        } else {
            assert(mp->skiplist->fsize == size);
        }
        mp->skiplist->head.file = 1;
        mp->skiplist->head.offset = 0;
    }

    for (int i = 2; i < fmap_max_files; i++) {
        path[snprintf(path, sizeof(path), "%s.%d", mp->fpath, i)] = 0;
        FILE* file = fopen(path, "r");
        if (!file) {
            continue;
        }
        fclose(file);

        int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            free(mp);
            return 0;
        }
        unsigned long long size = pow(2, i + 27);
        struct flock fl;
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = size;
        fl.l_pid = getpid();
        if (fcntl(fd, F_SETLK, &fl) == -1) {
            close(fd);
            free(mp);
            return 0;
        }
        char* ptr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            close(fd);
            free(mp);
            return 0;
        }
        mp->faddr[i] = ptr;
        mp->fd[i] = fd;
    }
    return mp;
}

void fmap_unmount(struct fmap* mp) {
    for (int i = 2; i < fmap_max_files; i++) {
        if (!mp->faddr[i]) {
            continue;
        }
        unsigned long long size = pow(2, i + 27);
        munmap(mp->faddr[i], size);
        close(mp->fd[i]);
    }
    munmap(mp->faddr[1], mp->skiplist->fsize);
    close(mp->fd[1]);

    memset(mp, 0, sizeof(struct fmap));
    free(mp);
}

int fmap_count(struct fmap* mp) {
    return mp->skiplist->count;
}

struct fmap_index* fmap_get(struct fmap* mp, const char* key) {
    struct fmap_ptr search = mp->skiplist->head;
    for (int i = mp->skiplist->level - 1; i >= 0; i--) {
        search = select_closest_(mp, search, i, key);
        struct fmap_index* psearch = fmap_ptr_val(search);
        struct fmap_ptr tar = psearch->next[i];
        if (fmap_ptr_no_null(tar)) {
            struct fmap_index* ptar = fmap_ptr_val(tar);
            if (0 == strcmp(ptar->key, key)) {
                return ptar;
            }
        }
    }
    return 0;
}

struct fmap_index* fmap_add(struct fmap* mp, const char* key, const void* val, unsigned int size);
struct fmap_index* fmap_del(struct fmap* mp, const char* key);
struct fmap_index* fmap_touch(struct fmap* mp, const char* key, unsigned int size) {
    struct fmap_index* it = fmap_get(mp, key);
    if (it) {
        if (it->val_size != size) {
            assert(0);    // 直接跪掉，不然丢数据
        }
        return it;
    }
    return fmap_add(mp, key, 0, size);
}
struct fmap_index* fmap_put(struct fmap* mp, const char* key, const void* val, unsigned int size) {
    struct fmap_index* it = fmap_get(mp, key);
    if (it) {
        if (it->size < size) {
            fmap_del(mp, key);
        } else {
            it->val_size = size;
            void* tar = fmap_ptr_val(it->val);
            if (val) {
                memcpy(tar, val, size);
            } else {
                memset(tar, 0, size);
            }
            return it;
        }
    }
    return fmap_add(mp, key, val, size);
}

static void fmap_add_(struct fmap* mp, struct fmap_ptr element) {
    int lv = random_level_();
    for (int i = mp->skiplist->level; i < lv; i++) {
        mp->skiplist->_head.next[i] = element;
    }
    struct fmap_index* pelement = fmap_ptr_val(element);
    struct fmap_ptr search = mp->skiplist->head;
    struct fmap_index* psearch = fmap_ptr_val(search);
    for (int i = mp->skiplist->level - 1; i >= 0; i--) {
        search = select_closest_(mp, search, i, pelement->key);
        psearch = fmap_ptr_val(search);
        if (i >= lv)
            continue;
        if (fmap_ptr_no_null(psearch->next[i]))
            pelement->next[i] = psearch->next[i];
        psearch->next[i] = element;
        if (i == 0) {
            pelement->prev = search;
        }
    }
    if (lv > mp->skiplist->level) {
        mp->skiplist->level = lv;
    }
    mp->skiplist->count++;
}
struct fmap_index* fmap_add(struct fmap* mp, const char* key, const void* val, unsigned int size) {
    struct fmap_ptr ptr = fmap_element_malloc_val_(mp, size);
    struct fmap_index* element = fmap_ptr_val(ptr);
    strncpy(element->key, key, sizeof(element->key) - 1);
    element->key[sizeof(element->key) - 1] = 0;
    element->val_size = size;
    void* tar = fmap_ptr_val(element->val);
    if (val) {
        memcpy(tar, val, size);
    } else {
        memset(tar, 0, size);
    }
    fmap_add_(mp, ptr);
    return element;
}

struct fmap_index* fmap_get_ge(struct fmap* mp, const char* key) {
    struct fmap_ptr search = mp->skiplist->head;
    struct fmap_index* psearch = fmap_ptr_val(search);
    struct fmap_index* le = 0;
    for (int i = mp->skiplist->level - 1; i >= 0; i--) {
        search = select_closest_(mp, search, i, key);
        psearch = fmap_ptr_val(search);
        struct fmap_ptr tar = psearch->next[i];
        if (fmap_ptr_no_null(tar)) {
            struct fmap_index* ptar = fmap_ptr_val(tar);
            if (strcmp(ptar->key, key) == 0) {
                return ptar;
            }
            le = ptar;
        }
    }
    return le;
}

struct fmap_index* fmap_get_le(struct fmap* mp, const char* key) {
    struct fmap_index* rst = fmap_get_ge(mp, key);
    while (1) {
        if (!rst) {
            return 0;
        }
        if (strcmp(rst->key, key) <= 0) {
            return rst;
        }
        rst = fmap_ptr_val(rst->prev);
    }
}

struct fmap_index* fmap_del(struct fmap* mp, const char* key) {
    struct fmap_ptr search = mp->skiplist->head;
    struct fmap_index* psearch = fmap_ptr_val(search);
    struct fmap_ptr find = {0};
    struct fmap_ptr next = {0};
    for (int i = mp->skiplist->level - 1; i >= 0; i--) {
        search = select_closest_(mp, search, i, key);
        psearch = fmap_ptr_val(search);
        struct fmap_ptr tar = psearch->next[i];
        if (fmap_ptr_no_null(tar)) {
            struct fmap_index* ptar = fmap_ptr_val(tar);
            if (0 == strcmp(ptar->key, key)) {
                psearch->next[i] = ptar->next[i];
                next = ptar->next[i];
                find = tar;
            }
        }
    }
    mp->skiplist->count--;
    if (fmap_ptr_no_null(find)) {
        fmap_element_free_(mp, find);
    }
    return fmap_ptr_val(next);
}

struct fmap_index* fmap_nxt(struct fmap* mp, struct fmap_index* it) {
    return fmap_ptr_val(it->next[0]);
}

struct fmap_index* fmap_prv(struct fmap* mp, struct fmap_index* it) {
    if (fmap_ptr_eqaul(it->prev, mp->skiplist->head)) {
        return 0;
    }
    return fmap_ptr_val(it->prev);
}

void* fmap_val(struct fmap* mp, struct fmap_index* element, unsigned int safe_size) {
    if (element->val_size != safe_size) {
        assert(0);
    }
    return fmap_ptr_val(element->val);
}

unsigned int fmap_val_size(struct fmap_index* element) {
    return element->val_size;
}

const char* fmap_key(struct fmap_index* element) {
    return element->key;
}

#define k_fmap_flush_async 0
#define k_fmap_flush_sync 0b10
#define k_fmap_flush_invalidate 0b100

int fmap_sync(struct fmap_index* element, int flag) {
    int _flag = 0;
    if (flag & k_fmap_flush_sync) {
        _flag |= MS_SYNC;
    } else {
        _flag |= MS_ASYNC;
    }
    if (flag & k_fmap_flush_invalidate) {
        _flag |= MS_INVALIDATE;
    }
    return msync(element, element->size, _flag);
}
