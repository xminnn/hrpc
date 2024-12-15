#pragma once

#define fmap_max_level 12
#define fmap_max_factor 272
#define fmap_max_files 16    // 最大31

struct fmap_index;
struct fmap;

/**
性能测试参考:
    -O0: 100w插入: 1467ms. 100w查找: 390ms
    -O3: 100w查找: 263ms
*/

/**
 * 挂载
 */
struct fmap* fmap_mount(const char* fpath);

/**
 * 卸载
 */
int fmap_unmount(struct fmap* mp);

/**
 * 通知下发磁盘
 */
#define k_fmap_flush_async 0             // 对所有元素进行异步下发
#define k_fmap_flush_sync 0b10           // 同步下发
#define k_fmap_flush_invalidate 0b100    // 下发后，并且从物理内存中清理掉
int fmap_sync(struct fmap_index* element, int flag);

/**
 * 获取map的条目数
 */
int fmap_count(struct fmap* mp);

/**
 * 创建或更新，存在且大小足够的时候更新。不存在的时候创建并且更新
 * val==0 的时候，会按照size进行创建内存。不进行任何内容复制
 */
struct fmap_index* fmap_put(struct fmap* mp, const char* key, const void* val, unsigned int size);

/**
 * ⚠ 仅在确定不存在的情况下使用, 提升插入速度
 * 创建资源, 不会查找是否存在。
 * val==0 的时候，会按照size进行创建内存。不进行任何内容复制
 */
struct fmap_index* fmap_add(struct fmap* mp, const char* key, const void* val, unsigned int size);

/**
 * 查找获取，如果不存则创建
 * ⚠ 如果存在但是大小不一致的话，为了安全会直接assert
 */
struct fmap_index* fmap_touch(struct fmap* mp, const char* key, unsigned int size);

/**
 * 按key获取，不存在返回null
 */
struct fmap_index* fmap_get(struct fmap* mp, const char* key);

/**
 * 按key删除，返回删除后的下一个, 如果存在的话
 */
struct fmap_index* fmap_del(struct fmap* mp, const char* key);

/**
 * 按key返回第一个大于等于的元素
 */
struct fmap_index* fmap_get_ge(struct fmap* mp, const char* key);

/**
 * 按key返回最后一个小于等于的元素
 */
struct fmap_index* fmap_get_le(struct fmap* mp, const char* key);

/**
 * 获取下一个, 指向空表示结束
 */
struct fmap_index* fmap_nxt(struct fmap* mp, struct fmap_index* it);

/**
 * 获取上一个, 指向空表示结束
 */
struct fmap_index* fmap_prv(struct fmap* mp, struct fmap_index* it);

/**
 * 获取element的val指针
 * ⚠ 如果大小不一致的话，为了安全会直接assert
 */
void* fmap_val(struct fmap* mp, struct fmap_index* element, unsigned int safe_size);

/**
 * 获取element的val大小
 */
unsigned int fmap_val_size(struct fmap_index* element);

/**
 * 获取element的key
 */
const char* fmap_key(struct fmap_index* element);
