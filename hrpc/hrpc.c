#include <arpa/inet.h>
#include <errno.h>
#include <math.h>
#include <memory.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <threads.h>
#include <time.h>
#include <unistd.h>

#include "bsearch.h"
#include "fmap.h"
#include "hashmap.h"

static long long time_curruent_us() {
    long long now;
    struct timeval tv;
    gettimeofday(&tv, 0);
    now = tv.tv_sec;
    now = now * 1000000;
    now += tv.tv_usec;
    return now;
}

static long long time_curruent_ms() {
    long long now;
    struct timeval tv;
    gettimeofday(&tv, 0);
    now = tv.tv_sec;
    now = now * 1000000;
    now += tv.tv_usec;
    return now / 1000;
}

static int util_rand(int min, int max) {
    static unsigned long long seed;
    static char seed_init = 0;
    static const unsigned long long a = 1103515245;
    static const unsigned long long c = 12345;
    static const unsigned long long m = 1 << 31;    // 2^31

    if (seed_init == 0) {
        seed_init = 1;
        seed = time_curruent_us();
    }
    seed = (a * seed + c) % m;    // 线性同余生成器
    if (max <= min) {
        return min;
    }
    return seed % (max - min + 1) + min;
}

#define k_hrpc_frame_data 0
#define k_hrpc_frame_ack 1
#define k_hrpc_frame_heartbeat 2

#define get_frame_count(size) ((size + 1023) / 1024)
#define offsetof(type, member) ((size_t) & ((type*)0)->member)

struct hrpc_frame {
    unsigned long long id;
    unsigned int nid;
    unsigned int size;
    long long connect_time;
    unsigned int type;
    union {
        struct {
            unsigned int i;
            char buff[1024];
        } pack;
        struct {
            unsigned int count;
            unsigned int recived[256];
        } ack;
        struct {
            unsigned long long reci;
            unsigned long long send;
            char _;
        } sync;
    } data;
};

struct hrpc_pack {
    unsigned long long id;
    unsigned int nid;
    unsigned int size;
    long long connect_time;
    long long last_time;
    unsigned int retry;
    char* done;
    char* buff;
};

struct hrpc_connection {
    int nid;
    unsigned long long send;     // 发送
    unsigned long long acked;    // 已经发送并且被acked了的
    unsigned long long reci;     // 已经接收完毕并且处理掉了的
    long long connect_time;
    long long active_time;
    long long last_heartbeat_time;
    struct sockaddr_in target_addr;
};

struct hrpc_connections {                             // 需要持久化。内部服务节点一般比较稳定变动小，这里使用数组为了获得更佳性能。如果是管理与客户端之间的连接，需要改成fmap来管理和遍历
    struct hrpc_connection connections[1024 * 10];    // 目前只用于服务端，暂定需要1w以内连接
    int connections_count_;
};

static struct {
    struct hashmap* send;
    struct hashmap* reci;
    struct hashmap* done;
    struct fmap* db;
    struct hrpc_connections* connections;
    int sockfd;
    int is_server;
    int nid;
    struct sockaddr_in (*get_addr)(int nid);
    long long once_timeout;
} self;

int hrpc_pack_hashcode_(const void* ptr) {
    const struct hrpc_pack* a = ptr;
    unsigned long long id = a->id << 8;
    id += a->nid;
    return id;
}

int hrpc_pack_equal_(const void* a, const void* b) {
    const struct hrpc_pack* m = a;
    const struct hrpc_pack* n = b;
    return m->id == n->id && m->nid == n->nid;
}

int hrpc_init(const char* dbpath, int nid, int bind_port, struct sockaddr_in (*get_addr)(int nid)) {    // 初始化
    self.get_addr = get_addr;
    self.nid = nid;
    self.sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (self.sockfd < 0) {
        return 0;
    }
    int buffer_size = 26214400;    // 25 MB
    socklen_t len = sizeof(buffer_size);
    setsockopt(self.sockfd, SOL_SOCKET, SO_RCVBUF, &buffer_size, len);
    int ret = getsockopt(self.sockfd, SOL_SOCKET, SO_RCVBUF, &buffer_size, &len);

    self.db = fmap_mount(dbpath);
    struct fmap_index* fi = fmap_touch(self.db, "/connections", sizeof(struct hrpc_connections));
    self.connections = fmap_val(self.db, fi, sizeof(struct hrpc_connections));
    self.send = hashmap_create(1000, 0, hrpc_pack_hashcode_, hrpc_pack_equal_);
    self.reci = hashmap_create(1000, 0, hrpc_pack_hashcode_, hrpc_pack_equal_);

    struct fmap_index* start = fmap_get_ge(self.db, "/send/");
    struct fmap_index* end = fmap_get_ge(self.db, "/send/~");
    while (start && start != end) {
        struct fmap_index* current = start;
        start = fmap_nxt(self.db, start);
        struct hrpc_pack* pack = fmap_val(self.db, current, fmap_val_size(current));
        pack->done = ((char*)pack) + sizeof(struct hrpc_pack);
        pack->buff = ((char*)pack) + sizeof(struct hrpc_pack) + get_frame_count(pack->size);
        hashmap_add(self.send, pack);
    }

    start = fmap_get_ge(self.db, "/reci/");
    end = fmap_get_ge(self.db, "/reci/~");
    while (start && start != end) {
        struct fmap_index* current = start;
        start = fmap_nxt(self.db, start);
        struct hrpc_pack* pack = fmap_val(self.db, current, fmap_val_size(current));
        pack->done = ((char*)pack) + sizeof(struct hrpc_pack);
        pack->buff = ((char*)pack) + sizeof(struct hrpc_pack) + get_frame_count(pack->size);
        hashmap_add(self.reci, pack);
    }

    if (bind_port) {
        struct sockaddr_in server_addr = {0};
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(bind_port);
        int ret = bind(self.sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr));
        if (ret == -1) {
            close(self.sockfd);
            hashmap_free(self.reci);
            hashmap_free(self.send);
            fmap_unmount(self.db);
            self.sockfd = 0;
            self.reci = 0;
            self.send = 0;
            self.db = 0;
            return 0;
        }
        self.is_server = 1;
    }
    for (int i = 0; i < self.connections->connections_count_; i++) {
        struct hrpc_connection* conn = &self.connections->connections[i];
        conn->last_heartbeat_time = 0;
    }
    return self.sockfd;
}

int hrpc_touch_connect(int nid) {
    if (self.is_server) {
        return 0;
    }
    struct hrpc_connection* conn = bsearch_get(self.connections->connections, cmp_int, &nid);
    if (!conn) {
        static typeof(*conn) tmp = {0};
        tmp.nid = nid;
        tmp.connect_time = time_curruent_us();
        tmp.active_time = 0;
        tmp.target_addr = self.get_addr(nid);
        int p = bsearch_put(self.connections->connections, cmp_int, &tmp);
        conn = &self.connections->connections[p];
    } else {
        conn->target_addr = self.get_addr(nid);
    }
    return 1;
}

int hrpc_is_connected(int nid) {
    struct hrpc_connection* conn = bsearch_get(self.connections->connections, cmp_int, &nid);
    return conn != 0;
}

int hrpc_once_timeout() {
    return self.once_timeout;
}

void hrpc_send_udp_(struct hrpc_connection* conn, struct hrpc_frame* frame) {
    if (!conn) {
        return;
    }
    if (self.is_server) {
        if (conn->active_time + 3000 < time_curruent_ms()) {    // 未活跃，直接放弃发送，等待活跃后发送
            return;
        }
    } else {
        if (conn->active_time + 3000 < time_curruent_ms() && frame->type != k_hrpc_frame_heartbeat) {
            return;
        }
    }
    int size = 0;
    if (frame->type == k_hrpc_frame_ack) {
        size = (const int)offsetof(struct hrpc_frame, data.ack.recived) + (frame->data.ack.count * sizeof(unsigned int));
    } else if (frame->type == k_hrpc_frame_data) {
        size = (const int)offsetof(struct hrpc_frame, data.pack.buff) + (frame->data.pack.i < get_frame_count(frame->size) - 1 ? 1024 : frame->size - frame->data.pack.i * 1024);
    } else {
        size = (const int)offsetof(struct hrpc_frame, data.sync._);
    }
    sendto(self.sockfd, frame, size, 0, (struct sockaddr*)&conn->target_addr, sizeof(struct sockaddr_in));
}

static unsigned long long retry_count = 0;

int hrpc_send_once_(struct hrpc_pack* pack, long long curtime) {
    static const long long delays[] = {100, 200, 500, 500, 200};
    static const int delays_count = sizeof(delays) / sizeof(long long);
    long long nextime = pack->last_time + delays[pack->retry % delays_count];
    if (nextime > curtime) {
        return nextime - curtime;
    }
    if (pack->retry > 0) {
        retry_count++;
    }
    pack->retry++;
    pack->last_time = curtime;
    int frame_count = get_frame_count(pack->size);
    int send_count = 0;
    struct hrpc_connection* conn = bsearch_get(self.connections->connections, cmp_int, &pack->nid);
    for (int p = 0; p < frame_count; p++) {
        if (pack->done[p]) {
            continue;
        }
        static struct hrpc_frame frame;
        frame.type = k_hrpc_frame_data;
        frame.id = pack->id;
        frame.nid = self.nid;
        frame.size = pack->size;
        frame.connect_time = pack->connect_time;
        frame.data.pack.i = p;
        memcpy(frame.data.pack.buff, pack->buff + p * 1024, p < frame_count - 1 ? 1024 : pack->size - p * 1024);
        hrpc_send_udp_(conn, &frame);
        send_count++;
    }
    return 0;
}

void* hrpc_send(int nid, int size) {
    if (nid == 0) {
        return 0;
    }
    struct hrpc_connection* conn = bsearch_get(self.connections->connections, cmp_int, &nid);
    if (!conn) {
        typeof(*conn) tmp = {0};
        tmp.nid = nid;
        tmp.connect_time = time_curruent_us();
        tmp.target_addr = self.get_addr(nid);
        int p = bsearch_put(self.connections->connections, cmp_int, &tmp);
        conn = &self.connections->connections[p];
    }
    unsigned long long id = ++conn->send;
    char path[128];
    snprintf(path, sizeof(path), "/send/%d/%llu", nid, id);
    int psize = sizeof(struct hrpc_pack) + get_frame_count(size) + size;
    struct fmap_index* fi = fmap_add(self.db, path, 0, psize);
    struct hrpc_pack* pack = fmap_val(self.db, fi, psize);
    pack->id = id;
    pack->nid = nid;
    pack->size = size;
    pack->connect_time = conn->connect_time;
    pack->last_time = 0;
    pack->retry = 0;
    pack->done = ((char*)pack) + sizeof(struct hrpc_pack);
    pack->buff = ((char*)pack) + sizeof(struct hrpc_pack) + get_frame_count(size);
    hashmap_add(self.send, pack);
    self.once_timeout = 0;
    return pack->buff;
}

void hrpc_reci_udp_(void* buff, int size, struct sockaddr_in* target_addr) {
    struct hrpc_frame* frame = buff;
    long long curtime = time_curruent_ms();
    char path[128];
    struct hrpc_connection* conn = bsearch_get(self.connections->connections, cmp_int, &frame->nid);
    if (conn && conn->connect_time != frame->connect_time) {
        conn = 0;
    }
    if (!conn) {
        static typeof(*conn) tmp = {0};
        tmp.nid = frame->nid;
        tmp.connect_time = frame->connect_time;
        tmp.active_time = curtime;
        tmp.target_addr = *target_addr;
        int p = bsearch_put(self.connections->connections, cmp_int, &tmp);
        conn = &self.connections->connections[p];
        // 删除所有的发送缓存
        long long send_clear = 0;
        long long reci_clear = 0;
        snprintf(path, sizeof(path), "/send/%d/", frame->nid);
        struct fmap_index* start = fmap_get_ge(self.db, path);
        snprintf(path, sizeof(path), "/send/%d/~", frame->nid);
        struct fmap_index* end = fmap_get_ge(self.db, path);
        while (start && start != end) {
            struct fmap_index* current = start;
            start = fmap_nxt(self.db, start);
            struct hrpc_pack* pack = fmap_val(self.db, current, fmap_val_size(current));
            hashmap_del(self.send, pack);
            fmap_del(self.db, fmap_key(current));
            send_clear++;
        }
        // 删除所有的接收缓存
        snprintf(path, sizeof(path), "/reci/%d/", frame->nid);
        start = fmap_get_ge(self.db, path);
        snprintf(path, sizeof(path), "/reci/%d/~", frame->nid);
        end = fmap_get_ge(self.db, path);
        while (start && start != end) {
            struct fmap_index* current = start;
            start = fmap_nxt(self.db, start);
            struct hrpc_pack* pack = fmap_val(self.db, current, fmap_val_size(current));
            hashmap_del(self.reci, pack);
            fmap_del(self.db, fmap_key(current));
            reci_clear++;
        }
    }
    if (frame->type == k_hrpc_frame_ack) {
        static struct hrpc_pack key;
        key.nid = conn->nid;
        key.id = frame->id;
        struct hrpc_pack* pack = hashmap_get(self.send, &key);
        if (pack) {
            int frame_count = get_frame_count(pack->size);
            for (unsigned int i = 0; i < frame->data.ack.count; i++) {
                pack->done[frame->data.ack.recived[i]] = 1;
            }
            for (int i = 0; i < frame_count; i++) {
                if (!pack->done[i]) {
                    pack = 0;
                    break;
                }
            }
        }
        if (pack) {
            char path[128];
            snprintf(path, sizeof(path), "/send/%u/%llu", key.nid, key.id);
            hashmap_del(self.send, &key);
            fmap_del(self.db, path);
        }
        // 这里不需要回复，只有接收方发送ack. 如果接收方的ack丢失问题也不大，无非再发一次，然后每2秒心跳会同步一次reci，所以不会造成一直重复发
    } else if (frame->type == k_hrpc_frame_data) {
        if (frame->id > conn->reci) {
            static struct hrpc_pack key;
            key.id = frame->id;
            key.nid = frame->nid;
            struct hrpc_pack* pack = hashmap_get(self.reci, &key);
            unsigned int frame_count = get_frame_count(frame->size);
            if (frame->data.pack.i >= frame_count) {
                return;
            }
            if (!pack) {
                snprintf(path, sizeof(path), "/reci/%u/%llu", frame->nid, frame->id);
                int psize = sizeof(struct hrpc_pack) + frame_count + frame->size;
                struct fmap_index* fi = fmap_add(self.db, path, 0, psize);
                pack = fmap_val(self.db, fi, psize);
                pack->id = key.id;
                pack->nid = key.nid;
                pack->done = ((char*)pack) + sizeof(struct hrpc_pack);
                pack->buff = ((char*)pack) + sizeof(struct hrpc_pack) + frame_count;
                hashmap_add(self.reci, pack);
            }
            if (!pack->done[frame->data.pack.i]) {
                pack->id = frame->id;
                pack->nid = frame->nid;
                pack->size = frame->size;
                pack->connect_time = frame->connect_time;
                pack->retry = 0;
                memcpy(pack->buff + frame->data.pack.i * 1024, frame->data.pack.buff, frame->data.pack.i < frame_count - 1 ? 1024 : frame->size - frame->data.pack.i * 1024);
            }
            pack->done[frame->data.pack.i] = 1;
        }
    } else {
        if (frame->data.sync.reci > conn->acked) {
            static struct hrpc_pack key;
            key.nid = conn->nid;
            key.id = frame->id;
            for (unsigned long long i = conn->acked + 1; i <= frame->data.sync.reci; i++) {
                key.id = i;
                struct hrpc_pack* pack = hashmap_get(self.send, &key);
                if (pack) {
                    char path[128];
                    snprintf(path, sizeof(path), "/send/%u/%llu", key.nid, key.id);
                    hashmap_del(self.send, &key);
                    fmap_del(self.db, path);
                }
            }
            conn->acked = frame->data.sync.reci;
        }
        if (self.is_server) {
            static struct hrpc_frame heartbeat;
            heartbeat.type = k_hrpc_frame_heartbeat;
            heartbeat.id = 0;
            heartbeat.nid = self.nid;
            heartbeat.size = 0;
            heartbeat.connect_time = conn->connect_time;
            heartbeat.data.sync.reci = conn->reci;
            heartbeat.data.sync.send = conn->send;
            hrpc_send_udp_(conn, &heartbeat);
        }
    }
    conn->active_time = curtime;
    conn->target_addr = *target_addr;
}

static unsigned long long try_handle = 0;
static unsigned long long real_handle = 0;
static unsigned long long send_heartbeat = 0;

int hrpc_once(void (*on_message)(int nid, void* message, unsigned int size)) {
    self.once_timeout = 1000;

    // 处理掉所有的
    for (int i = 0; i < self.connections->connections_count_; i++) {
        struct hrpc_connection* conn = &self.connections->connections[i];
        static struct hrpc_pack key;
        key.nid = conn->nid;
        while (1) {
            key.id = conn->reci + 1;
            struct hrpc_pack* find = hashmap_get(self.reci, &key);
            if (find) {
                try_handle++;
                int count = get_frame_count(find->size);
                for (int k = 0; k < count; k++) {
                    if (!find->done[k]) {
                        find = 0;
                        break;
                    }
                }
            }
            if (!find) {
                break;
            }
            real_handle++;
            on_message(find->nid, find->buff, find->size);
            hashmap_del(self.reci, &key);
            char path[128];
            snprintf(path, sizeof(path), "/reci/%u/%llu", key.nid, key.id);
            fmap_del(self.db, path);
            conn->reci += 1;
        }
    }

    // 接收请求
    char buff[1500];
    if (self.is_server) {
        struct sockaddr_in target_addr;
        unsigned int len = sizeof(target_addr);
        int times = 10000;
        while (times-- > 0) {
            int nbytes = recvfrom(self.sockfd, &buff, sizeof(buff), MSG_DONTWAIT, (struct sockaddr*)&target_addr, &len);
            if (nbytes <= 0) {
                break;
            }
            hrpc_reci_udp_(buff, nbytes, &target_addr);
            self.once_timeout = 0;
        }
    } else {
        for (int i = 0; i < self.connections->connections_count_; i++) {
            struct hrpc_connection* conn = &self.connections->connections[i];
            unsigned int len = sizeof(conn->target_addr);
            int times = 10000;
            while (times-- > 0) {
                int nbytes = recvfrom(self.sockfd, &buff, sizeof(buff), MSG_DONTWAIT, (struct sockaddr*)&conn->target_addr, &len);
                if (nbytes <= 0) {
                    break;
                }
                hrpc_reci_udp_(buff, nbytes, &conn->target_addr);
                self.once_timeout = 0;
            }
        }
    }

    long long curtime = time_curruent_ms();

    // 发送重试。随机起点是为了降低阻塞概率: 极端情况, 如果一个包随机定位到数组最后边, 前边一直在填充并且发送, 造成对端阻塞(永远无法收到最后一个), 对端消费可能会持续卡住直到网络压力缓解。
    int rand = util_rand(0, hashmap_count(self.send) - 1);
    int t = 0;
    hashmap_foreach(struct hrpc_pack*, pack, self.send) {
        if (t++ >= rand) {
            long long nextimeout = hrpc_send_once_(pack, curtime);
            if (nextimeout < self.once_timeout) {
                self.once_timeout = nextimeout;
            }
        }
    }
    t = 0;
    hashmap_foreach(struct hrpc_pack*, pack, self.send) {
        if (t++ < rand) {
            long long nextimeout = hrpc_send_once_(pack, curtime);
            if (nextimeout < self.once_timeout) {
                self.once_timeout = nextimeout;
            }
        } else {
            break;
        }
    }

    // 接收包ack
    hashmap_foreach(struct hrpc_pack*, pack, self.reci) {
        int frame_count = get_frame_count(pack->size);
        struct hrpc_connection* conn = bsearch_get(self.connections->connections, cmp_int, &pack->nid);
        static struct hrpc_frame frame;
        frame.type = k_hrpc_frame_ack;
        frame.id = pack->id;
        frame.nid = self.nid;
        frame.size = 0;
        frame.connect_time = pack->connect_time;
        frame.data.ack.count = 0;
        for (int p = 0; p < frame_count; p++) {
            if (pack->done[p] != 1) {
                continue;
            }
            pack->done[p] = 2;
            if (frame.data.ack.count >= 256) {
                hrpc_send_udp_(conn, &frame);
                frame.data.ack.count = 0;
            }
            frame.data.ack.recived[frame.data.ack.count++] = p;
        }
        if (frame.data.ack.count > 0) {
            hrpc_send_udp_(conn, &frame);
        }
    }

    // 客户端维护心跳
    if (!self.is_server) {
        for (int i = 0; i < self.connections->connections_count_; i++) {
            struct hrpc_connection* conn = &self.connections->connections[i];
            long long nextime = conn->last_heartbeat_time + 2000L;
            if (nextime > curtime) {
                long long nextimeout = nextime - curtime;
                if (nextimeout < self.once_timeout) {
                    self.once_timeout = nextimeout;
                }
                continue;
            }
            send_heartbeat++;
            conn->last_heartbeat_time = curtime;
            static struct hrpc_frame heartbeat = {0};
            heartbeat.type = k_hrpc_frame_heartbeat;
            heartbeat.nid = self.nid;
            heartbeat.connect_time = conn->connect_time;
            heartbeat.data.sync.reci = conn->reci;
            heartbeat.data.sync.send = conn->send;
            hrpc_send_udp_(conn, &heartbeat);
        }
    }

    return self.once_timeout;
}