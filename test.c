#include <arpa/inet.h>
#include <errno.h>
#include <math.h>
#include <memory.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "hashmap.h"
#include "hrpc.h"

static long long time_curruent_ms() {
    long long now;
    struct timeval tv;
    gettimeofday(&tv, 0);
    now = tv.tv_sec;
    now = now * 1000000;
    now += tv.tv_usec;
    return now / 1000;
}

struct sockaddr_in get_addr(int nid) {
    struct sockaddr_in addr = {0};
    if (nid == 1) {
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        addr.sin_port = htons(5670);
    }
    return addr;
}

static long long server_count = 0;
static long long server_count_size = 0;
static long long server_count_time = 0;

#define k_test_size 100

void on_client_data(int nid, void* data, unsigned int size) {
    char* text = hrpc_send(1, k_test_size);
    strcpy(text, "123321");
    server_count_size += size;
    server_count++;
}

void on_server_data(int nid, void* data, unsigned int size) {
    server_count_size += size;
    server_count++;
}

static int exited = 0;
void signal_handler_(int signum) {
    exited = 1;
    printf("app exit: signum=%d\n", signum);
}

int main(int argc, char const* argv[]) {
    signal(SIGINT, signal_handler_);
    signal(SIGQUIT, signal_handler_);
    signal(SIGTERM, signal_handler_);

    if (argc < 2) {
        printf("need argv[1]\n");
        return -1;
    }
    void (*on_data)(int nid, void* data, unsigned int size);
    int is_client = 1;
    if (strcmp(argv[1], "server") == 0) {
        int ret = hrpc_init("./fmap.bin.server", 1, 5670, get_addr);
        if (ret == 0) {
            printf("server init failed\n");
            return -1;
        }
        on_data = on_server_data;
        is_client = 0;
    }
    if (strcmp(argv[1], "client") == 0) {
        int ret = hrpc_init("./fmap.bin.client", 2, 0, get_addr);
        for (int i = 0; i < 200; i++) {
            char* text = hrpc_send(1, k_test_size);
            strcpy(text, "123321");
        }
        if (ret == 0) {
            printf("client init failed\n");
            return -1;
        }
        on_data = on_client_data;
    }
    while (!exited) {
        hrpc_once(on_data);
        long long curtime = time_curruent_ms();
        if (server_count_time == 0) {
            server_count_time = curtime;
            server_count = 0;
            server_count_size = 0;
        }
        if (is_client) {
            for (int i = 0; i < 10; i++) {
                char* text = hrpc_send(1, k_test_size);
                strcpy(text, "123321");
            }
        }
        if (curtime > server_count_time + 1000) {
            printf("%lld op/s, %lld kb/s\n", server_count * 1000 / (curtime - server_count_time), server_count_size * 1000 / (curtime - server_count_time) / 1024);
            server_count_time = curtime;
            server_count_size = 0;
            server_count = 0;
        }
    }
    return 0;
}
