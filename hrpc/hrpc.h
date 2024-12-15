#pragma once

#include <netinet/in.h>

// 返回 sockfd
int hrpc_init(const char* dbpath, int nid, int bind_port, struct sockaddr_in (*get_addr)(int nid));
// 客户端主动touch连接服务端, 拉起心跳
int hrpc_touch_connect(int nid);
// 服务端判断是否已经与指定nid建立连接
int hrpc_is_connected(int nid);
// 申请一个完整消息缓冲区
void* hrpc_send(int nid, int size);
// 执行一次交换
int hrpc_once(void (*on_message)(int nid, void* message, unsigned int size));
// 期望在这个超时时间到期后继续下一次hrpc_once. 如果有入包(selector监控到)也需要立即执行
int hrpc_once_timeout();