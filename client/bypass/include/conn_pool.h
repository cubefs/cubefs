#ifndef CONN_POOL_H
#define CONN_POOL_H

#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <iostream>
#include <map>
#include <queue>
#include <string>
#include <sstream>

using namespace std;

#define IDLE_CONN_TIMEOUT 30
#define SEND_TIMEOUT_MS 1000
#define RCV_TIMEOUT_MS 1000

typedef struct {
    int sock_fd;
    time_t idle;
} conn_t;

typedef struct {
    map<string, queue<conn_t>> *pool;
    pthread_rwlock_t lock;
} conn_pool_t;

conn_pool_t *new_conn_pool();
int get_conn(conn_pool_t *conn_pool, const char *ip, int port);
void put_conn(conn_pool_t *conn_pool, const char *ip, int port, int sock_fd);

#endif