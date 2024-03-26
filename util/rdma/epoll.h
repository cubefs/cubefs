
#ifndef EPOLL_H_
#define EPOLL_H_

#include<stdio.h>
#include<unistd.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<sys/epoll.h>
#include<fcntl.h>
#include<stdlib.h>

#include "rdma_proto.h"

typedef void (*EventCallBack)(void *ctx);

static int rdma_transferEvent_thread(Connection *conn, EventCallBack cb) {
    pthread_create(&conn->transferThread, NULL, cb, conn);
    return 1;
}

static int rdma_connectEvent_thread(int type, void *ctx, EventCallBack cb, struct ConnectionEvent *conn_ev) {
    if (type == 1) {
        struct RdmaListener* server = (struct RdmaListener*)ctx;
        pthread_create(&server->connectThread, NULL, cb, conn_ev);
    } else {
        struct RdmaContext* client = (struct RdmaContext*)ctx;
        pthread_create(&client->connectThread, NULL, cb, conn_ev);
    }
    return 1;
}

static void DelTransferEvent(Connection *conn) {
    conn->close = 1;
    return 1;
}

static void DelConnectEvent(int type, void *ctx) {
    if(type == 1) {
        struct RdmaListener* server = (struct RdmaListener*)ctx;
        server->conn_ev->close = 1;
    } else {
        struct RdmaContext* client = (struct RdmaContext*)ctx;
        client->conn_ev->close = 1;
    }

    return 1;
}

#endif