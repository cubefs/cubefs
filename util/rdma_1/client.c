#include <stdio.h>
#include <sys/socket.h>
#include <netdb.h>

#include <unistd.h>

#include "rdma.h"
//#include "connection.h"
//#include "connection_event.h"


static inline int open_event_fd();
static inline int wait_event(int);
static inline int notify_event(int, int); 


static int OnClientConnPreConnect(struct rdma_cm_id *id, void* ctx) {
    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)ctx;
    struct RdmaContext* client = (struct RdmaContext*)conn_ev->ctx;
    printf("client=%p, %s \n", client, __FUNCTION__);
    sprintf(buffer,"client=%p, %s \n", client, __FUNCTION__);
    PrintCallback(buffer);

    if(!client->isReConnect) {
        Connection* conn = AllocConnection(id, conn_ev, 2);
        if(conn == NULL) {
            //close connection
            //client->conn = conn;
            //notify_event(client->cFd, 0);
            //rdma_disconnect(id);
            EpollDelConnEvent(client->listen_id->channel->fd);
            rdma_destroy_id(id);
            client->listen_id = NULL;
            return C_ERR;
        }
        
        //client->conn = conn;
        conn->state = CONN_STATE_CONNECTING;
        conn->csContext = client;
        id->context = conn;
        
        //EpollAddSendAndRecvEvent(conn->comp_channel->fd, conn);
        return C_OK;
    } else {
        if(!UpdateConnection(client->conn)) {
            printf("update connection failed\n");
            EpollDelConnEvent(client->listen_id->channel->fd);
            rdma_destroy_id(id);
            client->conn->cm_id = NULL;
            client->listen_id = NULL;
            return C_ERR;
        }

        client->conn->state = CONN_STATE_CONNECTING;
        id->context = client->conn;
        return C_OK;
    }
}

static int OnClientConnConnected(struct rdma_cm_id *id, void* ctx) {
    struct RdmaContext* client = (struct RdmaContext*)ctx;
    printf("client=%p, %s \n", client, __FUNCTION__);
    sprintf(buffer,"client=%p, %s \n", client, __FUNCTION__);
    PrintCallback(buffer);

    Connection* conn = (Connection*)id->context;
    //conn->state = CONN_STATE_CONNECTED;

    //EpollAddSendAndRecvEvent(conn->comp_channel->fd, conn);
    

    if(!client->isReConnect) {
        client->conn = conn;
    }
    notify_event(client->cFd, 0);

    id->context = conn;
    return C_OK;
}

static int OnClientConnRejected(struct rdma_cm_id *id, void* ctx) {
    struct RdmaContext* client = (struct RdmaContext*)ctx;
    printf("client=%p, %s \n", client, __FUNCTION__);
    sprintf(buffer,"client=%p, %s \n", client, __FUNCTION__);
    PrintCallback(buffer);

    Connection* conn = (Connection*)id->context;

    //EpollDelConnEvent(conn->comp_channel->fd);
    EpollDelConnEvent(client->listen_id->channel->fd);

    if (conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            printf("Failed to destroy qp: %s\n", strerror(errno));
            sprintf(buffer,"Failed to destroy qp: %s\n", strerror(errno));
            PrintCallback(buffer);
            //printf("Failed to destroy qp cleanly\n");
            // we continue anyways;
        }
    }
    if (conn->cq) {
        int ret = ibv_destroy_cq(conn->cq);
        if(ret) {
            printf("Failed to destroy cq: %s\n", strerror(errno));
            sprintf(buffer,"Failed to destroy cq: %s\n", strerror(errno));
            PrintCallback(buffer);
            //printf("Failed to destroy cq cleanly\n");
            // we continue anyways;
        }
        conn->cq = NULL;
    }


    if(conn->cm_id) {
        rdma_destroy_id(conn->cm_id);
    }

    conn->cm_id = NULL;
    ((struct RdmaContext*)(conn->csContext))->listen_id = NULL;

    if(conn->cFd > 0) {
        notify_event(conn->cFd,0);
        close(conn->cFd);
        conn->cFd = -1;
    }


    //TODO before clear,need to determind all header or response is back to freeList
    if(conn->freeList) {
        ClearQueue(conn->freeList);
        printf("header freeList size: %d\n",GetSize(conn->freeList));
        sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        PrintCallback(buffer);
    }
    //conn->state = CONN_STATE_CLOSED;

    client->conn = conn;
    return C_OK;
}

static int OnClientConnDisconnected(struct rdma_cm_id *id, void* ctx) {//TODO

    struct RdmaContext* client = (struct RdmaContext*)ctx;
    printf("client=%p, %s \n", client, __FUNCTION__);
    sprintf(buffer,"client=%p, %s \n", client, __FUNCTION__);
    PrintCallback(buffer);
    //struct rdma_cm_id *id = event->id;

    Connection* conn = (Connection*)id->context;
    pthread_spin_lock(&conn->lock);
    //conn->state = CONN_STATE_CLOSING;
    conn->state = CONN_STATE_CLOSED;
    pthread_spin_unlock(&conn->lock);
    
    DisConnectCallback(conn->connContext);

    //EpollDelConnEvent(conn->comp_channel->fd);
    wait_group_wait(&(conn->wg));
            
    //EpollDelConnEvent(conn->comp_channel->fd);// do not transfer data 
    //printf("111\n");
    EpollDelConnEvent(client->listen_id->channel->fd);
    
    /*
    if (conn->cq) {
        ibv_destroy_cq(conn->cq);
    }
    if (conn->comp_channel) {
        ibv_destroy_comp_channel(conn->comp_channel);
    }
    if (conn->pd) {
        //ibv_dealloc_pd(conn->pd);
        conn->pd = NULL;
    }

    if (client->listen_id->qp) {
        ibv_destroy_qp(client->listen_id->qp);
    }

    rdmaDestroyIoBuf(conn);
    */
    //printf("222\n");

    
    //usleep(100);


    

    //printf("333\n");
    if (conn->cm_id->qp) {
        if(ibv_destroy_qp(conn->cm_id->qp)) {
            printf("Failed to destroy qp: %s\n", strerror(errno));
            sprintf(buffer,"Failed to destroy qp: %s\n", strerror(errno));
            PrintCallback(buffer);
            //printf("Failed to destroy qp cleanly\n");
            // we continue anyways;
        }
    }
    if (conn->cq) {
        if(ibv_destroy_cq(conn->cq)) {
            printf("Failed to destroy cq: %s\n", strerror(errno));
            sprintf(buffer,"Failed to destroy cq: %s\n", strerror(errno));
            PrintCallback(buffer);
            //printf("Failed to destroy cq cleanly\n");
            // we continue anyways;
        }
        conn->cq = NULL;
    }
    //if (conn->comp_channel) {
    //    if(ibv_destroy_comp_channel(conn->comp_channel)) {
    //        printf("Failed to destroy comp channel: %s\n", strerror(errno));
    //    }
    //    conn->comp_channel = NULL;
    //}



    //printf("444\n");
    if(conn->cm_id) {
        rdma_destroy_id(conn->cm_id);
    }

    conn->cm_id = NULL;
    ((struct RdmaContext*)(conn->csContext))->listen_id = NULL;

    //TODO before clear,need to determind all header or response is back to freeList
    //printf("555\n");
    if(conn->freeList) {
        ClearQueue(conn->freeList);
        printf("header freeList size: %d\n",GetSize(conn->freeList));
        sprintf(buffer,"header freeList size: %d\n",GetSize(conn->freeList));
        PrintCallback(buffer);
    }

    //DestroyQueue(conn->freeList);


    //rdma_destroy_event_channel(client->ec);
    //rdma_destroy_id(client->listen_id); //TODO

    //pthread_spin_lock(&conn->lock);
    //conn->state = CONN_STATE_CLOSED;
    //pthread_spin_unlock(&conn->lock);
 

    //printf("666\n");
    notify_event(conn->cFd, 0);
    //printf("777\n");
    //free(client->conn_ev);
    //client->conn_ev = NULL;
    //free(client);
    //client = NULL;

    //notify_event(((struct RdmaContext*)(conn->csContext))->closeFd,0);
    return C_OK;
}


static Connection* getClientConn(struct RdmaContext *client) {
    
    //for(;client->conn == NULL  || client->conn->state != CONN_STATE_CONNECTED;) {//阻塞实现方式?
        //printf("wait ClientConn\n");
        //printf("wait %d\n",client->conn);
        //usleep(10 * 1000);
    //}
    if(wait_event(client->cFd) <= 0) {//TODO error handler
		return NULL;
	}

    return client->conn;
}

static struct RdmaContext* Connect(const char* ip, const char* port, char* remoteAddr) {//, MemoryPool* pool, ObjectPool* headerPool, ObjectPool* responsePool, struct ibv_pd* pd, struct ibv_mr* mr
    struct addrinfo *addr;
    struct rdma_cm_id *conn = NULL;
    struct rdma_event_channel *ec = NULL;
    struct rdma_conn_param cm_params;

    printf("ip=%s, port=%s\n", ip, port);
    sprintf(buffer,"ip=%s, port=%s\n", ip, port);
    PrintCallback(buffer);
    TEST_NZ_(getaddrinfo(ip, port, NULL, &addr));

    TEST_Z_(ec = rdma_create_event_channel());
    TEST_NZ_(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
    TEST_NZ_(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));
    freeaddrinfo(addr);
    struct RdmaContext* client = (struct RdmaContext*)malloc(sizeof(struct RdmaContext));

    client->listen_id = conn;
    printf("client listen id %d\n",client->listen_id);
    sprintf(buffer,"client listen id %d\n",client->listen_id);
    PrintCallback(buffer);
    client->ec = ec;
    //client->remoteAddr = remoteAddr;
    client->ip = ip;
    client->port = port;
    client->state = 0;
    client->cFd = open_event_fd();
    //client->closeFd = open_event_fd();
    client->conn = NULL;
    client->isReConnect = false;
    struct ConnectionEvent* conn_ev = (struct ConnectionEvent*)malloc(sizeof(struct ConnectionEvent));
    conn_ev->cm_id = conn;
    conn_ev->ctx = client;
    //conn_ev->pool = pool;
    //conn_ev->header_pool = headerPool;
    //conn_ev->response_pool = responsePool;

    //conn_ev->pd = pd;
    //conn_ev->mr = mr;
    conn_ev->preconnect_callback = OnClientConnPreConnect;
    conn_ev->connected_callback = OnClientConnConnected;
    conn_ev->disconnected_callback = OnClientConnDisconnected;
    conn_ev->rejected_callback = OnClientConnRejected;

    client->conn_ev = conn_ev;


    EpollAddConnectEvent(client->listen_id->channel->fd, conn_ev);

    printf("start client %p \n", client);
    sprintf(buffer,"start client %p \n", client);
    PrintCallback(buffer);
    return client;
}


static int CloseClient(struct RdmaContext* client) {
    //for (;client->conn->state == CONN_STATE_CLOSED;) {
    //    usleep(10 * 1000);
    //}
    //free(client->conn);
    if(client->conn != NULL) {
        Connection *conn = client->conn;

        //if (conn->cq) {
        //    ibv_destroy_cq(conn->cq);
        //}

        if (conn->comp_channel) {
            if(ibv_destroy_comp_channel(conn->comp_channel) != 0) {
                printf("Failed to destroy comp channel: %s\n", strerror(errno));
                sprintf(buffer,"Failed to destroy comp channel: %s\n", strerror(errno));
                PrintCallback(buffer);
            }
            conn->comp_channel = NULL;
        }

        if (conn->pd) {
            //ibv_dealloc_pd(conn->pd);
            conn->pd = NULL;
        }

        //if (client->listen_id->qp) {
        //    ibv_destroy_qp(client->listen_id->qp);
        //}

        rdmaDestroyIoBuf(conn);

        if(conn->freeList) {
            DestroyQueue(conn->freeList);
        }
        /*
        if(conn->cFd > 0) {
            notify_event(conn->cFd,0);
            close(conn->cFd);
            conn->cFd = -1;
        }
        
        if(conn->initFd > 0) {
            notify_event(conn->initFd,0);
            close(conn->initFd);
            conn->initFd = -1;
        }
        */

        conn->connContext = NULL;
        ((struct RdmaContext*)conn->csContext)->conn = NULL;
        conn->csContext = NULL;

        if(conn->wg.wgInitialized == 1) {
            wait_group_destroy(&conn->wg);
        }
        if(conn->lockInitialized == 1) {
            pthread_spin_destroy(&conn->lock);
        }

        //rdma_destroy_id(conn->cm_id);
        free(conn);
    }

    if(client->cFd > 0) {
        notify_event(client->cFd,0);
        close(client->cFd);
        client->cFd = -1;
    }


    //client->conn = NULL;

    //EpollDelConnEvent(client->listen_id->channel->fd);
    //rdma_destroy_id(client->listen_id);
    if(client->ec) {
        rdma_destroy_event_channel(client->ec);    
    }

    free(client->conn_ev);
    free(client);
    return C_OK;
}
