#include "conn_pool.h"

conn_pool_t *new_conn_pool() {
    conn_pool_t *conn_pool = (conn_pool_t *)malloc(sizeof(conn_pool_t));
    memset(conn_pool, 0, sizeof(conn_pool_t));
    conn_pool->pool = new map<string, queue<conn_t>>;
    pthread_rwlock_init(&conn_pool->lock, NULL);
    return conn_pool;
}

int new_conn(const char *ip, int port) {
    int sock_fd = -1;
    if((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return sock_fd;
    }
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if(inet_pton(AF_INET, ip, &addr.sin_addr) < 0) {
		close(sock_fd);
        return -1;
    }
    if(connect(sock_fd, (struct sockaddr*)&addr, sizeof(addr))) {
		close(sock_fd);
        return -1;
    }

    struct timeval tv;
    tv.tv_sec = SEND_TIMEOUT_MS/1000;
    tv.tv_usec = (SEND_TIMEOUT_MS%1000)*1000;
    if(setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
        close(sock_fd);
        return -1;
    }
    tv.tv_sec = RCV_TIMEOUT_MS/1000;
    tv.tv_usec = (RCV_TIMEOUT_MS%1000)*1000;
    if(setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        close(sock_fd);
        return -1;
    }
    return sock_fd;
}

int get_conn(conn_pool_t *conn_pool, const char *ip, int port) {
    int sock_fd = -1;
    if(ip == NULL) {
		return sock_fd;
	}
    stringstream addr;
    addr << ip << ":" << port;
    pthread_rwlock_wrlock(&conn_pool->lock);
    auto it = conn_pool->pool->find(addr.str());
    if(it != conn_pool->pool->end()) {
        queue<conn_t> q = it->second;
        while(!q.empty()) {
            conn_t conn = q.front();
            q.pop();
            if(time(NULL) - conn.idle <= IDLE_CONN_TIMEOUT) {
                pthread_rwlock_unlock(&conn_pool->lock);
                return conn.sock_fd;
            } else {
                close(conn.sock_fd);
            }
        }
    }
    sock_fd = new_conn(ip, port);
    pthread_rwlock_unlock(&conn_pool->lock);
    return sock_fd;
}

void put_conn(conn_pool_t *conn_pool, const char *ip, int port, int sock_fd) {
    stringstream addr;
    addr << ip << ":" << port;
    conn_t conn;
    conn.sock_fd = sock_fd;
    conn.idle = time(NULL);
    pthread_rwlock_wrlock(&conn_pool->lock);
    (*conn_pool->pool)[addr.str()].push(conn);
    pthread_rwlock_unlock(&conn_pool->lock);
}
