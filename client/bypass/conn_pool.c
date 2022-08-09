#include "conn_pool.h"
#include <sys/ioctl.h>

conn_pool_t *new_conn_pool() {
    conn_pool_t *conn_pool = (conn_pool_t *)malloc(sizeof(conn_pool_t));
    memset(conn_pool, 0, sizeof(conn_pool_t));
    conn_pool->pool = new map<string, queue<conn_t*>*>;
    pthread_rwlock_init(&conn_pool->lock, NULL);
    return conn_pool;
}

int check_conn_timeout(int sock_fd, int64_t timeout_ms) {
    int ret = 0;
    struct timeval tv;
    fd_set rset, wset;
    int error = 0;
    socklen_t len = 0;

    len = sizeof(error);
    tv.tv_sec = timeout_ms/1000;
    tv.tv_usec = (timeout_ms%1000)*1000;
    FD_ZERO(&rset);
    FD_ZERO(&wset);
    FD_SET(sock_fd, &rset);
    wset = rset;

    ret = select(sock_fd+1, &rset, &wset, NULL, &tv);
    if( ret <= 0) {
        return -1;
    }

    if (FD_ISSET(sock_fd, &rset) || FD_ISSET(sock_fd, &wset)) {
        ret = getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, &error, &len);
        if(error != 0 || ret < 0) {
            return -1;
        }
    }

    return 0;
}

int set_socket_non_block(int sock_fd, unsigned long enable) {
    return ioctl(sock_fd, FIONBIO, &enable);
}

int set_fd_timeout(int sock_fd, int64_t recv_timeout_ms, int64_t send_timeout_ms) {
    struct timeval tv;

    tv.tv_sec = send_timeout_ms/1000;
    tv.tv_usec = (send_timeout_ms%1000)*1000;
    if(setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
        return -1;
    }

    tv.tv_sec = recv_timeout_ms/1000;
    tv.tv_usec = (recv_timeout_ms%1000)*1000;
    if(setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        return -1;
    }

    return 0;
}

int new_conn(const char *ip, int port) {
    int sock_fd = -1;
    int ret = 0;
    struct sockaddr_in addr;

    if((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return sock_fd;
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if(inet_pton(AF_INET, ip, &addr.sin_addr) < 0) {
		close(sock_fd);
        return -1;
    }

    ret = set_socket_non_block(sock_fd, 1);
    if (ret < 0) {
        close(sock_fd);
        return -1;
    }

    ret = connect(sock_fd, (struct sockaddr*)&addr, sizeof(addr));
    if (ret < 0) {
        //error
        if (errno != EINPROGRESS) {
            close(sock_fd);
            return -1;
        }

        ret = check_conn_timeout(sock_fd, CONN_TIMEOUT_MS);
        if (ret < 0) {
            close(sock_fd);
            return -1;
        }
    }

    ret = set_socket_non_block(sock_fd, 0);
    if (ret < 0) {
        close(sock_fd);
        return -1;
    }

    ret = set_fd_timeout(sock_fd, RECV_TIMEOUT_MS, SEND_TIMEOUT_MS);
    if (ret < 0) {
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
        queue<conn_t*> *q = it->second;
        while(!q->empty()) {
            conn_t *conn = q->front();
            q->pop();
            if(time(NULL) - conn->idle <= IDLE_CONN_TIMEOUT) {
                sock_fd = conn->sock_fd;
                pthread_rwlock_unlock(&conn_pool->lock);
                delete conn;
                return sock_fd;
            } else {
                close(conn->sock_fd);
                delete conn;
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
    conn_t *conn = new conn_t();
    conn->sock_fd = sock_fd;
    conn->idle = time(NULL);
    pthread_rwlock_wrlock(&conn_pool->lock);
    auto it = (conn_pool->pool)->find(addr.str());
    if (it == conn_pool->pool->end()) {
        queue<conn_t*> *q = new queue<conn_t*>();
        q->push(conn);
        (*conn_pool->pool)[addr.str()] = q;
    } else {
        auto *q = it->second;
        q->push(conn);
    }
    pthread_rwlock_unlock(&conn_pool->lock);
}
