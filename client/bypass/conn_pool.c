#include "conn_pool.h"

static int new_conn(const char *ip, int port);

static void* check_alive_func(void* args);

conn_pool_t *new_conn_pool() {
    conn_pool_t *conn_pool = (conn_pool_t *)malloc(sizeof(conn_pool_t));
    memset(conn_pool, 0, sizeof(conn_pool_t));
    conn_pool->pool = new map<string, queue<conn_t>*>;
    pthread_rwlock_init(&conn_pool->lock, NULL);
    conn_pool->stop = 0;
    if (pthread_create(&conn_pool->alive_check_pthread, NULL, check_alive_func, conn_pool) < 0) {
        conn_pool->alive_check_pthread = -1;
    }
    return conn_pool;
}

void release_conn_pool(conn_pool_t *conn_pool) {
    conn_pool->stop = 1;
    if (conn_pool->alive_check_pthread != -1) {
        pthread_join(conn_pool->alive_check_pthread, NULL);
    }

    pthread_rwlock_wrlock(&conn_pool->lock);
    for(const auto &item : *conn_pool->pool) {
        queue<conn_t> *q = item.second;
        while(!q->empty()) {
            conn_t conn = q->front();
            q->pop();
            libc_close(conn.sock_fd);
        }
        delete q;
    }
    delete conn_pool->pool;
    pthread_rwlock_unlock(&conn_pool->lock);
    free(conn_pool);
}

static void* check_alive_func(void* args) {
    conn_pool_t *conn_pool = (conn_pool_t*) args;
    list<int> wait_close_fds;

    if (conn_pool == NULL) {
        return NULL;
    }

    while(1) {
        if (conn_pool->stop) {
            break;
        }
        wait_close_fds.clear();

        pthread_rwlock_wrlock(&conn_pool->lock);
        for(const auto &item : *conn_pool->pool) {
            queue<conn_t> *q = item.second;
            while(!q->empty()) {
               conn_t conn = q->front();
               if(time(NULL) - conn.idle <= IDLE_CONN_TIMEOUT)  {
                    break;
               }
               wait_close_fds.push_back(conn.sock_fd);
               q->pop();
            }
        }
        pthread_rwlock_unlock(&conn_pool->lock);

        for(list<int>::iterator it = wait_close_fds.begin(); it != wait_close_fds.end(); it++) {
            libc_close(*it);
        }

        sleep(ALIVE_CHECK_INTERVAL_S);
    }

    wait_close_fds.clear();
    return NULL;
}

int check_conn_timeout_poll(int sock_fd,  int64_t timeout_ms) {
    struct pollfd wfds[1];
    int ret = 0;
    int error = 0;
    socklen_t len = sizeof(error);

    wfds[0].fd = sock_fd;
    wfds[0].events = POLLOUT;
    wfds[0].revents = 0;
    while (1) {
        ret = poll(wfds, 1, timeout_ms);
        if (ret < 0 && error == EINTR) {
            continue;
        }

        break;
    }

    if (ret <= 0) {
        return -1;
    }

    if (wfds[0].revents & POLLOUT) {
        ret = getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, &error, &len);
        if(error != 0 || ret < 0) {
            return -1;
        }
        return 0;
    }

    return -1;
}

int check_conn_timeout(int sock_fd, int64_t timeout_ms) {
    int ret;
    struct timeval tv;
    tv.tv_sec = timeout_ms/1000;
    tv.tv_usec = (timeout_ms%1000)*1000;
    fd_set *wset = (fd_set *)malloc(sizeof(fd_set));
    if(wset == NULL) {
        return -1;
    }

    FD_ZERO(wset);
    FD_SET(sock_fd, wset);
    ret = select(sock_fd+1, NULL, wset, NULL, &tv);
    if(ret <= 0) {
        free(wset);
        return -1;
    }

    if(FD_ISSET(sock_fd, wset)) {
        int error = 0;
        socklen_t len = sizeof(error);
        ret = getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, &error, &len);
        if(error != 0 || ret < 0) {
            free(wset);
            return -1;
        }
    }

    free(wset);
    return 0;
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

static int new_conn(const char *ip, int port) {
    int sock_fd = -1;
    int ret = 0;
    struct sockaddr_in addr;

    if((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return sock_fd;
    }

    int flag = 1;
    if(setsockopt(sock_fd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flag, sizeof(flag)) < 0 ||
       setsockopt(sock_fd, SOL_TCP, TCP_NODELAY, (void *)&flag, sizeof(flag)) < 0) {
        libc_close(sock_fd);
        return -1;
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if(inet_pton(AF_INET, ip, &addr.sin_addr) < 0) {
		libc_close(sock_fd);
        return -1;
    }

    int flags = fcntl(sock_fd, F_GETFL, 0);
    fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);

    ret = connect(sock_fd, (struct sockaddr*)&addr, sizeof(addr));
    if (ret < 0) {
        //error
        if (errno != EINPROGRESS) {
            libc_close(sock_fd);
            return -1;
        }

        ret = check_conn_timeout_poll(sock_fd, CONN_TIMEOUT_MS);
        if (ret < 0) {
            libc_close(sock_fd);
            return -1;
        }
    }

    fcntl(sock_fd, F_SETFL, flags);

    ret = set_fd_timeout(sock_fd, RECV_TIMEOUT_MS, SEND_TIMEOUT_MS);
    if (ret < 0) {
        libc_close(sock_fd);
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
        queue<conn_t> *q = it->second;
        while(!q->empty()) {
            conn_t conn = q->front();
            q->pop();
            if(time(NULL) - conn.idle <= IDLE_CONN_TIMEOUT) {
                sock_fd = conn.sock_fd;
                pthread_rwlock_unlock(&conn_pool->lock);
                return conn.sock_fd;
            } else {
                libc_close(conn.sock_fd);
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
    auto it = (conn_pool->pool)->find(addr.str());
    if (it == conn_pool->pool->end()) {
        queue<conn_t> *q = new queue<conn_t>();
        q->push(conn);
        (*conn_pool->pool)[addr.str()] = q;
    } else {
        auto *q = it->second;
        q->push(conn);
    }
    pthread_rwlock_unlock(&conn_pool->lock);
}
