#ifndef RDMA_H_
#define RDMA_H_

#include <stdint.h>
#include <stdio.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <signal.h>
#include <fcntl.h>
#include <poll.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/poll.h>
#include <sys/stat.h>

#include <sys/socket.h>
#include <netinet/in.h>
//#include <arpa/inet.h>

//#include "epoll.h"
#include "connection.h"
#include "transfer_event.h"
//#include "connection_event.h"
//#include "client.c"
//#include "server.c"
//#include "memory_pool.h"
//#include "queue.h"
//#include "hashmap_entry.h"
#include <sys/eventfd.h>
#include <pthread.h>

extern void PrintCallback(char* str);

//static FILE *file;
static char buffer[100];

static inline int open_event_fd() {
   return eventfd(0, EFD_SEMAPHORE);
}

static inline int wait_event(int fd) {
	uint64_t value = 0;
	return read(fd, &value, 8);
}

static inline int notify_event(int fd, int flag) {
	if (flag == 0) {
		uint64_t value = 1;
		return write(fd, &value, 8);
	} else {
		close(fd);
	}
	return 0;
}


//extern void ServerConnectionCallback(int, Connection*);
//extern void ClientConnectionCallback(int, Connection*);
#define TEST_NZ(x) if ( (x)) fprintf(stderr, "%s:%d %d, %s\n", __FILE__, __LINE__, errno, strerror(errno));
#define TEST_Z(x)  if (!(x)) fprintf(stderr, "%s:%d %d, %s\n", __FILE__, __LINE__, errno, strerror(errno));

#define TEST_NZ_(x) if ((x)) { \
    fprintf(stderr, "%s:%d %d, %s\n", __FILE__, __LINE__, errno, strerror(errno)); \
    return NULL; \
}

#define TEST_Z_(x) if (!(x)) { \
    fprintf(stderr, "%s:%d %d, %s\n", __FILE__, __LINE__, errno, strerror(errno)); \
    return NULL; \
}

#endif

