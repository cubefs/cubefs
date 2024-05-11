#ifndef RDMA_H
#define RDMA_H

#include <stdint.h>
#include <stdio.h>
#include <signal.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/poll.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "connection.h"
#include "transfer_event.h"

#include "connection_event.h"
#include <sys/eventfd.h>


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

