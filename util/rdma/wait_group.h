#ifndef WAIT_GROUP_H
#define WAIT_GROUP_H

#include <pthread.h>

struct WaitGroup {
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int count;
    int wgInitialized;
};

int wait_group_init(struct WaitGroup* wg);

void wait_group_add(struct WaitGroup* wg, int delta);

void wait_group_done(struct WaitGroup* wg);

void wait_group_wait(struct WaitGroup* wg);

void wait_group_destroy(struct WaitGroup* wg);

#endif