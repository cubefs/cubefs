#include "wait_group.h"
#include <stdio.h>

int wait_group_init(struct WaitGroup* wg) {
    if(pthread_mutex_init(&(wg->mutex), NULL)) {
        printf("Failed to initialize mutex\n");
        wg->wgInitialized = 0;
        return 1;
    }
    if(pthread_cond_init(&(wg->cond), NULL)) {
        printf("Failed to initialize cond\n");
        pthread_mutex_destroy(&wg->mutex);
        wg->wgInitialized = 0;
        return 1;
    }
    wg->count = 0;
    wg->wgInitialized = 1;
    return 0;
}

void wait_group_add(struct WaitGroup* wg, int delta) {
    pthread_mutex_lock(&wg->mutex);
    wg->count += delta;
    pthread_mutex_unlock(&wg->mutex);
}

void wait_group_done(struct WaitGroup* wg) {
    pthread_mutex_lock(&wg->mutex);
    wg->count--;
    if (wg->count == 0) {
        pthread_cond_broadcast(&wg->cond);
    }
    pthread_mutex_unlock(&wg->mutex);
}

void wait_group_wait(struct WaitGroup* wg) {
    pthread_mutex_lock(&wg->mutex);
    while (wg->count != 0) {
        pthread_cond_wait(&wg->cond, &wg->mutex);
    }
    pthread_mutex_unlock(&wg->mutex);
}

void wait_group_destroy(struct WaitGroup* wg) {
    pthread_mutex_destroy(&wg->mutex);
    pthread_cond_destroy(&wg->cond);
}