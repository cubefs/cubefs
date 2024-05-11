#ifndef BUDDY_MEMORY_ALLOCATION_H
#define BUDDY_MEMORY_ALLOCATION_H

struct buddy;

struct buddy * buddy_new(int level);
void buddy_delete(struct buddy *);
int buddy_alloc(struct buddy *, int size);
void buddy_free(struct buddy *, int offset);
int buddy_size(struct buddy *, int offset);
void buddy_dump(struct buddy *);

#endif

