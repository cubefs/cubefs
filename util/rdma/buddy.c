#include "buddy.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>

#define NODE_UNUSED 0
#define NODE_USED 1
#define NODE_SPLIT 2
#define NODE_FULL 3

struct buddy {
    pthread_mutex_t mutex;
	int level;
	uint8_t tree[1];
};

struct buddy *
buddy_new(int level) {
	int size = 1 << level;
	struct buddy * self = malloc(sizeof(struct buddy) + sizeof(uint8_t) * (size * 2 - 2));
	self->level = level;
	memset(self->tree , NODE_UNUSED , size*2-1);
	pthread_mutex_init(&self->mutex, NULL);
	return self;
}

void
buddy_delete(struct buddy * self) {
	free(self);
}

static inline int
is_pow_of_2(uint32_t x) {
	return !(x & (x-1));
}

static inline uint32_t
next_pow_of_2(uint32_t x) {
	if ( is_pow_of_2(x) )
		return x;
	x |= x>>1;
	x |= x>>2;
	x |= x>>4;
	x |= x>>8;
	x |= x>>16;
	return x+1;
}

static inline int
_index_offset(int index, int level, int max_level) {
	return ((index + 1) - (1 << level)) << (max_level - level);
}

static void
_mark_parent(struct buddy * self, int index) {
	for (;;) {
		int buddy = index - 1 + (index & 1) * 2;
		if (buddy > 0 && (self->tree[buddy] == NODE_USED ||	self->tree[buddy] == NODE_FULL)) {
			index = (index + 1) / 2 - 1;
			self->tree[index] = NODE_FULL;
		} else {
			return;
		}
	}
}

int
buddy_alloc(struct buddy * self , int s) {
    pthread_mutex_lock(&self->mutex);
	int size;
	if (s==0) {
		size = 1;
	} else {
		size = (int)next_pow_of_2(s);
	}
	int length = 1 << self->level;

	if (size > length) {
	    pthread_mutex_unlock(&self->mutex);
	    return -1;
	}


	int index = 0;
	int level = 0;

	while (index >= 0) {
		if (size == length) {
			if (self->tree[index] == NODE_UNUSED) {
				self->tree[index] = NODE_USED;
				_mark_parent(self, index);
				pthread_mutex_unlock(&self->mutex);
				return _index_offset(index, level, self->level);
			}
		} else {
			// size < length
			switch (self->tree[index]) {
			case NODE_USED:
			case NODE_FULL:
				break;
			case NODE_UNUSED:
				// split first
				self->tree[index] = NODE_SPLIT;
				self->tree[index*2+1] = NODE_UNUSED;
				self->tree[index*2+2] = NODE_UNUSED;
			default:
				index = index * 2 + 1;
				length /= 2;
				level++;
				continue;
			}
		}
		if (index & 1) {
			++index;
			continue;
		}
		for (;;) {
			level--;
			length *= 2;
			index = (index+1)/2 -1;
			if (index < 0) {
			    pthread_mutex_unlock(&self->mutex);
			    return -1;
			}

			if (index & 1) {
				++index;
				break;
			}
		}
	}
    pthread_mutex_unlock(&self->mutex);
	return -1;
}

static void
_combine(struct buddy * self, int index) {
	for (;;) {
		int buddy = index - 1 + (index & 1) * 2;
		if (buddy < 0 || self->tree[buddy] != NODE_UNUSED) {
			self->tree[index] = NODE_UNUSED;
			while (((index = (index + 1) / 2 - 1) >= 0) &&  self->tree[index] == NODE_FULL){
				self->tree[index] = NODE_SPLIT;
			}
			return;
		}
		index = (index + 1) / 2 - 1;
	}
}

void
buddy_free(struct buddy * self, int offset) {
    pthread_mutex_lock(&self->mutex);
	assert( offset < (1<< self->level));
	int left = 0;
	int length = 1 << self->level;
	int index = 0;

	for (;;) {
		switch (self->tree[index]) {
		case NODE_USED:
			assert(offset == left);
			_combine(self, index);
			pthread_mutex_unlock(&self->mutex);
			return;
		case NODE_UNUSED:
			assert(0);
			pthread_mutex_unlock(&self->mutex);
			return;
		default:
			length /= 2;
			if (offset < left + length) {
				index = index * 2 + 1;
			} else {
				left += length;
				index = index * 2 + 2;
			}
			break;
		}
	}
	pthread_mutex_unlock(&self->mutex);
}

int
buddy_size(struct buddy * self, int offset) {
    pthread_mutex_lock(&self->mutex);
	assert( offset < (1<< self->level));
	int left = 0;
	int length = 1 << self->level;
	int index = 0;

	for (;;) {
		switch (self->tree[index]) {
		case NODE_USED:
			assert(offset == left);
			pthread_mutex_unlock(&self->mutex);
			return length;
		case NODE_UNUSED:
			assert(0);
			pthread_mutex_unlock(&self->mutex);
			return length;
		default:
			length /= 2;
			if (offset < left + length) {
				index = index * 2 + 1;
			} else {
				left += length;
				index = index * 2 + 2;
			}
			break;
		}
	}
	pthread_mutex_unlock(&self->mutex);
}

static void
_dump(struct buddy * self, int index , int level) {
	switch (self->tree[index]) {
	case NODE_UNUSED:
		printf("(%d:%d)", _index_offset(index, level, self->level) , 1 << (self->level - level));
		break;
	case NODE_USED:
		printf("[%d:%d]", _index_offset(index, level, self->level) , 1 << (self->level - level));
		break;
	case NODE_FULL:
		printf("{");
		_dump(self, index * 2 + 1 , level+1);
		_dump(self, index * 2 + 2 , level+1);
		printf("}");
		break;
	default:
		printf("(");
		_dump(self, index * 2 + 1 , level+1);
		_dump(self, index * 2 + 2 , level+1);
		printf(")");
		break;
	}
}

void
buddy_dump(struct buddy * self) {
	_dump(self, 0 , 0);
	printf("\n");
}
