// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

#ifndef BTREE_H
#define BTREE_H

#include <linux/kernel.h>

struct btree;

// btree_new returns a new B-tree.
//
// Param elsize is the size of each element in the tree. Every element that
// is inserted, deleted, or searched will be this size.
// Param max_items is the maximum number of items per node. Setting this to
// zero will default to 256.
// Param compare is a function that compares items in the tree. See the
// qsort stdlib function for an example of how this function works.
// Param udata is user-defined data that is passed to the compare callback
// and the item callbacks defined in btree_set_item_callbacks.
//
// The btree must be freed with btree_free().
struct btree *btree_new(size_t elsize, size_t max_items,
			int (*compare)(const void *a, const void *b,
				       void *udata),
			void *udata);

// btree_set_item_callbacks sets the item clone and free callbacks that will be
// called internally by the btree when items are inserted and removed.
//
// These callbacks are optional but may be needed by programs that require
// copy-on-write support by using the btree_clone function.
//
// The clone function should return true if the clone succeeded or false if the
// system is out of memory.
void btree_set_item_callbacks(struct btree *btree,
			      bool (*clone)(const void *item, void *into,
					    void *udata),
			      void (*free)(const void *item, void *udata));

// btree_free removes all items from the btree and frees any allocated memory.
void btree_free(struct btree *btree);

// btree_free removes all items from the btree.
void btree_clear(struct btree *btree);

// btree_oom returns true if the last write operation failed because the system
// has no more memory available.
//
// Functions that have the first param being a non-const btree receiver are
// candidates for possible out-of-memory conditions, such as btree_set,
// btree_delete, btree_load, etc.
bool btree_oom(const struct btree *btree);

// btree_height returns the height of the btree from root to leaf or zero if
// the btree is empty.
size_t btree_height(const struct btree *btree);

// btree_count returns the number of items in the btree.
size_t btree_count(const struct btree *btree);

// btree_clone makes an instant copy of the btree.
// This operation uses shadowing / copy-on-write.
struct btree *btree_clone(struct btree *btree);

// btree_set inserts or replaces an item in the btree. If an item is replaced
// then it is returned otherwise NULL is returned.
//
// If the system fails allocate the memory needed then NULL is returned
// and btree_oom() returns true.
const void *btree_set(struct btree *btree, const void *item);

// btree_delete removes an item from the B-tree and returns it.
//
// Returns NULL if item not found.
// This operation may trigger node copies if the btree was cloned using
// btree_clone.
// If the system fails allocate the memory needed then NULL is returned
// and btree_oom() returns true.
const void *btree_delete(struct btree *btree, const void *key);

// btree_load is the same as btree_set but is optimized for sequential bulk
// loading. It can be up to 10x faster than btree_set when the items are
// in exact order, but up to 25% slower when not in exact order.
//
// If the system fails allocate the memory needed then NULL is returned
// and btree_oom() returns true.
const void *btree_load(struct btree *btree, const void *item);

// btree_pop_min removes the first item in the btree and returns it.
//
// Returns NULL if btree is empty.
// This operation may trigger node copies if the btree was cloned using
// btree_clone.
// If the system fails allocate the memory needed then NULL is returned
// and btree_oom() returns true.
const void *btree_pop_min(struct btree *btree);

// btree_pop_min removes the last item in the btree and returns it.
//
// Returns NULL if btree is empty.
// This operation may trigger node copies if the btree was cloned using
// btree_clone.
// If the system fails allocate the memory needed then NULL is returned
// and btree_oom() returns true.
const void *btree_pop_max(struct btree *btree);

// btree_pop_min returns the first item in the btree or NULL if btree is empty.
const void *btree_min(const struct btree *btree);

// btree_pop_min returns the last item in the btree or NULL if btree is empty.
const void *btree_max(const struct btree *btree);

// btree_get returns the item based on the provided key.
//
// Returns NULL if item is not found.
const void *btree_get(const struct btree *btree, const void *key);

// btree_ascend scans the tree within the range [pivot, last].
//
// In other words btree_ascend iterates over all items that are
// greater-than-or-equal-to pivot in ascending order.
//
// Param pivot can be NULL, which means all items are iterated over.
// Param iter can return false to stop iteration early.
// Returns false if the iteration has been stopped early.
bool btree_ascend(const struct btree *btree, const void *pivot,
		  bool (*iter)(const void *item, void *udata), void *udata);

// btree_descend scans the tree within the range [pivot, first].

// In other words btree_descend() iterates over all items that are
// less-than-or-equal-to pivot in descending order.
//
// Param pivot can be NULL, which means all items are iterated over.
// Param iter can return false to stop iteration early.
// Returns false if the iteration has been stopped early.
bool btree_descend(const struct btree *btree, const void *pivot,
		   bool (*iter)(const void *item, void *udata), void *udata);

// btree_set_hint is the same as btree_set except that an optional "hint" can
// be provided which may make the operation quicker when done as a batch or
// in a userspace context.
const void *btree_set_hint(struct btree *btree, const void *item,
			   uint64_t *hint);

// btree_get_hint is the same as btree_get except that an optional "hint" can
// be provided which may make the operation quicker when done as a batch or
// in a userspace context.
const void *btree_get_hint(const struct btree *btree, const void *key,
			   uint64_t *hint);

// btree_delete_hint is the same as btree_delete except that an optional "hint"
// can be provided which may make the operation quicker when done as a batch or
// in a userspace context.
const void *btree_delete_hint(struct btree *btree, const void *key,
			      uint64_t *hint);

// btree_ascend_hint is the same as btree_ascend except that an optional
// "hint" can be provided which may make the operation quicker when done as a
// batch or in a userspace context.
bool btree_ascend_hint(const struct btree *btree, const void *pivot,
		       bool (*iter)(const void *item, void *udata), void *udata,
		       uint64_t *hint);

// btree_descend_hint is the same as btree_descend except that an optional
// "hint" can be provided which may make the operation quicker when done as a
// batch or in a userspace context.
bool btree_descend_hint(const struct btree *btree, const void *pivot,
			bool (*iter)(const void *item, void *udata),
			void *udata, uint64_t *hint);

#endif