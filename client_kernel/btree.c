// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

#include <linux/slab.h>

#include "btree.h"

enum delact {
	DELKEY,
	POPFRONT,
	POPBACK,
	POPMAX,
};

static size_t align_size(size_t size)
{
	if (size & (sizeof(uintptr_t) - 1)) {
		size += sizeof(uintptr_t) - (size & (sizeof(uintptr_t) - 1));
	}
	return size;
}

struct node {
	atomic_t rc;
	bool leaf;
	size_t num_items : 16;
	char *items;
	struct node *children[];
};

struct btree {
	int (*compare)(const void *a, const void *b, void *udata);
	bool (*item_clone)(const void *item, void *into, void *udata);
	void (*item_free)(const void *item, void *udata);
	void *udata; // user data
	struct node *root; // root node or NULL if empty tree
	size_t count; // number of items in tree
	size_t height; // height of tree from root to leaf
	size_t max_items; // max items allowed per node before needing split
	size_t min_items; // min items allowed per node before needing join
	size_t elsize; // size of user item
	bool oom; // last write operation failed due to no memory
	size_t spare_elsize; // size of each spare element. This is aligned
	char spare_data[]; // spare element spaces for various operations
};

static void *spare_at(struct btree *btree, size_t index)
{
	return btree->spare_data + btree->spare_elsize * index;
}

#define NUM_SPARES 4
#define SPARE_RETURN spare_at((void *)btree, 0) // returned values
#define SPARE_NODE_COPY spare_at((void *)btree, 1) // item_clone in node_copy
#define SPARE_POP spare_at((void *)btree, 2) // btree_delete pop
#define SPARE_ITEM_CLONE spare_at((void *)btree, 3) // cloned inputs

static void *get_item_at(struct btree *btree, struct node *node, size_t index)
{
	return node->items + btree->elsize * index;
}

static void set_item_at(struct btree *btree, struct node *node, size_t index,
			const void *item)
{
	void *slot = get_item_at(btree, node, index);
	memcpy(slot, item, btree->elsize);
}

static void swap_item_at(struct btree *btree, struct node *node, size_t index,
			 const void *item, void *into)
{
	void *ptr = get_item_at(btree, node, index);
	memcpy(into, ptr, btree->elsize);
	memcpy(ptr, item, btree->elsize);
}

static void copy_item_into(struct btree *btree, struct node *node, size_t index,
			   void *into)
{
	memcpy(into, get_item_at(btree, node, index), btree->elsize);
}

static void node_shift_right(struct btree *btree, struct node *node,
			     size_t index)
{
	size_t num_items_to_shift = node->num_items - index;
	memmove(node->items + btree->elsize * (index + 1),
		node->items + btree->elsize * index,
		num_items_to_shift * btree->elsize);
	if (!node->leaf) {
		memmove(&node->children[index + 1], &node->children[index],
			(num_items_to_shift + 1) * sizeof(struct node *));
	}
	node->num_items++;
}

static void node_shift_left(struct btree *btree, struct node *node,
			    size_t index, bool for_merge)
{
	size_t num_items_to_shift = node->num_items - index - 1;
	memmove(node->items + btree->elsize * index,
		node->items + btree->elsize * (index + 1),
		num_items_to_shift * btree->elsize);
	if (!node->leaf) {
		if (for_merge) {
			index++;
		}
		memmove(&node->children[index], &node->children[index + 1],
			(num_items_to_shift + 1) * sizeof(struct node *));
	}
	node->num_items--;
}

static void copy_item(struct btree *btree, struct node *node_a, size_t index_a,
		      struct node *node_b, size_t index_b)
{
	memcpy(get_item_at(btree, node_a, index_a),
	       get_item_at(btree, node_b, index_b), btree->elsize);
}

static void node_join(struct btree *btree, struct node *left,
		      struct node *right)
{
	memcpy(left->items + btree->elsize * left->num_items, right->items,
	       right->num_items * btree->elsize);
	if (!left->leaf) {
		memcpy(&left->children[left->num_items], &right->children[0],
		       (right->num_items + 1) * sizeof(struct node *));
	}
	left->num_items += right->num_items;
}

static int btcompare(const struct btree *btree, const void *a, const void *b)
{
	return btree->compare(a, b, btree->udata);
}

static size_t node_bsearch(const struct btree *btree, struct node *node,
			   const void *key, bool *found)
{
	size_t i = 0;
	size_t n = node->num_items;
	while (i < n) {
		size_t j = (i + n) >> 1;
		void *item = get_item_at((void *)btree, node, j);
		int cmp = btcompare(btree, key, item);
		if (cmp == 0) {
			*found = true;
			return j;
		} else if (cmp < 0) {
			n = j;
		} else {
			i = j + 1;
		}
	}
	*found = false;
	return i;
}

static int node_bsearch_hint(const struct btree *btree, struct node *node,
			     const void *key, bool *found, uint64_t *hint,
			     int depth)
{
	int low = 0;
	int high = node->num_items - 1;
	int index;

	if (hint && depth < 8) {
		size_t index = (size_t)((uint8_t *)hint)[depth];
		if (index > 0) {
			void *item;
			int cmp;

			if (index > (size_t)(node->num_items - 1)) {
				index = node->num_items - 1;
			}
			item = get_item_at((void *)btree, node, (size_t)index);
			cmp = btcompare(btree, key, item);
			if (cmp == 0) {
				*found = true;
				return index;
			}
			if (cmp > 0) {
				low = index + 1;
			} else {
				high = index - 1;
			}
		}
	}
	while (low <= high) {
		int mid = (low + high) / 2;
		void *item = get_item_at((void *)btree, node, (size_t)mid);
		int cmp = btcompare(btree, key, item);
		if (cmp == 0) {
			*found = true;
			index = mid;
			goto done;
		}
		if (cmp < 0) {
			high = mid - 1;
		} else {
			low = mid + 1;
		}
	}
	*found = false;
	index = low;
done:
	if (hint && depth < 8) {
		((uint8_t *)hint)[depth] = (uint8_t)index;
	}
	return index;
}

static size_t btree_memsize(size_t elsize, size_t *spare_elsize)
{
	size_t size = align_size(sizeof(struct btree));
	size_t elsize_aligned = align_size(elsize);
	size += elsize_aligned * NUM_SPARES;
	if (spare_elsize)
		*spare_elsize = elsize_aligned;
	return size;
}

struct btree *btree_new(size_t elsize, size_t max_items,
			int (*compare)(const void *a, const void *b,
				       void *udata),
			void *udata)
{
	// normalize max_items
	size_t spare_elsize;
	size_t size = btree_memsize(elsize, &spare_elsize);
	struct btree *btree = kmalloc(size, GFP_NOFS);
	size_t deg;

	if (!btree)
		return NULL;
	memset(btree, 0, size);
	deg = max_items / 2;
	deg = deg == 0 ? 128 : deg == 1 ? 2 : deg;
	btree->max_items =
		deg * 2 - 1; // max items per node. max children is +1
	if (btree->max_items > 2045) {
		// there must be a reasonable limit.
		btree->max_items = 2045;
	}
	btree->min_items = btree->max_items / 2;
	btree->compare = compare;
	btree->elsize = elsize;
	btree->udata = udata;
	btree->spare_elsize = spare_elsize;
	return btree;
}

static size_t node_size(struct btree *btree, bool leaf, size_t *items_offset)
{
	size_t size = sizeof(struct node);
	if (!leaf) {
		// add children as flexible array
		size += sizeof(struct node *) * (btree->max_items + 1);
	}
	if (items_offset)
		*items_offset = size;
	size += btree->elsize * btree->max_items;
	size = align_size(size);
	return size;
}

static struct node *node_new(struct btree *btree, bool leaf)
{
	size_t items_offset;
	size_t size = node_size(btree, leaf, &items_offset);
	struct node *node = kmalloc(size, GFP_NOFS);

	if (!node)
		return NULL;
	memset(node, 0, size);
	node->leaf = leaf;
	node->items = (char *)node + items_offset;
	return node;
}

static void node_free(struct btree *btree, struct node *node)
{
	size_t i;

	if (atomic_fetch_sub(1, &node->rc) > 0)
		return;
	if (!node->leaf) {
		for (i = 0; i < (size_t)(node->num_items + 1); i++) {
			node_free(btree, node->children[i]);
		}
	}
	if (btree->item_free) {
		for (i = 0; i < node->num_items; i++) {
			void *item = get_item_at(btree, node, i);
			btree->item_free(item, btree->udata);
		}
	}
	kfree(node);
}

static struct node *node_copy(struct btree *btree, struct node *node)
{
	struct node *node2 = node_new(btree, node->leaf);
	size_t items_cloned = 0;
	size_t i;

	if (!node2)
		return NULL;
	node2->num_items = node->num_items;
	if (!node2->leaf) {
		for (i = 0; i < (size_t)(node2->num_items + 1); i++) {
			node2->children[i] = node->children[i];
			atomic_fetch_add(1, &node2->children[i]->rc);
		}
	}
	if (btree->item_clone) {
		for (i = 0; i < node2->num_items; i++) {
			void *item = get_item_at(btree, node, i);
			if (!btree->item_clone(item, SPARE_NODE_COPY,
					       btree->udata)) {
				goto failed;
			}
			set_item_at(btree, node2, i, SPARE_NODE_COPY);
			items_cloned++;
		}
	} else {
		for (i = 0; i < node2->num_items; i++) {
			void *item = get_item_at(btree, node, i);
			set_item_at(btree, node2, i, item);
		}
	}
	return node2;
failed:
	if (!node2->leaf) {
		for (i = 0; i < (size_t)(node2->num_items + 1); i++) {
			atomic_fetch_sub(1, &node2->children[i]->rc);
		}
	}
	if (btree->item_free) {
		for (i = 0; i < items_cloned; i++) {
			void *item = get_item_at(btree, node2, i);
			btree->item_free(item, btree->udata);
		}
	}
	kfree(node2);
	return NULL;
}

#define cow_node_or(bnode, code)                                        \
	{                                                               \
		if (atomic_read(&(bnode)->rc) > 0) {                    \
			struct node *node2 = node_copy(btree, (bnode)); \
			if (!node2) {                                   \
				code;                                   \
			}                                               \
			atomic_fetch_sub(1, &(bnode)->rc);              \
			(bnode) = node2;                                \
		}                                                       \
	}

void btree_clear(struct btree *btree)
{
	if (btree->root) {
		node_free(btree, btree->root);
	}
	btree->oom = false;
	btree->root = NULL;
	btree->count = 0;
	btree->height = 0;
}

void btree_free(struct btree *btree)
{
	btree_clear(btree);
	kfree(btree);
}

void btree_set_item_callbacks(struct btree *btree,
			      bool (*clone)(const void *item, void *into,
					    void *udata),
			      void (*free)(const void *item, void *udata))
{
	btree->item_clone = clone;
	btree->item_free = free;
}

struct btree *btree_clone(struct btree *btree)
{
	size_t size;
	struct btree *btree2;

	if (!btree)
		return NULL;
	size = btree_memsize(btree->elsize, NULL);
	btree2 = kmalloc(size, GFP_NOFS);
	if (!btree2)
		return NULL;
	memcpy(btree2, btree, size);
	if (btree2->root)
		atomic_fetch_add(1, &btree2->root->rc);
	return btree2;
}

static size_t btree_search(const struct btree *btree, struct node *node,
			   const void *key, bool *found, uint64_t *hint,
			   int depth)
{
	if (!hint) {
		return node_bsearch(btree, node, key, found);
	}
	return node_bsearch_hint(btree, node, key, found, hint, depth);
}

enum mut_result {
	NOCHANGE = 0,
	INSERTED = 1,
	MUST_SPLIT = 2,
	REPLACED = 3,
	NOMEM = 4,
	DELETED = 5,
	// INSERTED or REPLACED or DELETED can be checked with (res&1)
};

static void node_split(struct btree *btree, struct node *node,
		       struct node **right, void **median)
{
	size_t i;
	int mid;

	*right = node_new(btree, node->leaf);
	if (!*right)
		return; // NOMEM
	mid = (int)(btree->max_items) / 2;
	*median = get_item_at(btree, node, (size_t)mid);
	(*right)->leaf = node->leaf;
	(*right)->num_items = node->num_items - ((short)mid + 1);
	memmove((*right)->items, node->items + (int)btree->elsize * (mid + 1),
		(size_t)(*right)->num_items * btree->elsize);
	if (!node->leaf) {
		for (i = 0; i <= (*right)->num_items; i++) {
			(*right)->children[i] = node->children[mid + 1 + i];
		}
	}
	node->num_items = (short)mid;
}

static enum mut_result bnode_set(struct btree *btree, struct node *node,
				 const void *item, uint64_t *hint, int depth)
{
	// assert(atomic_load(&node->rc) == 0);
	bool found = false;
	size_t i;
	enum mut_result result;
	void *median = NULL;
	struct node *right = NULL;

	i = btree_search(btree, node, item, &found, hint, depth);
	if (found) {
		swap_item_at(btree, node, i, item, SPARE_RETURN);
		return REPLACED;
	}
	if (node->leaf) {
		if (node->num_items == btree->max_items) {
			return MUST_SPLIT;
		}
		node_shift_right(btree, node, i);
		set_item_at(btree, node, i, item);
		return INSERTED;
	}
	cow_node_or(node->children[i], return NOMEM);
	result = bnode_set(btree, node->children[i], item, hint, depth + 1);
	if (result & 1) { // if (INSERTED or REPLACED)
		return result;
	} else if (result == NOMEM) {
		return NOMEM;
	}
	// assert(result == MUST_SPLIT);
	if (node->num_items == btree->max_items) {
		return MUST_SPLIT;
	}
	node_split(btree, node->children[i], &right, &median);
	if (!right) {
		return NOMEM;
	}
	node_shift_right(btree, node, i);
	set_item_at(btree, node, i, median);
	node->children[i + 1] = right;
	return bnode_set(btree, node, item, hint, depth);
}

static void *btree_set0(struct btree *btree, const void *item, uint64_t *hint,
			bool no_item_clone)
{
	bool item_cloned = false;
	enum mut_result result;
	void *old_root;
	struct node *new_root = NULL;
	struct node *right = NULL;
	void *median = NULL;

	btree->oom = false;
	if (btree->item_clone && !no_item_clone) {
		if (!btree->item_clone(item, SPARE_ITEM_CLONE, btree->udata)) {
			goto oom;
		}
		item = SPARE_ITEM_CLONE;
		item_cloned = true;
	}
	if (!btree->root) {
		btree->root = node_new(btree, true);
		if (!btree->root)
			goto oom;
		set_item_at(btree, btree->root, 0, item);
		btree->root->num_items = 1;
		btree->count++;
		btree->height++;
		return NULL;
	}
	cow_node_or(btree->root, goto oom);
set:
	result = bnode_set(btree, btree->root, item, hint, 0);
	if (result == REPLACED) {
		if (btree->item_free) {
			btree->item_free(SPARE_RETURN, btree->udata);
		}
		return SPARE_RETURN;
	} else if (result == INSERTED) {
		btree->count++;
		return NULL;
	} else if (result == NOMEM) {
		goto oom;
	}
	// assert(result == MUST_SPLIT);
	old_root = btree->root;
	new_root = node_new(btree, false);
	if (!new_root)
		goto oom;
	node_split(btree, old_root, &right, &median);
	if (!right) {
		kfree(new_root);
		goto oom;
	}
	btree->root = new_root;
	btree->root->children[0] = old_root;
	set_item_at(btree, btree->root, 0, median);
	btree->root->children[1] = right;
	btree->root->num_items = 1;
	btree->height++;
	goto set;
oom:
	if (btree->item_free) {
		if (item_cloned) {
			btree->item_free(SPARE_ITEM_CLONE, btree->udata);
		}
	}
	btree->oom = true;
	return NULL;
}

static const void *btree_get0(const struct btree *btree, const void *key,
			      uint64_t *hint)
{
	struct node *node = btree->root;
	bool found;
	int depth = 0;

	if (!node)
		return NULL;
	while (1) {
		size_t i = btree_search(btree, node, key, &found, hint, depth);
		if (found)
			return get_item_at((void *)btree, node, i);
		if (node->leaf)
			return NULL;
		node = node->children[i];
		depth++;
	}
}

static void node_rebalance(struct btree *btree, struct node *node, size_t i)
{
	struct node *left;
	struct node *right;

	if (i == node->num_items) {
		i--;
	}

	left = node->children[i];
	right = node->children[i + 1];

	// assert(atomic_load(&left->rc)==0);
	// assert(atomic_load(&right->rc)==0);

	if (left->num_items + right->num_items < btree->max_items) {
		// Merges the left and right children nodes together as a single node
		// that includes (left,item,right), and places the contents into the
		// existing left node. Delete the right node altogether and move the
		// following items and child nodes to the left by one slot.

		// merge (left,item,right)
		copy_item(btree, left, left->num_items, node, i);
		left->num_items++;
		node_join(btree, left, right);
		kfree(right);
		node_shift_left(btree, node, i, true);
	} else if (left->num_items > right->num_items) {
		// move left -> right over one slot

		// Move the item of the parent node at index into the right-node first
		// slot, and move the left-node last item into the previously moved
		// parent item slot.
		node_shift_right(btree, right, 0);
		copy_item(btree, right, 0, node, i);
		if (!left->leaf) {
			right->children[0] = left->children[left->num_items];
		}
		copy_item(btree, node, i, left, left->num_items - 1);
		if (!left->leaf) {
			left->children[left->num_items] = NULL;
		}
		left->num_items--;
	} else {
		// move right -> left

		// Same as above but the other direction
		copy_item(btree, left, left->num_items, node, i);
		if (!left->leaf) {
			left->children[left->num_items + 1] =
				right->children[0];
		}
		left->num_items++;
		copy_item(btree, node, i, right, 0);
		node_shift_left(btree, right, 0, false);
	}
}

static enum mut_result node_delete(struct btree *btree, struct node *node,
				   enum delact act, size_t index,
				   const void *key, void *prev, uint64_t *hint,
				   int depth)
{
	// assert(atomic_load(&node->rc)==0);
	size_t i = 0;
	bool found = false;
	enum mut_result result;

	switch (act) {
	case POPMAX:
		i = node->num_items - 1;
		found = true;
		break;
	case POPFRONT:
		i = 0;
		found = node->leaf;
		break;
	case POPBACK:
		if (!node->leaf) {
			i = node->num_items;
			found = false;
		} else {
			i = node->num_items - 1;
			found = true;
		}
		break;
	case DELKEY:
		i = btree_search(btree, node, key, &found, hint, depth);
		break;
	}
	if (node->leaf) {
		if (found) {
			// Item was found in leaf, copy its contents and delete it.
			// This might cause the number of items to drop below min_items,
			// and it so, the caller will take care of the rebalancing.
			copy_item_into(btree, node, i, prev);
			node_shift_left(btree, node, i, false);
			return DELETED;
		}
		return NOCHANGE;
	}

	if (found) {
		if (act == POPMAX) {
			// Popping off the max item into into its parent branch to maintain
			// a balanced tree.
			i++;
			cow_node_or(node->children[i], return NOMEM);
			cow_node_or(
				node->children[i == node->num_items ? i - 1 :
								      i + 1],
				return NOMEM);
			result = node_delete(btree, node->children[i], POPMAX,
					     0, NULL, prev, hint, depth + 1);
			if (result == NOMEM)
				return NOMEM;
			result = DELETED;
		} else {
			// item was found in branch, copy its contents, delete it, and
			// begin popping off the max items in child nodes.
			copy_item_into(btree, node, i, prev);
			cow_node_or(node->children[i], return NOMEM);
			cow_node_or(
				node->children[i == node->num_items ? i - 1 :
								      i + 1],
				return NOMEM);
			result = node_delete(btree, node->children[i], POPMAX,
					     0, NULL, SPARE_POP, hint,
					     depth + 1);
			if (result == NOMEM)
				return NOMEM;
			set_item_at(btree, node, i, SPARE_POP);
			result = DELETED;
		}
	} else {
		// item was not found in this branch, keep searching.
		cow_node_or(node->children[i], return NOMEM);
		cow_node_or(
			node->children[i == node->num_items ? i - 1 : i + 1],
			return NOMEM);
		result = node_delete(btree, node->children[i], act, index, key,
				     prev, hint, depth + 1);
	}
	if (result != DELETED) {
		return result;
	}
	if (node->children[i]->num_items < btree->min_items) {
		node_rebalance(btree, node, i);
	}
	return DELETED;
}

static void *btree_delete0(struct btree *btree, enum delact act, size_t index,
			   const void *key, uint64_t *hint)
{
	enum mut_result result;

	btree->oom = false;
	if (!btree->root)
		return NULL;
	cow_node_or(btree->root, goto oom);
	result = node_delete(btree, btree->root, act, index, key, SPARE_RETURN,
			     hint, 0);
	if (result == NOCHANGE) {
		return NULL;
	} else if (result == NOMEM) {
		goto oom;
	}
	// assert(result == DELETED);
	if (btree->root->num_items == 0) {
		struct node *old_root = btree->root;
		if (!btree->root->leaf) {
			btree->root = btree->root->children[0];
		} else {
			btree->root = NULL;
		}
		kfree(old_root);
		btree->height--;
	}
	btree->count--;
	if (btree->item_free) {
		btree->item_free(SPARE_RETURN, btree->udata);
	}
	return SPARE_RETURN;
oom:
	btree->oom = true;
	return NULL;
}

const void *btree_set_hint(struct btree *btree, const void *item,
			   uint64_t *hint)
{
	return btree_set0(btree, item, hint, false);
}

const void *btree_set(struct btree *btree, const void *item)
{
	return btree_set0(btree, item, NULL, false);
}

const void *btree_get_hint(const struct btree *btree, const void *key,
			   uint64_t *hint)
{
	return btree_get0(btree, key, hint);
}

const void *btree_get(const struct btree *btree, const void *key)
{
	return btree_get0(btree, key, NULL);
}

const void *btree_delete_hint(struct btree *btree, const void *key,
			      uint64_t *hint)
{
	return btree_delete0(btree, DELKEY, 0, key, hint);
}

const void *btree_delete(struct btree *btree, const void *key)
{
	return btree_delete0(btree, DELKEY, 0, key, NULL);
}

const void *btree_pop_min(struct btree *btree)
{
	struct node *node;

	btree->oom = false;
	if (btree->root) {
		cow_node_or(btree->root, goto oom);
		node = btree->root;
		while (1) {
			if (node->leaf) {
				if (node->num_items > btree->min_items) {
					size_t i = 0;
					copy_item_into(btree, node, i,
						       SPARE_RETURN);
					node_shift_left(btree, node, i, false);
					if (btree->item_free) {
						btree->item_free(SPARE_RETURN,
								 btree->udata);
					}
					btree->count--;
					return SPARE_RETURN;
				}
				break;
			}
			cow_node_or(node->children[0], goto oom);
			node = node->children[0];
		}
	}
	return btree_delete0(btree, POPFRONT, 0, NULL, NULL);
oom:
	btree->oom = true;
	return NULL;
}

const void *btree_pop_max(struct btree *btree)
{
	struct node *node;

	btree->oom = false;
	if (btree->root) {
		cow_node_or(btree->root, goto oom);
		node = btree->root;
		while (1) {
			if (node->leaf) {
				if (node->num_items > btree->min_items) {
					size_t i = node->num_items - 1;
					copy_item_into(btree, node, i,
						       SPARE_RETURN);
					node->num_items--;
					if (btree->item_free) {
						btree->item_free(SPARE_RETURN,
								 btree->udata);
					}
					btree->count--;
					return SPARE_RETURN;
				}
				break;
			}
			cow_node_or(node->children[node->num_items], goto oom);
			node = node->children[node->num_items];
		}
	}
	return btree_delete0(btree, POPBACK, 0, NULL, NULL);
oom:
	btree->oom = true;
	return NULL;
}

bool btree_oom(const struct btree *btree)
{
	return !btree || btree->oom;
}

size_t btree_count(const struct btree *btree)
{
	return btree->count;
}

int btree_compare(const struct btree *btree, const void *a, const void *b)
{
	return btree->compare(a, b, btree->udata);
}

static bool node_scan(const struct btree *btree, struct node *node,
		      bool (*iter)(const void *item, void *udata), void *udata)
{
	size_t i;

	if (node->leaf) {
		for (i = 0; i < node->num_items; i++) {
			if (!iter(get_item_at((void *)btree, node, i), udata)) {
				return false;
			}
		}
		return true;
	}
	for (i = 0; i < node->num_items; i++) {
		if (!node_scan(btree, node->children[i], iter, udata)) {
			return false;
		}
		if (!iter(get_item_at((void *)btree, node, i), udata)) {
			return false;
		}
	}
	return node_scan(btree, node->children[node->num_items], iter, udata);
}

static bool node_ascend(const struct btree *btree, struct node *node,
			const void *pivot,
			bool (*iter)(const void *item, void *udata),
			void *udata, uint64_t *hint, int depth)
{
	bool found;
	size_t i = btree_search(btree, node, pivot, &found, hint, depth);
	if (!found) {
		if (!node->leaf) {
			if (!node_ascend(btree, node->children[i], pivot, iter,
					 udata, hint, depth + 1)) {
				return false;
			}
		}
	}
	for (; i < node->num_items; i++) {
		if (!iter(get_item_at((void *)btree, node, i), udata)) {
			return false;
		}
		if (!node->leaf) {
			if (!node_scan(btree, node->children[i + 1], iter,
				       udata)) {
				return false;
			}
		}
	}
	return true;
}

bool btree_ascend_hint(const struct btree *btree, const void *pivot,
		       bool (*iter)(const void *item, void *udata), void *udata,
		       uint64_t *hint)
{
	if (btree->root) {
		if (!pivot) {
			return node_scan(btree, btree->root, iter, udata);
		}
		return node_ascend(btree, btree->root, pivot, iter, udata, hint,
				   0);
	}
	return true;
}

bool btree_ascend(const struct btree *btree, const void *pivot,
		  bool (*iter)(const void *item, void *udata), void *udata)
{
	return btree_ascend_hint(btree, pivot, iter, udata, NULL);
}

static bool node_reverse(const struct btree *btree, struct node *node,
			 bool (*iter)(const void *item, void *udata),
			 void *udata)
{
	size_t i;

	if (node->leaf) {
		i = node->num_items - 1;
		while (1) {
			if (!iter(get_item_at((void *)btree, node, i), udata)) {
				return false;
			}
			if (i == 0)
				break;
			i--;
		}
		return true;
	}
	if (!node_reverse(btree, node->children[node->num_items], iter,
			  udata)) {
		return false;
	}
	i = node->num_items - 1;
	while (1) {
		if (!iter(get_item_at((void *)btree, node, i), udata)) {
			return false;
		}
		if (!node_reverse(btree, node->children[i], iter, udata)) {
			return false;
		}
		if (i == 0)
			break;
		i--;
	}
	return true;
}

static bool node_descend(const struct btree *btree, struct node *node,
			 const void *pivot,
			 bool (*iter)(const void *item, void *udata),
			 void *udata, uint64_t *hint, int depth)
{
	bool found;
	size_t i = btree_search(btree, node, pivot, &found, hint, depth);
	if (!found) {
		if (!node->leaf) {
			if (!node_descend(btree, node->children[i], pivot, iter,
					  udata, hint, depth + 1)) {
				return false;
			}
		}
		if (i == 0)
			return true;
		i--;
	}
	while (1) {
		if (!iter(get_item_at((void *)btree, node, i), udata)) {
			return false;
		}
		if (!node->leaf) {
			if (!node_reverse(btree, node->children[i], iter,
					  udata)) {
				return false;
			}
		}
		if (i == 0)
			break;
		i--;
	}
	return true;
}

bool btree_descend_hint(const struct btree *btree, const void *pivot,
			bool (*iter)(const void *item, void *udata),
			void *udata, uint64_t *hint)
{
	if (btree->root) {
		if (!pivot) {
			return node_reverse(btree, btree->root, iter, udata);
		}
		return node_descend(btree, btree->root, pivot, iter, udata,
				    hint, 0);
	}
	return true;
}

bool btree_descend(const struct btree *btree, const void *pivot,
		   bool (*iter)(const void *item, void *udata), void *udata)
{
	return btree_descend_hint(btree, pivot, iter, udata, NULL);
}

const void *btree_min(const struct btree *btree)
{
	struct node *node = btree->root;
	if (!node)
		return NULL;
	while (1) {
		if (node->leaf) {
			return get_item_at((void *)btree, node, 0);
		}
		node = node->children[0];
	}
}

const void *btree_max(const struct btree *btree)
{
	struct node *node = btree->root;
	if (!node)
		return NULL;
	while (1) {
		if (node->leaf) {
			return get_item_at((void *)btree, node,
					   node->num_items - 1);
		}
		node = node->children[node->num_items];
	}
}

const void *btree_load(struct btree *btree, const void *item)
{
	bool item_cloned;
	struct node *node;
	const void *prev;

	btree->oom = false;
	if (!btree->root) {
		return btree_set0(btree, item, NULL, false);
	}
	item_cloned = false;
	if (btree->item_clone) {
		if (!btree->item_clone(item, SPARE_ITEM_CLONE, btree->udata)) {
			goto oom;
		}
		item = SPARE_ITEM_CLONE;
		item_cloned = true;
	}
	cow_node_or(btree->root, goto oom);
	node = btree->root;
	while (1) {
		if (node->leaf) {
			void *litem;

			if (node->num_items == btree->max_items)
				break;
			litem = get_item_at(btree, node, node->num_items - 1);
			if (btcompare(btree, item, litem) <= 0)
				break;
			set_item_at(btree, node, node->num_items, item);
			node->num_items++;
			btree->count++;
			return NULL;
		}
		cow_node_or(node->children[node->num_items], goto oom);
		node = node->children[node->num_items];
	}
	prev = btree_set0(btree, item, NULL, true);
	if (!btree->oom)
		return prev;
oom:
	if (btree->item_free && item_cloned) {
		btree->item_free(SPARE_ITEM_CLONE, btree->udata);
	}
	btree->oom = true;
	return NULL;
}

size_t btree_height(const struct btree *btree)
{
	return btree->height;
}

#ifdef TEST_PRIVATE_FUNCTIONS
#include "tests/priv_funcs.h"
#endif