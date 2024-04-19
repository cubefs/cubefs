#ifndef _CB_HASH_MAP_H_
#define _CB_HASH_MAP_H_
#include <stdint.h>
#include "khash.h"

KHASH_MAP_INIT_INT64(map, uint64_t);
khash_t(map) *hashmap_create();
void hashmap_destroy(khash_t(map) *hashmap);
uint64_t hashmap_get(khash_t(map) *hashmap, uint64_t key);
int32_t hashmap_put(khash_t(map) *hashmap, uint64_t key, uint64_t value);
int32_t hashmap_exist(khash_t(map) *hashmap, uint64_t key);
int hashmap_del(khash_t(map) *hashmap, uint64_t key);

#endif