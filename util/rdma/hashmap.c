#include "hashmap.h"

khash_t(map) *hashmap_create() {
    return kh_init(map);
}

void hashmap_destroy(khash_t(map) *hashmap) {
    kh_destroy(map, hashmap);
    return;
}

uint64_t hashmap_get(khash_t(map) *hashmap, uint64_t key) {
    khiter_t idx;

    idx = kh_get(map, hashmap, key);
	if (idx == kh_end(hashmap)) {
        // ucs_warn("key not exists, key(0x%lx)", key);
        return 0;
    }

	return kh_value(hashmap, idx);
}

int32_t hashmap_put(khash_t(map) *hashmap, uint64_t key, uint64_t value) {
    int32_t rc;
    khiter_t idx;

    idx = kh_put(map, hashmap, key, &rc);
    if(rc == -1) {
        //ucs_error("key(0x%lx), value(0x%lx)", key, value);
        return -1;
    }
	kh_value(hashmap, idx) = value;

    return 0;
}

int32_t hashmap_exist(khash_t(map) *hashmap, uint64_t key) {
    khiter_t idx = kh_get(map, hashmap, key);
    return idx != kh_end(hashmap) ? 1 : 0;
}

int hashmap_del(khash_t(map) *hashmap, uint64_t key) {
    khiter_t idx;

    idx = kh_get(map, hashmap, key);
    if (idx == kh_end(hashmap)) {
        //ucs_warn("key not exists, key(0x%lx)", key);
        return 0;
    }
	kh_del(map, hashmap, idx);

    return 0;
}
