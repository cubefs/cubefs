/*
 * Copyright 2023 The CubeFS Authors.
 */
#ifndef __CFS_JSON_H__
#define __CFS_JSON_H__

#include "cfs_common.h"

typedef struct cfs_json {
	void *parser;
	size_t index;
} cfs_json_t;

cfs_json_t *cfs_json_parse(const char *js, size_t len);
void cfs_json_release(cfs_json_t *json);
int cfs_json_get_object(cfs_json_t *json, const char *key, cfs_json_t *val);
int cfs_json_get_object_key(cfs_json_t *json, char **key);
int cfs_json_get_object_key_ptr(cfs_json_t *json, const char **key,
				size_t *len);
int cfs_json_get_object_value(cfs_json_t *json, cfs_json_t *val);
int cfs_json_get_string(cfs_json_t *json, const char *key, char **val);
int cfs_json_get_string_ptr(cfs_json_t *json, const char *key, const char **val,
			    size_t *len);
int cfs_json_get_u64(cfs_json_t *json, const char *key, u64 *val);
int cfs_json_get_s64(cfs_json_t *json, const char *key, s64 *val);
int cfs_json_get_u32(cfs_json_t *json, const char *key, u32 *val);
int cfs_json_get_u8(cfs_json_t *json, const char *key, u8 *val);
int cfs_json_get_s8(cfs_json_t *json, const char *key, s8 *val);
int cfs_json_get_bool(cfs_json_t *json, const char *key, bool *val);
size_t cfs_json_get_array_size(cfs_json_t *array);
int cfs_json_get_array_item(cfs_json_t *array, size_t index, cfs_json_t *item);
int cfs_json_get_value_string(cfs_json_t *json, char **val);
int cfs_json_get_value_string_ptr(cfs_json_t *json, const char **val,
				  size_t *len);
int cfs_json_get_value_u64(cfs_json_t *json, u64 *val);
int cfs_json_get_value_s64(cfs_json_t *json, s64 *val);
int cfs_json_get_value_u32(cfs_json_t *json, u32 *val);
int cfs_json_get_value_u8(cfs_json_t *json, u8 *val);
int cfs_json_get_value_s8(cfs_json_t *json, s8 *val);
int cfs_json_get_value_bool(cfs_json_t *json, bool *val);

#endif
