/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_json.h"

#define JSMN_PARENT_LINKS
#include "jsmn.h"

struct cfs_json_parser {
	jsmn_parser parser;
	jsmntok_t *tokens;
	size_t tokens_count;
	const char *js;
	cfs_json_t root;
};

static int jsoneq(const char *json, jsmntok_t *tok, const char *s)
{
	if (tok->type == JSMN_STRING &&
	    (int)strlen(s) == tok->end - tok->start &&
	    strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
		return 0;
	}
	return -1;
}

cfs_json_t *cfs_json_parse(const char *js, size_t len)
{
	struct cfs_json_parser *parser;
	int ret;

	parser = kmalloc(sizeof(*parser), GFP_NOFS);
	if (!parser)
		return NULL;
	jsmn_init(&parser->parser);
	ret = jsmn_parse(&parser->parser, js, len, NULL, 0);
	if (ret < 0) {
		kfree(parser);
		return NULL;
	}
	if (ret == 0) {
		kfree(parser);
		return NULL;
	}
	parser->tokens_count = ret;
	parser->tokens =
		kcalloc(parser->tokens_count, sizeof(jsmntok_t), GFP_NOFS);
	if (!parser->tokens) {
		kfree(parser);
		return NULL;
	}
	jsmn_init(&parser->parser);
	jsmn_parse(&parser->parser, js, len, parser->tokens,
		   parser->tokens_count);

	parser->js = js;
	parser->root.parser = parser;
	parser->root.index = 0;
	return &parser->root;
}

void cfs_json_release(cfs_json_t *json)
{
	struct cfs_json_parser *parser;

	parser = json->parser;
	kfree(parser->tokens);
	kfree(parser);
}

int cfs_json_get_object(cfs_json_t *json, const char *key, cfs_json_t *val)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;
	size_t i;

	parser = json->parser;
	for (i = json->index + 1; i < parser->tokens_count; i++) {
		token = &parser->tokens[i];
		if (token->parent == json->index &&
		    jsoneq(parser->js, token, key) == 0) {
			val->parser = parser;
			val->index = i + 1;
			return 0;
		}
	}
	return -ENOENT;
}

/**
 * Return the key of json object.
 */
int cfs_json_get_object_key(cfs_json_t *json, char **key)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = json->parser;
	token = &parser->tokens[json->index];
	if (token->type != JSMN_OBJECT)
		return -1;
	*key = kstrndup(parser->js + token->start, token->end - token->start,
			GFP_KERNEL);
	if (!*key)
		return -ENOMEM;
	return 0;
}

/**
 * Return the key of json object.
 */
int cfs_json_get_object_key_ptr(cfs_json_t *json, const char **key, size_t *len)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = json->parser;
	token = &parser->tokens[json->index];
	*key = parser->js + token->start;
	*len = token->end - token->start;
	return 0;
}

/**
 * Return the value of json object.
 */
int cfs_json_get_object_value(cfs_json_t *json, cfs_json_t *val)
{
	struct cfs_json_parser *parser;

	parser = json->parser;
	if (json->index + 1 >= parser->tokens_count)
		return -1;
	val->parser = parser;
	val->index = json->index + 1;
	return 0;
}

int cfs_json_get_string(cfs_json_t *json, const char *key, char **val)
{
	cfs_json_t json_val;
	int ret;

	ret = cfs_json_get_object(json, key, &json_val);
	if (ret < 0)
		return ret;
	return cfs_json_get_value_string(&json_val, val);
}

int cfs_json_get_string_ptr(cfs_json_t *json, const char *key, const char **val,
			    size_t *len)
{
	cfs_json_t json_val;
	int ret;

	ret = cfs_json_get_object(json, key, &json_val);
	if (ret < 0)
		return ret;
	return cfs_json_get_value_string_ptr(&json_val, val, len);
}

int cfs_json_get_u64(cfs_json_t *json, const char *key, u64 *val)
{
	cfs_json_t json_val;
	int ret;

	ret = cfs_json_get_object(json, key, &json_val);
	if (ret < 0)
		return ret;
	return cfs_json_get_value_u64(&json_val, val);
}

int cfs_json_get_s64(cfs_json_t *json, const char *key, s64 *val)
{
	cfs_json_t json_val;
	int ret;

	ret = cfs_json_get_object(json, key, &json_val);
	if (ret < 0)
		return ret;
	return cfs_json_get_value_s64(&json_val, val);
}

int cfs_json_get_u32(cfs_json_t *json, const char *key, u32 *val)
{
	cfs_json_t json_val;
	int ret;

	ret = cfs_json_get_object(json, key, &json_val);
	if (ret < 0)
		return ret;
	return cfs_json_get_value_u32(&json_val, val);
}

int cfs_json_get_u8(cfs_json_t *json, const char *key, u8 *val)
{
	cfs_json_t json_val;
	int ret;

	ret = cfs_json_get_object(json, key, &json_val);
	if (ret < 0)
		return ret;
	return cfs_json_get_value_u8(&json_val, val);
}

int cfs_json_get_s8(cfs_json_t *json, const char *key, s8 *val)
{
	cfs_json_t json_val;
	int ret;

	ret = cfs_json_get_object(json, key, &json_val);
	if (ret < 0)
		return ret;
	return cfs_json_get_value_s8(&json_val, val);
}

int cfs_json_get_bool(cfs_json_t *json, const char *key, bool *val)
{
	cfs_json_t json_val;
	int ret;

	ret = cfs_json_get_object(json, key, &json_val);
	if (ret < 0)
		return ret;
	return cfs_json_get_value_bool(&json_val, val);
}

size_t cfs_json_get_array_size(cfs_json_t *array)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = array->parser;
	token = &parser->tokens[array->index];
	return token->size;
}

int cfs_json_get_array_item(cfs_json_t *array, size_t index, cfs_json_t *item)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;
	size_t i;
	size_t count = 0;

	parser = array->parser;
	for (i = array->index + 1; i < parser->tokens_count; i++) {
		token = &parser->tokens[i];
		if (token->parent == array->index) {
			count++;
			if (count == index + 1) {
				item->parser = parser;
				item->index = i;
				return 0;
			}
		}
	}
	return -EOVERFLOW;
}

int cfs_json_get_value_string(cfs_json_t *json, char **val)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = json->parser;
	token = &parser->tokens[json->index];
	if (token->type != JSMN_STRING)
		return -1;
	*val = kstrndup(parser->js + token->start, token->end - token->start,
			GFP_KERNEL);
	if (!*val)
		return -ENOMEM;
	return 0;
}

int cfs_json_get_value_string_ptr(cfs_json_t *json, const char **val,
				  size_t *len)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = json->parser;
	token = &parser->tokens[json->index];
	if (token->type != JSMN_STRING)
		return -1;
	*val = parser->js + token->start;
	*len = token->end - token->start;
	return 0;
}

int cfs_json_get_value_u64(cfs_json_t *json, u64 *val)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = json->parser;
	token = &parser->tokens[json->index];
	if (token->type != JSMN_PRIMITIVE)
		return -1;
	return cfs_kstrntou64(parser->js + token->start,
			      token->end - token->start, 10, val);
}

int cfs_json_get_value_s64(cfs_json_t *json, s64 *val)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = json->parser;
	token = &parser->tokens[json->index];
	if (token->type != JSMN_PRIMITIVE)
		return -1;
	return cfs_kstrntos64(parser->js + token->start,
			      token->end - token->start, 10, val);
}

int cfs_json_get_value_u32(cfs_json_t *json, u32 *val)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = json->parser;
	token = &parser->tokens[json->index];
	if (token->type != JSMN_PRIMITIVE)
		return -1;
	return cfs_kstrntou32(parser->js + token->start,
			      token->end - token->start, 10, val);
}

int cfs_json_get_value_u8(cfs_json_t *json, u8 *val)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = json->parser;
	token = &parser->tokens[json->index];
	if (token->type != JSMN_PRIMITIVE)
		return -1;
	return cfs_kstrntou8(parser->js + token->start,
			     token->end - token->start, 10, val);
}

int cfs_json_get_value_s8(cfs_json_t *json, s8 *val)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = json->parser;
	token = &parser->tokens[json->index];
	if (token->type != JSMN_PRIMITIVE)
		return -1;
	return cfs_kstrntos8(parser->js + token->start,
			     token->end - token->start, 10, val);
}

int cfs_json_get_value_bool(cfs_json_t *json, bool *val)
{
	struct cfs_json_parser *parser;
	jsmntok_t *token;

	parser = json->parser;
	token = &parser->tokens[json->index];
	if (token->type != JSMN_PRIMITIVE)
		return -1;
	return cfs_kstrntobool(parser->js + token->start,
			       token->end - token->start, val);
}
