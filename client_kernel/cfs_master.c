#include "cfs_master.h"
#include "cfs_buffer.h"
#include "cfs_json.h"
#include "cfs_packet.h"
#include "cfs_socket.h"

#define HTTP_GET "GET"
#define HTTP_POST "POST"

#define HTTP_STATUS_OK 200
#define HTTP_STATUS_FORBIDDEN 403

#define HTTP_DATA_SIZE 16384u

#define HTTP_RECV_TIMEOUT_MS 5000u

#define CHECK(ret)                  \
	do {                        \
		int r__ = (ret);    \
		if (r__ < 0)        \
			return r__; \
	} while (0)

struct http_request {
	const char *method;
	const char *path;
	struct sockaddr_storage host;

	/* param */
	const char *p_name;
	const char *p_auth_key;
	const char *p_version;

	/* header */
	bool h_skip_owner_validation;
};

struct http_response {
	u32 status;
	struct sockaddr_storage master_addr;
	cfs_json_t *json_body;
	struct cfs_buffer *buffer;
};

static void http_request_init(struct http_request *request)
{
	memset(request, 0, sizeof(*request));
}

static void http_response_init(struct http_response *response)
{
	memset(response, 0, sizeof(*response));
}

static void http_response_clear(struct http_response *response)
{
	if (response->buffer)
		cfs_buffer_release(response->buffer);
	if (response->json_body)
		cfs_json_release(response->json_body);
	http_response_init(response);
}

/**
 * Parse 'HTTP/1.1 200 OK'.
 */
static int parse_status_code(const char *line, size_t len, u32 *status_code)
{
	char *s;
	char *e;

	s = strnchr(line, len, ' ');
	if (!s)
		return -1;
	s += 1;
	if (len == (s - line))
		return -1;
	else
		len -= (s - line);
	e = strnchr(s, len, ' ');
	if (!e)
		return -1;
	return cfs_kstrntou32(s, e - s, 10, status_code);
}

static int http_request_marshal(struct http_request *request,
				struct cfs_buffer *buffer)
{
	bool first_param = true;

	CHECK(cfs_buffer_write(buffer, "%s %s", request->method,
			       request->path));

	if (request->p_name) {
		if (first_param)
			CHECK(cfs_buffer_write(buffer, "?"));
		else
			CHECK(cfs_buffer_write(buffer, "&"));
		CHECK(cfs_buffer_write(buffer, "name=%s", request->p_name));
		first_param = false;
	}
	if (request->p_auth_key) {
		if (first_param)
			CHECK(cfs_buffer_write(buffer, "?"));
		else
			CHECK(cfs_buffer_write(buffer, "&"));
		CHECK(cfs_buffer_write(buffer, "authKey=%s",
				       request->p_auth_key));
		first_param = false;
	}
	if (request->p_version) {
		if (first_param)
			CHECK(cfs_buffer_write(buffer, "?"));
		else
			CHECK(cfs_buffer_write(buffer, "&"));
		CHECK(cfs_buffer_write(buffer, "version=%s",
				       request->p_version));
		first_param = false;
	}
	CHECK(cfs_buffer_write(buffer, " HTTP/1.1\r\n"));

	CHECK(cfs_buffer_write(buffer, "Host: %s\r\n",
			       cfs_pr_addr(&request->host)));
	if (request->h_skip_owner_validation)
		CHECK(cfs_buffer_write(buffer,
				       "Skip-Owner-Validation: true\r\n"));
	CHECK(cfs_buffer_write(buffer, "Connection: close\r\n"));
	CHECK(cfs_buffer_write(buffer, "Content-Length: 0\r\n"));
	CHECK(cfs_buffer_write(buffer, "\r\n\r\n"));
	return 0;
}

static int do_send_http_request(struct cfs_socket *csk,
				struct http_request *request)
{
	struct cfs_buffer *buffer;
	int ret;

	buffer = cfs_buffer_new(HTTP_DATA_SIZE);
	if (!buffer)
		return -ENOMEM;
	memcpy(&request->host, &csk->ss_dst, sizeof(request->host));
	ret = http_request_marshal(request, buffer);
	if (ret < 0) {
		cfs_buffer_release(buffer);
		return ret;
	}

	// cfs_log_debug("%.*s\n", (int)cfs_buffer_size(buffer),
	// 	      cfs_buffer_data(buffer));

	ret = cfs_socket_send(csk, cfs_buffer_data(buffer),
			      cfs_buffer_size(buffer));
	cfs_buffer_release(buffer);
	return ret;
}

static int do_recv_http_response(struct cfs_socket *csk,
				 struct http_response *response)
{
	char *p;
	int ret;

	http_response_init(response);
	response->buffer = cfs_buffer_new(HTTP_DATA_SIZE);
	if (!response->buffer)
		return -ENOMEM;

	do {
		ret = cfs_socket_recv(csk,
				      cfs_buffer_data(response->buffer) +
					      cfs_buffer_size(response->buffer),
				      cfs_buffer_avail_size(response->buffer));
		if (ret <= 0)
			break;
		cfs_buffer_seek(response->buffer, ret);
		if (cfs_buffer_avail_size(response->buffer) == 0) {
			ret = cfs_buffer_grow(response->buffer, HTTP_DATA_SIZE);
			if (ret < 0)
				break;
		}
	} while (true);
	if (ret < 0)
		goto failed;

	/* http status line */
	p = strnstr(cfs_buffer_data(response->buffer), "\r\n",
		    cfs_buffer_size(response->buffer));
	if (!p) {
		ret = -EBADMSG;
		goto failed;
	}
	ret = parse_status_code(cfs_buffer_data(response->buffer),
				p - cfs_buffer_data(response->buffer),
				&response->status);
	if (ret < 0) {
		ret = -EBADMSG;
		goto failed;
	}

	/* http body */
	p = strnstr(cfs_buffer_data(response->buffer), "\r\n\r\n",
		    cfs_buffer_size(response->buffer));
	if (!p) {
		ret = -EBADMSG;
		goto failed;
	}
	p += 4;

	// cfs_log_debug("%.*s\n",
	// 	      (int)(cfs_buffer_data(response->buffer) +
	// 		    cfs_buffer_size(response->buffer) - p),
	// 	      p);

	switch (response->status) {
	case HTTP_STATUS_FORBIDDEN:
		// ret = cfs_parse_addr(
		// 	p,
		// 	cfs_buffer_data(response->buffer) +
		// 		cfs_buffer_size(response->buffer) - p,
		// 	&response->master_addr);
		// if (ret < 0) {
		// 	cfs_log_err(
		// 		"server response status 403: body is empty\n");
		// 	goto failed;
		// }
		// break;
		cfs_log_err("server response status 403\n");
		ret = -EBADMSG;
		goto failed;
	case HTTP_STATUS_OK: {
		u32 code;
		response->json_body = cfs_json_parse(
			p, cfs_buffer_data(response->buffer) +
				   cfs_buffer_size(response->buffer) - p);
		if (!response->json_body) {
			cfs_log_err(
				"server response status 200: body is invalid json\n");
			ret = -EBADMSG;
			goto failed;
		}
		ret = cfs_json_get_u32(response->json_body, "code", &code);
		if (ret < 0) {
			goto failed;
		}
		if (code != 0) {
			cfs_log_err(
				"server response status 200: body.code=%u\n",
				code);
			return -EBADMSG;
			goto failed;
		}
		break;
	}
	default:
		cfs_log_err("server response unknow status %u\n",
			    response->status);
		ret = -EBADMSG;
		goto failed;
	}
	return 0;

failed:
	http_response_clear(response);
	return ret;
}

static int do_http_request(struct cfs_master_client *mc,
			   struct http_request *request,
			   struct http_response *response)
{
	struct sockaddr_storage *host;
	struct cfs_socket *csk;
	int ret;
	u32 max = mc->hosts.num;
	u32 i = prandom_u32() % mc->hosts.num;

	while (max-- > 0) {
		host = &mc->hosts.base[i++];
		if (i == mc->hosts.num)
			i = 0;
		ret = cfs_socket_create(CFS_SOCK_TYPE_TCP, host, &csk);
		if (ret < 0) {
			cfs_log_err("connect master node %s error %d\n",
				    cfs_pr_addr(host), ret);
			continue;
		}
		ret = cfs_socket_set_recv_timeout(csk, HTTP_RECV_TIMEOUT_MS);
		if (ret < 0) {
			cfs_socket_release(csk, true);
			continue;
		}

		ret = do_send_http_request(csk, request);
		if (ret < 0) {
			cfs_log_err("send http request error %d\n", ret);
			cfs_socket_release(csk, true);
			continue;
		}

		ret = do_recv_http_response(csk, response);
		if (ret < 0) {
			cfs_log_err("recv http response error %d\n", ret);
			cfs_socket_release(csk, true);
			continue;
		}

		cfs_socket_release(csk, true);
		return ret;
	}

	return -1;
}

struct cfs_master_client *
cfs_master_client_new(const struct sockaddr_storage_array *hosts,
		      const char *volume)
{
	struct cfs_master_client *mc;

	mc = kzalloc(sizeof(*mc), GFP_NOFS);
	if (!mc)
		return NULL;
	mc->volume = kstrdup(volume, GFP_NOFS);
	if (!mc->volume) {
		kfree(mc);
		return NULL;
	}
	if (sockaddr_storage_array_clone(&mc->hosts, hosts) < 0) {
		kfree(mc->volume);
		kfree(mc);
		return NULL;
	}
	return mc;
}

void cfs_master_client_release(struct cfs_master_client *mc)
{
	if (!mc)
		return;
	sockaddr_storage_array_clear(&mc->hosts);
	if (mc->volume)
		kfree(mc->volume);
	kfree(mc);
}

/**
 * @param vol_view [out]
 */
int cfs_master_get_volume_without_authkey(struct cfs_master_client *mc,
					  struct cfs_volume_view *vol_view)
{
	struct http_request request;
	struct http_response response;
	cfs_json_t json_data;
	int ret;

	http_request_init(&request);
	request.method = HTTP_POST;
	request.path = "/client/vol";
	request.p_name = mc->volume;
	request.h_skip_owner_validation = true;

	ret = do_http_request(mc, &request, &response);
	if (ret < 0)
		return ret;

	if (!response.json_body) {
		http_response_clear(&response);
		return -EBADMSG;
	}
	ret = cfs_json_get_object(response.json_body, "data", &json_data);
	if (ret < 0) {
		cfs_log_err("not found body.data\n");
		http_response_clear(&response);
		return ret;
	}
	ret = cfs_volume_view_from_json(&json_data, vol_view);
	http_response_clear(&response);
	return ret;
}

/**
 * @param stat [out]
 */
int cfs_master_get_volume_stat(struct cfs_master_client *mc,
			       struct cfs_volume_stat *stat)
{
	struct http_request request;
	struct http_response response;
	cfs_json_t json_data;
	int ret;

	http_request_init(&request);
	request.method = HTTP_GET;
	request.path = "/client/volStat";
	request.p_name = mc->volume;
	request.p_version = "1";

	ret = do_http_request(mc, &request, &response);
	if (ret < 0)
		return ret;

	if (!response.json_body) {
		http_response_clear(&response);
		return -EBADMSG;
	}
	ret = cfs_json_get_object(response.json_body, "data", &json_data);
	if (ret < 0) {
		cfs_log_err("not found body.data\n");
		http_response_clear(&response);
		return ret;
	}
	ret = cfs_volume_stat_from_json(&json_data, stat);
	http_response_clear(&response);
	return ret;
}

/**
 * @param dp_views [out]
 */
int cfs_master_get_data_partitions(
	struct cfs_master_client *mc,
	struct cfs_data_partition_view_array *dp_views)
{
	struct http_request request;
	struct http_response response;
	cfs_json_t json_data;
	cfs_json_t json_dp_views, json_dp_view;
	size_t i;
	int ret;

	http_request_init(&request);
	request.method = HTTP_GET;
	request.path = "/client/partitions";
	request.p_name = mc->volume;

	ret = do_http_request(mc, &request, &response);
	if (ret < 0)
		return ret;

	if (!response.json_body) {
		ret = -EBADMSG;
		goto end;
	}
	ret = cfs_json_get_object(response.json_body, "data", &json_data);
	if (ret < 0) {
		cfs_log_err("not found body.data\n");
		goto end;
	}
	ret = cfs_json_get_object(&json_data, "DataPartitions", &json_dp_views);
	if (ret == 0)
		ret = cfs_json_get_array_size(&json_dp_views);
	if (ret >= 0) {
		ret = cfs_data_partition_view_array_init(dp_views, ret);
		if (ret < 0)
			goto end;
		for (i = 0; i < dp_views->num; i++) {
			ret = cfs_json_get_array_item(&json_dp_views, i,
						      &json_dp_view);
			if (unlikely(ret < 0))
				goto end;
			ret = cfs_data_partition_view_from_json(
				&json_dp_view, &dp_views->base[i]);
			if (ret < 0) {
				cfs_log_err("parse DataPartitions error %d\n",
					    ret);
				goto end;
			}
		}
	}

end:
	http_response_clear(&response);
	return ret;
}

int cfs_master_get_cluster_info(struct cfs_master_client *mc,
				struct cfs_cluster_info *info)
{
	struct http_request request;
	struct http_response response;
	cfs_json_t json_data;
	int ret;

	http_request_init(&request);
	request.method = HTTP_GET;
	request.path = "/admin/getIp";

	ret = do_http_request(mc, &request, &response);
	if (ret < 0)
		return ret;

	if (!response.json_body) {
		ret = -EBADMSG;
		goto end;
	}
	ret = cfs_json_get_object(response.json_body, "data", &json_data);
	if (ret < 0) {
		cfs_log_err("not found body.data\n");
		goto end;
	}
	ret = cfs_cluster_info_from_json(&json_data, info);
	if (ret < 0) {
		cfs_log_err("parse data error %d\n", ret);
		goto end;
	}

end:
	http_response_clear(&response);
	return ret;
}
