#include "cfs_common.h"

/*
 * Nicely render a sockaddr as a string.  An array of formatted
 * strings is used, to approximate reentrancy.
 */
#define ADDR_STR_COUNT_LOG 5 /* log2(# address strings in array) */
#define ADDR_STR_COUNT (1 << ADDR_STR_COUNT_LOG)
#define ADDR_STR_COUNT_MASK (ADDR_STR_COUNT - 1)
#define MAX_ADDR_STR_LEN 64 /* 54 is enough */

static char addr_str[ADDR_STR_COUNT][MAX_ADDR_STR_LEN];
static atomic_t addr_str_seq = ATOMIC_INIT(0);

const char *cfs_pr_addr(const struct sockaddr_storage *ss)
{
	int i;
	char *s;
	struct sockaddr_in *in4 = (struct sockaddr_in *)ss;
	struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)ss;

	i = atomic_inc_return(&addr_str_seq) & ADDR_STR_COUNT_MASK;
	s = addr_str[i];

	switch (ss->ss_family) {
	case AF_INET:
		snprintf(s, MAX_ADDR_STR_LEN, "%pI4:%hu", &in4->sin_addr,
			 ntohs(in4->sin_port));
		break;

	case AF_INET6:
		snprintf(s, MAX_ADDR_STR_LEN, "[%pI6c]:%hu", &in6->sin6_addr,
			 ntohs(in6->sin6_port));
		break;

	default:
		snprintf(s, MAX_ADDR_STR_LEN, "(unknown sockaddr family %hu)",
			 ss->ss_family);
	}

	return s;
}

/**
 * Parse '127.0.0.1:80', '[::1]:80'.
 */
int cfs_parse_addr(const char *str, size_t len, struct sockaddr_storage *ss)
{
	struct sockaddr_in *in4 = (struct sockaddr_in *)ss;
	struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)ss;
	char *p;

	memset(ss, 0, sizeof(*ss));

	if ((p = strnstr(str, "]", len))) {
		ss->ss_family = AF_INET6;
		if (!in6_pton(str + 1, p - (str + 1),
			      (u8 *)&in6->sin6_addr.s6_addr, -1, NULL))
			return -1;
		p = p + 2;
		if ((p - str) >= len)
			return -1;
		len = len - (p - str);
		if (cfs_kstrntou16(p, len, 10, &in6->sin6_port) < 0)
			return -1;
		in6->sin6_port = cpu_to_be16(in6->sin6_port);
		return 0;
	} else if ((p = strnstr(str, ":", len))) {
		ss->ss_family = AF_INET;
		if (!in4_pton(str, (p - str), (u8 *)&in4->sin_addr.s_addr, -1,
			      NULL))
			return -1;
		p = p + 1;
		if ((p - str) >= len)
			return -1;
		len = len - (p - str);
		if (cfs_kstrntou16(p, len, 10, &in4->sin_port) < 0)
			return -1;
		in4->sin_port = cpu_to_be16(in4->sin_port);
		return 0;
	} else {
		return -1;
	}
}

int cfs_addr_cmp(const struct sockaddr_storage *ss1,
		 const struct sockaddr_storage *ss2)
{
	const struct sockaddr_in *in41 = (const struct sockaddr_in *)ss1;
	const struct sockaddr_in6 *in61 = (const struct sockaddr_in6 *)ss1;
	const struct sockaddr_in *in42 = (const struct sockaddr_in *)ss2;
	const struct sockaddr_in6 *in62 = (const struct sockaddr_in6 *)ss2;

	if (ss1->ss_family != ss2->ss_family)
		return -1;

	switch (ss1->ss_family) {
	case AF_INET:
		if (in41->sin_addr.s_addr != in42->sin_addr.s_addr)
			return -1;
		if (in41->sin_port != in42->sin_port)
			return -1;
		return 0;

	case AF_INET6:
		if (memcmp(&in61->sin6_addr, &in62->sin6_addr,
			   sizeof(in61->sin6_addr)) != 0)
			return -1;
		if (in61->sin6_port != in62->sin6_port)
			return -1;
		return 0;

	default:
		return -1;
	}
}

#define TIME_STR_COUNT_LOG 5 /* log2(# time strings in array) */
#define TIME_STR_COUNT (1 << TIME_STR_COUNT_LOG)
#define TIME_STR_COUNT_MASK (TIME_STR_COUNT - 1)
#define MAX_TIME_STR_LEN 32

static char time_str[TIME_STR_COUNT][MAX_TIME_STR_LEN];
static atomic_t time_str_seq = ATOMIC_INIT(0);

const char *cfs_pr_time(struct timespec *time)
{
	int i;
	char *s;
	struct tm result;

	i = atomic_inc_return(&time_str_seq) & TIME_STR_COUNT_MASK;
	s = time_str[i];
	time_to_tm(time->tv_sec, sys_tz.tz_minuteswest / 60, &result);
	snprintf(s, MAX_TIME_STR_LEN,
		 "%04ld-%02d-%02dT%02d:%02d:%02d+%02d:%02d",
		 result.tm_year + 1900, result.tm_mon + 1, result.tm_mday,
		 result.tm_hour, result.tm_min, result.tm_sec,
		 sys_tz.tz_minuteswest / 60, abs(sys_tz.tz_minuteswest % 60));
	return s;
}

/**
 * @param str in, format: 2023-07-24T20:44:10+08:00
 */
int cfs_parse_time(const char *str, size_t len, struct timespec *time)
{
	u32 year, mon, day, hour, min, sec;
	u32 tz_hour, tz_min, tz_offset;
	const char *end = str + len;
	const char *pos;
	int ret;

	if (str == NULL || time == NULL)
		return -EINVAL;

	pos = strnchr(str, end - str, '-');
	if (pos == NULL)
		return -EINVAL;
	ret = cfs_kstrntou32(str, pos - str, 10, &year);
	if (ret < 0)
		return ret;
	str = pos + 1;

	pos = strnchr(str, end - str, '-');
	if (!pos)
		return -EINVAL;
	ret = cfs_kstrntou32(str, pos - str, 10, &mon);
	if (ret < 0)
		return ret;
	str = pos + 1;

	pos = strnchr(str, end - str, 'T');
	if (!pos)
		return -EINVAL;
	ret = cfs_kstrntou32(str, pos - str, 10, &day);
	if (ret < 0)
		return ret;
	str = pos + 1;

	pos = strnchr(str, end - str, ':');
	if (!pos)
		return -EINVAL;
	ret = cfs_kstrntou32(str, pos - str, 10, &hour);
	if (ret < 0)
		return ret;
	str = pos + 1;

	pos = strnchr(str, end - str, ':');
	if (!pos)
		return -EINVAL;
	ret = cfs_kstrntou32(str, pos - str, 10, &min);
	if (ret < 0)
		return ret;
	str = pos + 1;

	pos = strnchr(str, end - str, '+');
	if (!pos)
		return -EINVAL;
	ret = cfs_kstrntou32(str, pos - str, 10, &sec);
	if (ret < 0)
		return ret;
	str = pos + 1;

	pos = strnchr(str, end - str, ':');
	if (!pos)
		return -EINVAL;
	ret = cfs_kstrntou32(str, pos - str, 10, &tz_hour);
	if (ret < 0)
		return ret;
	str = pos + 1;

	pos = end;
	ret = cfs_kstrntou32(str, pos - str, 10, &tz_min);
	if (ret < 0)
		return ret;

	tz_offset = ((tz_hour * 60) + tz_min) * 60;
	time->tv_sec = mktime(year, mon, day, hour, min, sec) - tz_offset;
	time->tv_nsec = 0;
	return 0;
}

static const char base64table[] =
	"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static int encode_bits(int c)
{
	return base64table[c];
}

static int decode_bits(char c)
{
	if (c >= 'A' && c <= 'Z')
		return c - 'A';
	if (c >= 'a' && c <= 'z')
		return c - 'a' + 26;
	if (c >= '0' && c <= '9')
		return c - '0' + 52;
	if (c == '+')
		return 62;
	if (c == '/')
		return 63;
	if (c == '=')
		return 0;
	return -EINVAL;
}

int cfs_base64_encode(const char *str, size_t str_len, char **base64)
{
	size_t i;
	char *buf, *dst;

	buf = kzalloc(((str_len / 3) + !!(str_len % 3)) * 4 + 1, GFP_KERNEL);
	if (!buf)
		return -ENOMEM;
	dst = buf;
	for (i = 0; i < (str_len / 3) * 3; i += 3) {
		*dst++ = encode_bits(str[i] >> 2);
		*dst++ = encode_bits((str[i] & 0x3) << 4 | str[i + 1] >> 4);
		*dst++ = encode_bits((str[i + 1] & 0xF) << 2 | str[i + 2] >> 6);
		*dst++ = encode_bits(str[i + 2] & 0x3F);
	}
	if (i == str_len - 2) {
		*dst++ = encode_bits(str[i] >> 2);
		*dst++ = encode_bits((str[i] & 0x3) << 4 | str[i + 1] >> 4);
		*dst++ = encode_bits((str[i + 1] & 0xF) << 2);
		*dst++ = '=';
	}
	if (i == str_len - 1) {
		*dst++ = encode_bits(str[i] >> 2);
		*dst++ = encode_bits((str[i] & 0x3) << 4);
		*dst++ = '=';
		*dst++ = '=';
	}
	*base64 = buf;
	return 0;
}

int cfs_base64_decode(const char *base64, size_t base64_len, char **str)
{
	size_t i;
	int a, b, c, d;
	char *buf, *dst;

	if (base64_len % 4 != 0)
		return -EINVAL;
	buf = kzalloc((base64_len / 4) * 3 + 1, GFP_KERNEL);
	if (!buf)
		return -ENOMEM;
	dst = buf;
	for (i = 0; i < base64_len; i += 4) {
		a = decode_bits(base64[i]);
		b = decode_bits(base64[i + 1]);
		c = decode_bits(base64[i + 2]);
		d = decode_bits(base64[i + 3]);
		if (a < 0 || b < 0 || c < 0 || d < 0)
			return -EINVAL;

		*dst++ = (a << 2) | (b >> 4);
		*dst++ = (b << 4) | (c >> 2);
		*dst++ = (c << 6) | d;
	}
	*str = buf;
	return 0;
}
