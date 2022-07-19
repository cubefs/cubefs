#ifndef PACKET_H
#define PACKET_H

#include <arpa/inet.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define htonll(x) ((1==htonl(1)) ? (x) : ((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
#define ntohll(x) ((1==ntohl(1)) ? (x) : ((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))

#define OpStreamRead 0x05
#define OpOk 0xF0
#define NormalExtentType 1
#define PacketHeaderSize 57
#define ProtoMagic 0xFF
typedef struct packet {
	uint8_t Magic;
	uint8_t ExtentType;
	uint8_t Opcode;
	uint8_t ResultCode;
	uint8_t RemainingFollowers;
	uint32_t CRC;
	uint32_t Size;
	uint32_t ArgLen;
	uint64_t KernelOffset;
	uint64_t PartitionID;
	uint64_t ExtentID;
	uint64_t ExtentOffset;
	int64_t ReqID;
	char *Arg;
	char *Data;
} packet_t;

packet_t *new_read_packet(uint64_t partition_id, uint64_t extent_id, uint64_t extent_offset, char *data, uint32_t size, uint64_t file_offset);
packet_t *new_reply(int64_t req_id, uint64_t partition_id, uint64_t extent_id);
void marshal_header(packet_t *p, char *out);
void unmarshal_header(packet_t *p, char *in);
ssize_t write_sock(int sock_fd, packet_t *p);
ssize_t read_sock(int sock_fd, packet_t *p);
ssize_t get_read_reply(int sock_fd, packet_t *req);
bool check_read_reply(packet_t *req, packet_t *reply);

#endif