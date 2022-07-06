#include <packet.h>

packet_t *new_read_packet(uint64_t partition_id, uint64_t extent_id, uint64_t extent_offset, char *data, uint32_t size, uint64_t file_offset) {
	packet_t *p = (packet_t *)malloc(sizeof(packet_t));
	if(p == NULL) {
		return NULL;
	}
	memset(p, 0, sizeof(packet_t));
	p->Magic = ProtoMagic;
	p->PartitionID = partition_id;
	p->ExtentID = extent_id;
	p->ExtentOffset = extent_offset;
	p->Size = size;
	p->Opcode = OpStreamRead;
	p->ExtentType = NormalExtentType;
	p->ReqID = 0;
	p->RemainingFollowers = 0;
	p->KernelOffset = file_offset;
	p->Data = data;
	return p;
}

packet_t *new_reply(int64_t req_id, uint64_t partition_id, uint64_t extent_id) {
	packet_t *p = (packet_t *)malloc(sizeof(packet_t));
	if(p == NULL) {
		return NULL;
	}
	memset(p, 0, sizeof(packet_t));
	p->ReqID = req_id;
	p->PartitionID = partition_id;
	p->ExtentID = extent_id;
	p->Magic = ProtoMagic;
	p->ExtentType = NormalExtentType;
	p->ReqID = 0;
	return p;
}

void marshal_header(packet_t *p, char *out) {
	out[0] = p->Magic;
	out[1] = p->ExtentType;
	out[2] = p->Opcode;
	out[3] = p->ResultCode;
	out[4] = p->RemainingFollowers;
	*(uint32_t *)(out + 5) = htonl(p->CRC);
    *(uint32_t *)(out + 9) = htonl(p->Size);
    *(uint32_t *)(out + 13) = htonl(p->ArgLen);
    *(uint64_t *)(out + 17) = htonll(p->PartitionID);
    *(uint64_t *)(out + 25) = htonll(p->ExtentID);
    *(uint64_t *)(out + 33) = htonll(p->ExtentOffset);
    *(int64_t *)(out + 41) = htonll(p->ReqID);
    *(uint64_t *)(out + 49) = htonll(p->KernelOffset);
}

void unmarshal_header(packet_t *p, char *in) {
	p->Magic = in[0];
	if(p->Magic != ProtoMagic) {
		return;
	}
	p->ExtentType = in[1];
	p->Opcode = in[2];
	p->ResultCode = in[3];
	p->RemainingFollowers = in[4];
	p->CRC = ntohl(*(uint32_t *)(in + 5));
	p->Size = ntohl(*(uint32_t *)(in + 9));
	p->ArgLen = ntohl(*(uint32_t *)(in + 13));
	p->PartitionID = ntohll(*(uint64_t *)(in + 17));
	p->ExtentID = ntohll(*(uint64_t *)(in + 25));
	p->ExtentOffset = ntohll(*(uint64_t *)(in + 33));
	p->ReqID = ntohll(*(int64_t *)(in + 41));
	p->KernelOffset = ntohll(*(uint64_t *)(in + 49));
}

int new_connection(const char *ip, int port) {
    int sock_fd = -1, client_fd;
	if(ip == NULL) {
		return sock_fd;
	}
    if((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return sock_fd;
    }
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if(inet_pton(AF_INET, ip, &addr.sin_addr) < 0) {
		close(sock_fd);
        return sock_fd;
    }
    if((client_fd = connect(sock_fd, (struct sockaddr*)&addr, sizeof(addr)))) {
		close(sock_fd);
        return sock_fd;
    }
    return sock_fd;
}

ssize_t write_sock(int sock_fd, packet_t *p) {
    char *header = (char *)malloc(PacketHeaderSize);
	if(header == NULL) {
		return -1;
	}
	marshal_header(p, header);
	ssize_t re = send(sock_fd, header, PacketHeaderSize, 0);
	free(header);
	return re;
}

ssize_t read_sock(int sock_fd, packet_t *p) {
	char *header = (char *)malloc(PacketHeaderSize);
	if(header == NULL) {
		return -1;
	}
	ssize_t re = recv(sock_fd, header, PacketHeaderSize, MSG_WAITALL);
	if(re < 0) {
		free(header);
		return re;
	}
	unmarshal_header(p, header);
	free(header);
	if(p->Size <= 0) {
		return re;
	}
	re = recv(sock_fd, p->Data, p->Size, MSG_WAITALL);
	return re;
}

ssize_t get_read_reply(int sock_fd, packet_t *req) {
	ssize_t read = 0;
	while(read < req->Size) {
		packet_t *reply = new_reply(req->ReqID, req->PartitionID, req->ExtentID);
		int size = req->Size - read;
		if(size > ReadBlockSize) {
			size = ReadBlockSize;
		}
		reply->Data = req->Data + read;
		reply->Size = size;
		ssize_t re = read_sock(sock_fd, reply);
		bool valid = check_read_reply(req, reply);
		free(reply);
		if(re < 0 || !valid) {
			return read;
		}
		read += size;
	}
	return read;
}

bool check_read_reply(packet_t *req, packet_t *reply) {
	if(reply->ResultCode != OpOk) {
		return false;
	}
	if(req->ReqID != reply->ReqID || req->PartitionID != reply->PartitionID || req->ExtentID != reply->ExtentID) {
		return false;
	}
	return true;
}
