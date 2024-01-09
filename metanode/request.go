package metanode

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/proto"
)

const (
	requestInfoBytesLen = 41
)

type RequestInfo struct {
	ReqID              int64  `json:"id"`
	DataCrc            uint32 `json:"crc"`
	RequestTime        int64  `json:"req_time"`
	EnableRemoveDupReq bool   `json:"enable_rm_dupReq"`
	RespCode           uint8  `json:"-"`

	proto.ClientInfo
}

func NewRequestInfo(info proto.ClientInfo, reqID int64, dataCrc uint32, enableState bool) *RequestInfo {
	return &RequestInfo{
		ClientInfo:         info,
		ReqID:              reqID,
		DataCrc:            dataCrc,
		RequestTime:        time.Now().UnixMilli(),
		EnableRemoveDupReq: enableState,
	}
}

func (req *RequestInfo) String() string {
	return fmt.Sprintf("clientID(%v), clientIP(%v), reqID(%v), dataCrc(%v), requestTime(unixTimestamp:%v - time:%s)", req.ClientID,
		req.ClientIP, req.ReqID, req.DataCrc, req.RequestTime, time.UnixMilli(req.RequestTime).Format(proto.TimeFormat))
}

func (req *RequestInfo) Less(than BtreeItem) bool {
	request, ok := than.(*RequestInfo)
	return ok && (req.ClientID < request.ClientID ||
		(req.ClientID == request.ClientID && req.ClientIP < request.ClientIP) ||
		(req.ClientID == request.ClientID && req.ClientIP == request.ClientIP && req.ClientStartTime < req.ClientStartTime) ||
		(req.ClientID == request.ClientID && req.ClientIP == request.ClientIP && req.ClientStartTime == req.ClientStartTime && req.ReqID < request.ReqID) ||
		(req.ClientID == request.ClientID && req.ClientIP == request.ClientIP && req.ClientStartTime == req.ClientStartTime && req.ReqID == request.ReqID && req.DataCrc < request.DataCrc))
}

func (req *RequestInfo) Copy() BtreeItem {
	newReq := &RequestInfo{}
	newReq.ClientID = req.ClientID
	newReq.ClientIP = req.ClientIP
	newReq.ClientStartTime = req.ClientStartTime
	newReq.ReqID = req.ReqID
	newReq.DataCrc = req.DataCrc
	newReq.RequestTime = req.RequestTime
	newReq.RespCode = req.RespCode
	return newReq
}

func (req *RequestInfo) Equal(info *RequestInfo) bool {
	return info.RequestTime == req.RequestTime && info.ClientIP == req.ClientIP && info.ClientID == req.ClientID &&
		info.DataCrc == info.DataCrc && info.ReqID == req.ReqID && info.ClientStartTime == info.ClientStartTime
}

func (req *RequestInfo) MarshalBinary() (data []byte) {
	data = make([]byte, requestInfoBytesLen)
	offset := 0
	binary.BigEndian.PutUint64(data[offset:offset+8], req.ClientID)
	offset += 8
	binary.BigEndian.PutUint64(data[offset:offset+8], uint64(req.ClientStartTime))
	offset += 8
	binary.BigEndian.PutUint64(data[offset:offset+8], uint64(req.ReqID))
	offset += 8
	binary.BigEndian.PutUint32(data[offset:offset+4], req.ClientIP)
	offset += 4
	binary.BigEndian.PutUint32(data[offset:offset+4], req.DataCrc)
	offset += 4
	binary.BigEndian.PutUint64(data[offset:offset+8], uint64(req.RequestTime))
	offset += 8
	data[offset] = req.RespCode
	return data
}

func (req *RequestInfo) Unmarshal(data []byte) (err error) {
	if len(data) != requestInfoBytesLen {
		return fmt.Errorf("error data length:%v", len(data))
	}
	offset := 0
	req.ClientID = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	req.ClientStartTime = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8
	req.ReqID = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8
	req.ClientIP = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	req.DataCrc = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	req.RequestTime = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8
	req.RespCode = data[offset]
	return
}

type RequestInfoBatch []*RequestInfo

func (batchReq RequestInfoBatch) MarshalBinary() (data []byte) {
	data = make([]byte, len(batchReq)*requestInfoBytesLen+4)
	binary.BigEndian.PutUint32(data[:4], uint32(len(batchReq)))
	offset := 4
	for _, reqInfo := range batchReq {
		binary.BigEndian.PutUint64(data[offset:offset+8], reqInfo.ClientID)
		offset += 8
		binary.BigEndian.PutUint64(data[offset:offset+8], uint64(reqInfo.ClientStartTime))
		offset += 8
		binary.BigEndian.PutUint64(data[offset:offset+8], uint64(reqInfo.ReqID))
		offset += 8
		binary.BigEndian.PutUint32(data[offset:offset+4], reqInfo.ClientIP)
		offset += 4
		binary.BigEndian.PutUint32(data[offset:offset+4], reqInfo.DataCrc)
		offset += 4
		binary.BigEndian.PutUint64(data[offset:offset+8], uint64(reqInfo.RequestTime))
		offset += 8
		data[offset] = reqInfo.RespCode
		offset += 1
	}
	return
}

func UnmarshalBatchRequestInfo(data []byte) (batchReq RequestInfoBatch, err error) {
	if len(data) < 4 {
		err = fmt.Errorf("err data length, less than 4")
		return
	}
	requestCount := binary.BigEndian.Uint32(data[:4])
	batchReq = make(RequestInfoBatch, 0, int(requestCount))
	offset := 4
	for index := 0; index < int(requestCount); index++ {
		reqInfo := &RequestInfo{}
		if err = reqInfo.Unmarshal(data[offset : offset+requestInfoBytesLen]); err != nil {
			return
		}
		batchReq = append(batchReq, reqInfo)
		offset += requestInfoBytesLen
	}
	return
}
