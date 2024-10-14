package rdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm -lrt -lpthread
#cgo CFLAGS: -std=gnu99 -g
#include "client.h"
#include "server.h"
*/
import "C"
import (
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cubefs/cubefs/util/log"
)

const (
	CONN_ST_CLOSED     = 1
	CONN_ST_CONNECTING = 2
	CONN_ST_CONNECTED  = 3

	CLIENT_CONN = 4
	SERVER_CONN = 5
)

type RdmaAddr struct {
	network string
	address string
}

type RdmaBuffer struct {
	Data      []byte
	lkey      uint32
	Len       int
	dataEntry unsafe.Pointer
}

type Server struct {
	LocalIp    string
	LocalPort  string
	RemoteIp   string
	RemotePort string
	addr       *RdmaAddr
	cListener  unsafe.Pointer
}

type Connection struct {
	cConn       unsafe.Pointer
	state       int32
	mu          sync.RWMutex
	dataMap     sync.Map
	recvDataMap sync.Map
	conntype    int
	Ctx         interface{}
	localAddr   *RdmaAddr
	remoteAddr  *RdmaAddr
	TargetIp    string
	TargetPort  string
}

func (conn *Connection) GetCCon() unsafe.Pointer {
	return conn.cConn
}

func CbuffToSlice(ptr unsafe.Pointer, length int) []byte {
	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(ptr)
	hdr.Len = int(length)
	hdr.Cap = int(length)
	return buffer
}

func NewRdmaServer(targetIp, targetPort string) (server *Server, err error) {
	server = &Server{}
	server.RemoteIp = ""
	server.RemotePort = ""
	server.LocalIp = targetIp
	server.LocalPort = targetPort
	server.addr = &RdmaAddr{address: targetIp + targetPort, network: "rdma"}
	cCtx := C.start_rdma_server_by_addr(C.CString(server.LocalIp), C.CString(server.LocalPort))
	if cCtx == nil {
		return nil, fmt.Errorf("server(%p) start failed", server)
	}
	server.cListener = unsafe.Pointer(cCtx)
	return server, nil
}

func (server *Server) Accept() (net.Conn, error) {
	conn := &Connection{}
	cConn := C.get_rdma_server_conn((*C.struct_rdma_listener)(server.cListener))
	if cConn == nil {
		return nil, fmt.Errorf("server(%p) accept failed", server.cListener)
	}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTING)
	conn.init(cConn)

	conn.conntype = SERVER_CONN
	conn.Ctx = server
	conn.localAddr = &RdmaAddr{address: C.GoString(&(cConn.local_addr[0])), network: "rdma"}
	conn.remoteAddr = &RdmaAddr{address: C.GoString(&(cConn.remote_addr[0])), network: "rdma"}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	log.LogDebugf("server(%p) accept conn(%p)", server.cListener, cConn)
	return conn, nil
}

func (server *Server) Close() error {
	C.close_rdma_server((*C.struct_rdma_listener)(server.cListener))
	return nil
}

func (server *Server) Addr() net.Addr {
	return server.addr
}

func (conn *Connection) Dial(targetIp, targetPort string) error {
	cConn := C.rdma_connect_by_addr(C.CString(targetIp), C.CString(targetPort))
	if cConn == nil {
		return fmt.Errorf("conn(%p) dial failed", conn)
	}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTING)
	conn.init(cConn)
	conn.conntype = CLIENT_CONN
	conn.localAddr = &RdmaAddr{address: C.GoString(&(cConn.local_addr[0])), network: "rdma"}
	conn.remoteAddr = &RdmaAddr{address: C.GoString(&(cConn.remote_addr[0])), network: "rdma"}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	return nil
}

func (conn *Connection) DialTimeout(targetIp, targetPort string, timeout time.Duration) error {
	cConn := C.rdma_connect_by_addr_with_timeout(C.CString(targetIp), C.CString(targetPort), C.int64_t(timeout.Nanoseconds()))
	if cConn == nil {
		return fmt.Errorf("conn(%p) dialTimeout failed", conn)
	}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTING)
	conn.init(cConn)
	conn.conntype = CLIENT_CONN
	conn.localAddr = &RdmaAddr{address: C.GoString(&(cConn.local_addr[0])), network: "rdma"}
	conn.remoteAddr = &RdmaAddr{address: C.GoString(&(cConn.remote_addr[0])), network: "rdma"}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	return nil
}

func (rdmaAddr *RdmaAddr) Network() string {
	return rdmaAddr.network
}

func (rdmaAddr *RdmaAddr) String() string {
	return rdmaAddr.address
}

func (conn *Connection) LocalAddr() net.Addr {
	return conn.localAddr
}

func (conn *Connection) RemoteAddr() net.Addr {
	return conn.remoteAddr
}

func (conn *Connection) SetLoopExchange() {
	C.set_loop_exchange((*C.connection)(conn.cConn))
}

func (conn *Connection) init(cConn *C.connection) {
	conn.cConn = unsafe.Pointer(cConn)
}

func (conn *Connection) SetDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED && atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTING {
		return fmt.Errorf("set deadline failed, conn(%p) has been closed", conn.cConn)
	}
	if t.IsZero() {
		C.set_send_timeout_ns((*C.connection)(conn.cConn), -1)
		C.set_recv_timeout_ns((*C.connection)(conn.cConn), -1)
	} else {
		C.set_send_timeout_ns((*C.connection)(conn.cConn), C.int64_t(t.UnixNano()-time.Now().UnixNano()))
		C.set_recv_timeout_ns((*C.connection)(conn.cConn), C.int64_t(t.UnixNano()-time.Now().UnixNano()))
	}
	return nil
}

func (conn *Connection) SetWriteDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("set write deadline failed, conn(%p) has been closed", conn.cConn)
	}
	if t.IsZero() {
		C.set_send_timeout_ns((*C.connection)(conn.cConn), -1)
	} else {
		C.set_send_timeout_ns((*C.connection)(conn.cConn), C.int64_t(t.UnixNano()-time.Now().UnixNano()))
	}
	return nil
}

func (conn *Connection) SetReadDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("set read deadline failed, conn(%p) has been closed", conn.cConn)
	}
	if t.IsZero() {
		C.set_recv_timeout_ns((*C.connection)(conn.cConn), -1)
	} else {
		C.set_recv_timeout_ns((*C.connection)(conn.cConn), C.int64_t(t.UnixNano()-time.Now().UnixNano()))
	}
	return nil
}

func (conn *Connection) Close() error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CLOSED {
		atomic.StoreInt32(&conn.state, CONN_ST_CLOSED)
		C.conn_disconnect((*C.connection)(conn.cConn))
		conn.dataMap.Range(func(key, value interface{}) bool {
			conn.dataMap.Delete(key)
			return true
		})
		conn.recvDataMap.Range(func(key, value interface{}) bool {
			conn.recvDataMap.Delete(key)
			return true
		})
	}
	return nil
}

func GetDataBuffer(len uint32) (*RdmaBuffer, error) {
	dataEntry := C.get_pool_data_buffer(C.uint32_t(len))
	if dataEntry == nil {
		return nil, fmt.Errorf("get data buffer failed, rdma pool memory resource exhausted")
	}
	dataBuffer := CbuffToSlice(unsafe.Pointer(dataEntry.addr), int(dataEntry.mem_len))
	rdmaBuffer := &RdmaBuffer{Data: dataBuffer, dataEntry: unsafe.Pointer(dataEntry), lkey: uint32(dataEntry.lkey), Len: int(dataEntry.mem_len)}
	log.LogDebugf("getDataBuffer(%p)", dataBuffer)
	return rdmaBuffer, nil
}

func ReleaseDataBuffer(rdmaBuffer *RdmaBuffer) {
	log.LogDebugf("releaseDataBuffer(%p)", rdmaBuffer.Data)
	C.release_pool_data_buffer((*C.data_entry)(rdmaBuffer.dataEntry))
	return
}

func (conn *Connection) GetConnTxDataBuffer(len uint32) (*RdmaBuffer, error) {
	dataEntry := C.get_conn_tx_data_buffer((*C.connection)(conn.cConn), C.uint32_t(len))
	if dataEntry == nil {
		return nil, fmt.Errorf("conn(%p) get tx data buffer failed", conn.cConn)
	}
	dataBuffer := CbuffToSlice(unsafe.Pointer(dataEntry.addr), int(dataEntry.mem_len))
	rdmaBuffer := &RdmaBuffer{Data: dataBuffer, dataEntry: unsafe.Pointer(dataEntry), Len: int(dataEntry.mem_len)}
	conn.dataMap.Store(rdmaBuffer, dataEntry)
	log.LogDebugf("conn(%p) getConnTxDataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	return rdmaBuffer, nil
}

func (conn *Connection) ReleaseConnTxDataBuffer(rdmaBuffer *RdmaBuffer) error { //rdma todo
	entry, ok := conn.dataMap.Load(rdmaBuffer)
	if !ok {
		return fmt.Errorf("conn(%p) release tx data buffer failed, no such dataEntry", conn.cConn)
	}
	dataEntry, ok := entry.(*C.data_entry)
	if !ok {
		return fmt.Errorf("conn(%p) release tx data buffer failed, type convert error", conn.cConn)
	}
	log.LogDebugf("conn(%p) releaseConnTxDataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	ret := C.release_conn_tx_data_buffer((*C.connection)(conn.cConn), (*C.data_entry)(dataEntry))
	conn.dataMap.Delete(rdmaBuffer)
	if ret == 1 {
		return fmt.Errorf("conn(%p) release tx data buffer failed", conn.cConn)
	}
	return nil
}

func (conn *Connection) Write(data []byte) (int, error) {
	return 0, nil
}

func (conn *Connection) WriteExternalBuffer(rdmaBuffer *RdmaBuffer, size int) (int, error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn(%p) has been closed", conn.cConn)
	}
	dataEntry := C.get_conn_tx_data_buffer((*C.connection)(conn.cConn), C.uint32_t(size))
	if dataEntry == nil {
		return -1, fmt.Errorf("conn(%p) get tx data buffer failed", conn.cConn)
	}
	log.LogDebugf("conn(%p) getConnTxDataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	conn.dataMap.Store(rdmaBuffer, dataEntry)
	ret := C.conn_app_write_external_buffer((*C.connection)(conn.cConn), unsafe.Pointer(&rdmaBuffer.Data[0]), dataEntry, C.uint32_t(rdmaBuffer.lkey), C.uint32_t(size))
	if ret != 0 {
		return -1, fmt.Errorf("conn(%p) write external data buffer failed: dataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	}
	log.LogDebugf("conn(%p) write external data buffer: dataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	return size, nil
}

func (conn *Connection) ReleaseConnExternalDataBuffer(rdmaBuffer *RdmaBuffer) error { //rdma todo
	entry, ok := conn.dataMap.Load(rdmaBuffer)
	if !ok {
		return fmt.Errorf("conn(%p) release external data buffer failed, no such dataEntry", conn.cConn)
	}
	dataEntry, ok := entry.(*C.data_entry)
	if !ok {
		return fmt.Errorf("conn(%p) release external data buffer failed, type convert error", conn.cConn)
	}
	log.LogDebugf("conn(%p) releaseConnExternalDataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	ret := C.release_conn_tx_data_buffer((*C.connection)(conn.cConn), (*C.data_entry)(dataEntry))
	conn.dataMap.Delete(rdmaBuffer)
	if ret == 1 {
		return fmt.Errorf("conn(%p) release external data buffer failed", conn.cConn)
	}
	return nil
}

func (conn *Connection) WriteBuffer(rdmaBuffer *RdmaBuffer) (int, error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn(%p) has been closed", conn.cConn)
	}
	entry, ok := conn.dataMap.Load(rdmaBuffer)
	if !ok {
		return -1, fmt.Errorf("conn(%p) write buffer failed, no such dataEntry", conn.cConn)
	}
	dataEntry, ok := entry.(*C.data_entry)
	if !ok {
		return -1, fmt.Errorf("conn(%p) write buffer failed, type convert error", conn.cConn)
	}
	ret := C.conn_app_write((*C.connection)(conn.cConn), (*C.data_entry)(dataEntry))
	if ret != 0 {
		return -1, fmt.Errorf("conn(%p) write data buffer failed, dataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	}
	log.LogDebugf("conn(%p) write data buffer: dataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	return rdmaBuffer.Len, nil
}

func (conn *Connection) AddWriteRequest(rdmaBuffer *RdmaBuffer) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("conn(%p) has been closed", conn.cConn)
	}
	entry, ok := conn.dataMap.Load(rdmaBuffer)
	if !ok {
		return fmt.Errorf("conn(%p) add write sge failed, no such dataEntry", conn.cConn)
	}
	dataEntry, ok := entry.(*C.data_entry)
	if !ok {
		return fmt.Errorf("conn(%p) add write sge failed, type convert error", conn.cConn)
	}
	ret := C.conn_add_write_request((*C.connection)(conn.cConn), (*C.data_entry)(dataEntry))
	if ret != 0 {
		return fmt.Errorf("conn(%p) add write sge failed: dataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	}
	log.LogDebugf("conn(%p) add write sge: dataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	conn.dataMap.Delete(rdmaBuffer)
	return nil
}

func (conn *Connection) FlushWriteRequest() error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("conn(%p) has been closed", conn.cConn)
	}
	ret := C.conn_flush_write_request((*C.connection)(conn.cConn))
	if ret != 0 {
		return fmt.Errorf("conn(%p) flush write sge failed", conn.cConn)
	}
	return nil
}

func (conn *Connection) Read([]byte) (int, error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn(%p) has been closed", conn.cConn)
	}
	return 0, nil
}

func (conn *Connection) ReleaseConnRxDataBuffer(rdmaBuffer *RdmaBuffer) error { //rdma todo
	entry, ok := conn.recvDataMap.Load(rdmaBuffer)
	if !ok {
		return fmt.Errorf("conn(%p) release rx data buffer failed, no such dataEntry", conn.cConn)
	}
	dataEntry, ok := entry.(*C.data_entry)
	if !ok {
		return fmt.Errorf("conn(%p) release rx data buffer failed, type convert error", conn.cConn)
	}
	log.LogDebugf("conn(%p) releaseConnRxDataBuffer(%v) len(%v)", conn.cConn, dataEntry.addr, dataEntry.mem_len)
	ret := C.release_conn_rx_data_buffer((*C.connection)(conn.cConn), (*C.data_entry)(dataEntry))
	conn.recvDataMap.Delete(rdmaBuffer)
	if ret == 1 {
		return fmt.Errorf("conn(%p) release rx data buffer failed", conn.cConn)
	}
	return nil
}

func (conn *Connection) GetRecvMsgBuffer() (*RdmaBuffer, error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil, fmt.Errorf("conn(%p) has been closed", conn.cConn)
	}
	dataEntry := C.get_recv_msg_buffer((*C.connection)(conn.cConn))
	if dataEntry == nil {
		return nil, fmt.Errorf("conn(%p) get recv msg failed", conn.cConn)
	}
	recvDataBuffer := CbuffToSlice(unsafe.Pointer(dataEntry.addr), int(dataEntry.mem_len))
	rdmaBuffer := &RdmaBuffer{Data: recvDataBuffer, dataEntry: unsafe.Pointer(dataEntry), lkey: uint32(dataEntry.lkey), Len: int(dataEntry.mem_len)}
	conn.recvDataMap.Store(rdmaBuffer, dataEntry)
	log.LogDebugf("rdma conn(%p) get recv msg: entry(%p) addr(%p) addr(%p)", conn.cConn, dataEntry, dataEntry.addr, &recvDataBuffer[0])
	return rdmaBuffer, nil
}

type RdmaEnvConfig struct {
	RdmaDpPort   string
	RdmaMpPort   string
	MemBlockNum  int
	MemBlockSize int
	MemPoolLevel int
	ConnDataSize int
	WqDepth      int
	MinCqeNum    int
	RdmaLogLevel string
	RdmaLogFile  string
	WorkerNum    int
}

func GetRdmaLogLevel(val string) (ret C.int) {
	switch val {
	case "trace", "TRACE", "Trace":
		ret = 0
	case "debug", "DEBUG", "Debug":
		ret = 1
	case "info", "INFO", "Info":
		ret = 2
	case "warn", "WARN", "Warn":
		ret = 3
	case "error", "ERROR", "Error":
		ret = 4
	case "fatal", "FATAL", "Fatal":
		ret = 5
	default:
		ret = 2
	}
	return ret
}

func parseRdmaEnvConfig(gCfg *RdmaEnvConfig, cCfg *C.struct_rdma_env_config) error {
	if cCfg == nil {
		return fmt.Errorf("gCfg parse to cCfg failed: cCfg is NULL")
	}
	if gCfg == nil {
		return nil
	}
	if gCfg.MemBlockNum != 0 {
		cCfg.mem_block_num = C.int(gCfg.MemBlockNum)
	}
	if gCfg.MemBlockSize != 0 {
		cCfg.mem_block_size = C.int(gCfg.MemBlockSize)
	}
	if gCfg.MemPoolLevel != 0 {
		cCfg.mem_pool_level = C.int(gCfg.MemPoolLevel)
	}
	if gCfg.ConnDataSize != 0 {
		cCfg.conn_data_size = C.int(gCfg.ConnDataSize)
	}
	if gCfg.WqDepth != 0 {
		cCfg.wq_depth = C.int(gCfg.WqDepth)
	}
	if gCfg.MinCqeNum != 0 {
		cCfg.min_cqe_num = C.int(gCfg.MinCqeNum)
	}
	cCfg.rdma_log_level = GetRdmaLogLevel(gCfg.RdmaLogLevel)
	if gCfg.RdmaLogFile == "" {
		gCfg.RdmaLogFile = "/home/service/rdma.log"
	}
	C.set_rdma_log_file(cCfg, C.CString(gCfg.RdmaLogFile))
	if gCfg.WorkerNum != 0 {
		cCfg.worker_num = C.int(gCfg.WorkerNum)
	}

	return nil
}

func InitEnv(cfg *RdmaEnvConfig) error {
	cCfg := C.get_rdma_env_config()
	if err := parseRdmaEnvConfig(cfg, cCfg); err != nil {
		return err
	}
	ret := int(C.init_rdma_env(cCfg))
	if ret != 0 {
		return fmt.Errorf("init pool failed")
	}
	return nil
}

func DestroyEnv() {
	C.destroy_rdma_env()
}
