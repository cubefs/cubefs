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

type Server struct {
	LocalIp    string
	LocalPort  string
	RemoteIp   string
	RemotePort string
	cListener  unsafe.Pointer
}

type Connection struct {
	cConn unsafe.Pointer
	state int32
	mu    sync.RWMutex
	//recvMsgList *list.List
	dataMap     sync.Map
	recvDataMap sync.Map
	rFd         chan struct{}
	wFd         chan struct{}
	conntype    int
	Ctx         interface{}
	localAddr   *RdmaAddr
	remoteAddr  *RdmaAddr
	TargetIp    string
	TargetPort  string
}

func CbuffToSlice(ptr unsafe.Pointer, length int) []byte {
	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(ptr)
	hdr.Len = int(length)
	hdr.Cap = int(length)
	return buffer
}

func NewRdmaServer(targetIp, targetPort string) (server *Server, err error) { //, memoryPool *MemoryPool, headerPool *ObjectPool, responsePool *ObjectPool
	server = &Server{}
	server.RemoteIp = ""
	server.RemotePort = ""
	server.LocalIp = targetIp
	server.LocalPort = targetPort
	cCtx := C.start_rdma_server_by_addr(C.CString(server.LocalIp), C.CString(server.LocalPort))
	if cCtx == nil {
		return nil, fmt.Errorf("server(%p) start failed", server)
	}
	server.cListener = unsafe.Pointer(cCtx)
	return server, nil
}

func (server *Server) Accept() (*Connection, error) {
	conn := &Connection{}
	cConn := C.get_rdma_server_conn((*C.struct_rdma_listener)(server.cListener))
	if cConn == nil {
		return nil, fmt.Errorf("server(%p) accept failed", server)
	}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTING)
	conn.init(cConn)
	conn.conntype = SERVER_CONN
	conn.Ctx = server
	conn.localAddr = &RdmaAddr{address: C.GoString(&(cConn.local_addr[0])), network: "rdma"}
	conn.remoteAddr = &RdmaAddr{address: C.GoString(&(cConn.remote_addr[0])), network: "rdma"}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	return conn, nil
}

func (server *Server) Close() {
	C.close_rdma_server((*C.struct_rdma_listener)(server.cListener))
	return
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

func (conn *Connection) init(cConn *C.connection) {
	conn.cConn = unsafe.Pointer(cConn)
	//conn.SetDeadline(time.Now().Add(200 * time.Millisecond))
	//C.set_conn_context(cConn, unsafe.Pointer(conn))
	conn.rFd = make(chan struct{}, 100)
	conn.wFd = make(chan struct{}, 100)
}

func (conn *Connection) SetDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED && atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTING {
		return fmt.Errorf("set deadline failed, conn(%p) has been closed", conn)
	}
	C.set_send_timeout_us((*C.connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	C.set_recv_timeout_us((*C.connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	return nil
}

func (conn *Connection) SetWriteDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("set deadline failed, conn(%p) has been closed", conn)
	}
	C.set_send_timeout_us((*C.connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	return nil
}

func (conn *Connection) SetReadDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("set deadline failed, conn(%p) has been closed", conn)
	}
	C.set_recv_timeout_us((*C.connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	return nil
}

func (conn *Connection) Close() (err error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CLOSED {
		C.conn_disconnect((*C.connection)(conn.cConn)) //TODO
		atomic.StoreInt32(&conn.state, CONN_ST_CLOSED)
	}
	return nil
}

func GetDataBuffer(len uint32) ([]byte, error) {
	var bufferSize C.int64_t
	dataPtr := C.get_pool_data_buffer(C.uint32_t(len), &bufferSize)
	dataBuffer := CbuffToSlice(dataPtr, int(bufferSize))
	return dataBuffer, nil
}

func ReleaseDataBuffer(conn *Connection, dataBuffer []byte, size uint32) error {
	if dataBuffer != nil {
		C.release_pool_data_buffer((*C.connection)(conn.cConn), unsafe.Pointer(&dataBuffer[0]), C.uint32_t(size))
	} else {
		C.release_pool_data_buffer((*C.connection)(conn.cConn), unsafe.Pointer(nil), C.uint32_t(size))
	}
	return nil
}

func (conn *Connection) GetConnTxDataBuffer(len uint32) ([]byte, error) {
	dataEntry := C.get_conn_tx_data_buffer((*C.connection)(conn.cConn), C.uint32_t(len))
	if dataEntry == nil {
		return nil, fmt.Errorf("conn(%p) get tx data buffer failed", conn)
	}
	dataBuffer := CbuffToSlice(unsafe.Pointer(dataEntry.addr), int(dataEntry.mem_len))
	conn.dataMap.Store(&dataBuffer[0], dataEntry)
	return dataBuffer, nil
}

func (conn *Connection) ReleaseConnTxDataBuffer(dataBuffer []byte) error {
	entry, ok := conn.dataMap.Load(&dataBuffer[0])
	if !ok {
		return fmt.Errorf("conn(%p) release tx data buffer failed, no such dataEntry", conn)
	}
	dataEntry, ok := entry.(*C.data_entry)
	if !ok {
		return fmt.Errorf("conn(%p) release tx data buffer failed, type convert error", conn)
	}
	C.release_conn_tx_data_buffer((*C.connection)(conn.cConn), (*C.data_entry)(dataEntry))
	conn.dataMap.Delete(&dataBuffer[0])
	return nil
}

func (conn *Connection) Write(data []byte) (int, error) {
	return 0, nil
}

func (conn *Connection) WriteExternalBuffer(data []byte, size int) (int, error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn(%p) has been closed", conn)
	}
	ret := C.conn_app_write_external_buffer((*C.connection)(conn.cConn), unsafe.Pointer(&data[0]), C.uint32_t(size))
	if ret != 0 {
		return -1, fmt.Errorf("conn(%p) write external data buffer failed", conn)
	}
	return size, nil
}

func (conn *Connection) WriteBuffer(data []byte, size int) (int, error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn(%p) has been closed", conn)
	}
	entry, ok := conn.dataMap.Load(&data[0])
	if !ok {
		return -1, fmt.Errorf("conn(%p) write buffer failed, no such dataEntry", conn)
	}
	dataEntry, ok := entry.(*C.data_entry)
	if !ok {
		return -1, fmt.Errorf("conn(%p) write buffer failed, type convert error", conn)
	}
	ret := C.conn_app_write((*C.connection)(conn.cConn), (*C.data_entry)(dataEntry), C.uint32_t(size))
	if ret != 0 {
		return -1, fmt.Errorf("conn(%p) write data buffer failed", conn)
	}
	return size, nil
}

func (conn *Connection) Read([]byte) (int, error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn(%p) has been closed", conn)
	}
	return 0, nil
}

func (conn *Connection) ReleaseConnRxDataBuffer(recvDataBuffer []byte) error {
	entry, ok := conn.recvDataMap.Load(&recvDataBuffer[0])
	if !ok {
		return fmt.Errorf("conn(%p) release rx data buffer failed, no such dataEntry", conn)
	}
	dataEntry, ok := entry.(*C.data_entry)
	if !ok {
		return fmt.Errorf("conn(%p) release rx data buffer failed, type convert error", conn)
	}
	ret := C.release_conn_rx_data_buffer((*C.connection)(conn.cConn), (*C.data_entry)(dataEntry))
	conn.recvDataMap.Delete(&recvDataBuffer[0])
	if ret == 1 {
		return fmt.Errorf("conn(%p) release rx data buffer failed", conn)
	}
	return nil
}

func (conn *Connection) GetRecvMsgBuffer() ([]byte, error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil, fmt.Errorf("conn(%p) has been closed", conn)
	}
	dataEntry := C.get_recv_msg_buffer((*C.connection)(conn.cConn))
	if dataEntry == nil {
		return nil, fmt.Errorf("conn(%p) get recv msg failed", conn)
	}
	recvDataBuffer := CbuffToSlice(unsafe.Pointer(dataEntry.addr), int(dataEntry.data_len))
	conn.recvDataMap.Store(&recvDataBuffer[0], dataEntry)
	return recvDataBuffer, nil
}

type RdmaEnvConfig struct {
	RdmaPort      string
	MemBlockNum   int
	MemBlockSize  int
	MemPoolLevel  int
	ConnDataSize  int
	WqDepth       int
	MinCqeNum     int
	EnableRdmaLog bool
	RdmaLogDir    string
	WorkerNum     int
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
	if gCfg.EnableRdmaLog {
		cCfg.enable_rdma_log = C.int(1)
	}
	if gCfg.RdmaLogDir != "" {
		cCfg.rdma_log_dir = C.CString(gCfg.RdmaLogDir)
	}
	if gCfg.WorkerNum != 0 {
		cCfg.worker_num = C.int(gCfg.WorkerNum)
	}
	return nil
}

func InitPool(cfg *RdmaEnvConfig) error {
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

func DestroyPool() {
	C.destroy_rdma_env()
}
