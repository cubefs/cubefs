package rdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm -lrt -pthread
#include "client.h"
#include "server.h"
#include "rdma.h"
#include "rdma_pool.h"
*/
import "C"
import (
	"container/list"
	errors2 "errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	CONN_ST_CLOSING    = 0
	CONN_ST_CLOSED     = 1
	CONN_ST_CONNECTING = 2
	CONN_ST_CONNECTED  = 3

	//BUFFER_HEADER = 4
	//BUFFER_DATA = 5
	//BUFFER_RESPONSE = 6

	CLIENT_CONN = 7
	SERVER_CONN = 8
)

//export PrintCallback
func PrintCallback(cstr *C.char) {
	str := C.GoString(cstr)
	println(str)
}

//export EpollAddSendAndRecvEvent
func EpollAddSendAndRecvEvent(fd C.int, ctx unsafe.Pointer) {
	GetEpoll().EpollAdd(int(fd), func() {
		if ok := C.transport_sendAndRecv_event_cb(ctx); ok == 0 {
		}
	})
}

//export EpollAddConnectEvent
func EpollAddConnectEvent(fd C.int, ctx unsafe.Pointer) {
	GetEpoll().EpollAdd(int(fd), func() {
		if ok := C.connection_event_cb(unsafe.Pointer(ctx)); ok == 0 {
		}
	})
}

//export EpollDelConnEvent
func EpollDelConnEvent(fd C.int) {
	GetEpoll().EpollDel(int(fd))
}

//export RecvHeaderCallback
func RecvHeaderCallback(header unsafe.Pointer, len C.int, fielaName *C.char) C.int {
	return C.int(145)
}

//export RecvMessageCallback
func RecvMessageCallback(ctx unsafe.Pointer, entry *C.MemoryEntry) {
	conn := (*Connection)(ctx)
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return
	}
	if !entry.isResponse {
		recvHeaderBuff := CbuffToSlice(entry.header_buff, int(entry.header_len))
		recvDataBuff := CbuffToSlice(entry.data_buff, int(entry.data_len))
		conn.OnRecvCB(recvHeaderBuff, recvDataBuff)
	} else {
		recvResponseBuff := CbuffToSlice(entry.response_buff, int(entry.response_len))
		conn.OnRecvCB(recvResponseBuff)
	}
}

//export DisConnectCallback
func DisConnectCallback(ctx unsafe.Pointer) {
	conn := (*Connection)(ctx)
	atomic.StoreInt32(&conn.state, CONN_ST_CLOSING)
	conn.rFd <- struct{}{}
}

func CbuffToSlice(ptr unsafe.Pointer, length int) []byte {
	var buffer []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(ptr)
	hdr.Len = int(length)
	hdr.Cap = int(length)
	return buffer
}

type Server struct {
	LocalIp    string
	LocalPort  string
	RemoteIp   string
	RemotePort string
	cListener  unsafe.Pointer
}

func NewRdmaServer(targetIp, targetPort string) (server *Server, err error) { //, memoryPool *MemoryPool, headerPool *ObjectPool, responsePool *ObjectPool
	server = &Server{}
	server.RemoteIp = ""
	server.RemotePort = ""
	server.LocalIp = targetIp
	server.LocalPort = targetPort
	nPort, _ := strconv.Atoi(server.LocalPort)
	serverAddr := strings.Join([]string{server.LocalIp, server.LocalPort}, ":")
	cCtx := C.StartServer(C.CString(server.LocalIp), C.ushort(nPort), C.CString(serverAddr))
	if cCtx == nil {
		return nil, errors2.New("server start failed")
	}
	server.cListener = unsafe.Pointer(cCtx)
	return server, nil
}

func (server *Server) Accept() *Connection {
	conn := &Connection{}
	cConn := C.getServerConn((*C.struct_RdmaListener)(server.cListener))
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTING)
	conn.init(cConn)
	conn.conntype = SERVER_CONN
	conn.Ctx = server
	conn.localAddr = &RdmaAddr{address: C.GoString(&(cConn.local_addr[0])), network: "rdma"}
	conn.remoteAddr = &RdmaAddr{address: C.GoString(&(cConn.remote_addr[0])), network: "rdma"}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	return conn
}

func (server *Server) Close() (err error) {
	ret := C.CloseServer((*C.struct_RdmaListener)(server.cListener))
	if ret != 1 {
		err = errors2.New("server free failed")
		return err
	}
	return
}

type Client struct {
	LocalIp       string
	LocalPort     string
	RemoteIp      string
	RemotePort    string
	cClient       unsafe.Pointer
	reConnectFlag bool
}

func NewRdmaClient(targetIp, targetPort string) (client *Client, err error) { //, memoryPool *MemoryPool, headerPool *ObjectPool, responsePool *ObjectPool
	client = &Client{}
	client.RemoteIp = targetIp
	client.RemotePort = targetPort
	targetAddr := strings.Join([]string{targetIp, targetPort}, ":")
	cCtx := C.Connect(C.CString(client.RemoteIp), C.CString(client.RemotePort), C.CString(targetAddr))
	if cCtx == nil {
		err = errors2.New("client connect failed")
		return nil, err
	}
	client.cClient = unsafe.Pointer(cCtx)
	return client, nil
}

func (client *Client) Dial() (conn *Connection) {
	conn = &Connection{}
	cConn := C.getClientConn((*C.struct_RdmaContext)(client.cClient))
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTING)
	conn.init(cConn)
	conn.conntype = CLIENT_CONN
	conn.Ctx = client
	conn.localAddr = &RdmaAddr{address: C.GoString(&(cConn.local_addr[0])), network: "rdma"}
	conn.remoteAddr = &RdmaAddr{address: C.GoString(&(cConn.remote_addr[0])), network: "rdma"}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	return conn
}

func (client *Client) Close() (err error) {
	ret := C.CloseClient((*C.struct_RdmaContext)(client.cClient))
	if ret != 1 {
		err = errors2.New("client free failed")
		return err
	}
	return nil
}

type RecvMsg struct {
	headerPtr   []byte
	dataPtr     []byte
	responsePtr []byte
}

type Connection struct {
	cConn       unsafe.Pointer
	state       int32
	mu          sync.RWMutex
	recvMsgList *list.List
	rFd         chan struct{}
	wFd         chan struct{}
	conntype    int
	Ctx         interface{}
	localAddr   *RdmaAddr
	remoteAddr  *RdmaAddr
}

type RdmaAddr struct {
	network string
	address string
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

func (conn *Connection) init(cConn *C.Connection) {
	conn.cConn = unsafe.Pointer(cConn)
	conn.SetDeadline(time.Now().Add(50 * time.Millisecond))
	C.setConnContext(cConn, unsafe.Pointer(conn))
	conn.rFd = make(chan struct{}, 100)
	conn.wFd = make(chan struct{}, 100)
	conn.recvMsgList = list.New()
}

func (conn *Connection) SetDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED && atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTING {
		return fmt.Errorf("set deadline failed, conn has been closed")
	}
	C.setSendTimeoutUs((*C.Connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	C.setRecvTimeoutUs((*C.Connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	return nil
}

func (conn *Connection) SetWriteDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("set deadline failed, conn has been closed")
	}
	C.setSendTimeoutUs((*C.Connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	return nil
}

func (conn *Connection) SetReadDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("set deadline failed, conn has been closed")
	}
	C.setRecvTimeoutUs((*C.Connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	return nil
}

func (conn *Connection) OnRecvCB(buffs ...[]byte) {
	conn.mu.Lock()
	if len(buffs) == 2 {
		conn.recvMsgList.PushBack(&RecvMsg{buffs[0], buffs[1], nil})
	} else {
		conn.recvMsgList.PushBack(&RecvMsg{nil, nil, buffs[0]})
	}
	conn.mu.Unlock()
	conn.rFd <- struct{}{}
	return
}

func (conn *Connection) OnSendCB(sendLen int) {
	conn.wFd <- struct{}{}
	return
}

func (conn *Connection) ReConnect() error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CLOSED {
		return fmt.Errorf("conn has not been closed")
	}
	if ret := int(C.ReConnect((*C.Connection)(conn.cConn))); ret == 0 {
		return fmt.Errorf("conn reconnect failed")
	}
	conn.rFd = make(chan struct{}, 100)
	conn.wFd = make(chan struct{}, 100)
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	return nil
}

func (conn *Connection) Close() (err error) {
	C.DisConnect((*C.Connection)(conn.cConn), false)
	if conn.rFd != nil {
		conn.rFd <- struct{}{}
	}
	if conn.wFd != nil {
		conn.wFd <- struct{}{}
	}

	if conn.recvMsgList != nil {
		for elem := conn.recvMsgList.Front(); elem != nil; elem = elem.Next() {
			msg := elem.Value.(*RecvMsg)
			if msg.headerPtr != nil {
				if err := conn.RdmaPostRecvHeader(msg.headerPtr); err != nil {
				}
				if msg.dataPtr != nil {
					ReleaseDataBuffer(msg.dataPtr)
				}
			} else {
				if msg.responsePtr != nil {
					if err := conn.RdmaPostRecvResponse(msg.responsePtr); err != nil {
					}
				}
			}

		}
		conn.recvMsgList.Init()
	}
	atomic.StoreInt32(&conn.state, CONN_ST_CLOSED)
	return nil
}

func GetDataBuffer(len uint32, timeout_us int) ([]byte, error) {
	var bufferSize C.int64_t

	dataPtr := C.getDataBuffer(C.uint32_t(len), C.int64_t(timeout_us), &bufferSize)
	if bufferSize == 0 {
		return nil, fmt.Errorf("get data buffer timeout")
	}
	if bufferSize == -1 {
		return nil, fmt.Errorf("get data buffer failed, memoery pool has been closed")
	}
	dataBuffer := CbuffToSlice(dataPtr, int(bufferSize))
	return dataBuffer, nil
}

func ReleaseDataBuffer(dataBuffer []byte) error {
	if int(C.releaseDataBuffer(unsafe.Pointer(&dataBuffer[0]))) == 0 {
		return fmt.Errorf("release data buffer(%p) failed", dataBuffer)
	}
	return nil
}

func (conn *Connection) GetHeaderBuffer(timeout_us int) ([]byte, error) {
	var bufferSize C.int32_t
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil, fmt.Errorf("conn(%p) has been closed", conn)
	}
	buffPtr := C.getHeaderBuffer((*C.Connection)(conn.cConn), C.int64_t(timeout_us), &bufferSize)
	if bufferSize == 0 {
		return nil, fmt.Errorf("conn(%p) get header buffer timeout", conn)
	}
	if bufferSize == -1 {
		return nil, fmt.Errorf("conn(%p) get header buffer failed, conn has been closed", conn)
	}
	headerBuffer := CbuffToSlice(buffPtr, GetHeaderSize())
	return headerBuffer, nil
}

func (conn *Connection) ReleaseHeaderBuffer(headerBuffer []byte) error {
	if int(C.releaseHeaderBuffer((*C.Connection)(conn.cConn), unsafe.Pointer(&headerBuffer[0]))) == 0 {
		return fmt.Errorf("client conn(%p) release header buffer(%p) failed", conn, headerBuffer)
	}
	return nil
}

func (conn *Connection) GetResponseBuffer(timeout_us int) ([]byte, error) { //*Buffer
	var bufferSize C.int32_t
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil, fmt.Errorf("conn(%p) has been closed", conn)
	}
	buffPtr := C.getResponseBuffer((*C.Connection)(conn.cConn), C.int64_t(timeout_us), &bufferSize)
	if bufferSize == 0 {
		return nil, fmt.Errorf("conn(%p) get response buffer timeout", conn)
	}
	if bufferSize == -1 {
		return nil, fmt.Errorf("conn(%p) get response buffer failed, conn has been closed", conn)
	}
	respBuffer := CbuffToSlice(buffPtr, GetResponseSize())
	return respBuffer, nil
}

func (conn *Connection) ReleaseResponseBuffer(responseBuffer []byte) error {
	if int(C.releaseResponseBuffer((*C.Connection)(conn.cConn), unsafe.Pointer(&responseBuffer[0]))) == 0 {
		return fmt.Errorf("server conn(%p) release response buffer(%p) failed", conn, responseBuffer)
	}
	return nil
}

func (conn *Connection) Write(data []byte) (int, error) {
	return 0, nil
}

func (conn *Connection) WriteBuffer(headerBuffer []byte, dataBuffer []byte, dataBufferSize int) (int, error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn(%p) has been closed", conn)
	}
	ret := C.connAppWrite((*C.Connection)(conn.cConn), unsafe.Pointer(&dataBuffer[0]), unsafe.Pointer(&headerBuffer[0]), C.int32_t(dataBufferSize))
	if ret == 0 {
		return -1, fmt.Errorf("conn(%p) write failed", conn)
	}
	return len(dataBuffer), nil
}

func (conn *Connection) Read([]byte) (int, error) {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn(%p) has been closed", conn)
	}
	return 0, nil
}

func (conn *Connection) GetRecvMsgBuffer() ([]byte, []byte, error) { //*Buffer, *Buffer
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil, nil, fmt.Errorf("conn(%p) has been closed", conn)
	}
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	entry := C.getRecvMsgBuffer((*C.Connection)(conn.cConn))
	recvHeaderBuff := CbuffToSlice(entry.header_buff, int(entry.header_len))
	recvDataBuff := CbuffToSlice(entry.data_buff, int(entry.data_len))
	C.free(unsafe.Pointer(entry))
	return recvHeaderBuff, recvDataBuff, nil
}

func (conn *Connection) RdmaPostRecvHeader(headerBuffer []byte) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("server conn(%p) has been closed", conn)
		//return 0
	}
	ret := int(C.rdmaPostRecvHeader((*C.Connection)(conn.cConn), unsafe.Pointer(&headerBuffer[0])))
	if ret == 0 {
		return fmt.Errorf("server conn(%p) post recv header failed", conn)
		//return 0
	}
	return nil
}

func (conn *Connection) SendResp(responseBuffer []byte) (int, error) { //*Buffer
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn has been closed")
	}
	ret := C.connAppSendResp((*C.Connection)(conn.cConn), unsafe.Pointer(&responseBuffer[0]), C.int32_t(len(responseBuffer)))
	if ret <= 0 {
		return -1, fmt.Errorf("conn(%p) send response failed", conn)
	}
	return len(responseBuffer), nil
}

func (conn *Connection) RecvResp([]byte) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("conn(%p) has been closed", conn)
	}
	return nil
}

func (conn *Connection) GetRecvResponseBuffer() ([]byte, error) { //*Buffer
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil, fmt.Errorf("conn(%p) has been closed", conn)
	}
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	entry := C.getRecvResponseBuffer((*C.Connection)(conn.cConn))
	recvResponseBuff := CbuffToSlice(entry.response_buff, int(entry.response_len))
	C.free(unsafe.Pointer(entry))
	return recvResponseBuff, nil
}

func (conn *Connection) RdmaPostRecvResponse(responseBuffer []byte) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("client conn(%p) has been closed", conn)
	}
	ret := int(C.rdmaPostRecvResponse((*C.Connection)(conn.cConn), unsafe.Pointer(&responseBuffer[0])))
	if ret == 0 {
		return fmt.Errorf("client conn(%p) post recv response failed", conn)
	}
	return nil
}

type RdmaPoolConfig struct {
	MemBlockNum       int
	MemBlockSize      int
	MemPoolLevel      int
	HeaderBlockNum    int
	HeaderPoolLevel   int
	ResponseBlockNum  int
	ResponsePoolLevel int
	WqDepth           int
	MinCqeNum         int
}

func parseRdmaPoolConfig(gCfg *RdmaPoolConfig, cCfg *C.struct_RdmaPoolConfig) error {
	if cCfg == nil {
		return fmt.Errorf("gCfg parse to cCfg failed: cCfg is NULL")
	}
	if gCfg == nil {
		return nil
	}
	if gCfg.MemBlockNum != 0 {
		cCfg.memBlockNum = C.int(gCfg.MemBlockNum)
	}
	if gCfg.MemBlockSize != 0 {
		cCfg.memBlockSize = C.int(gCfg.MemBlockSize)
	}
	if gCfg.MemPoolLevel != 0 {
		cCfg.memPoolLevel = C.int(gCfg.MemPoolLevel)
	}
	if gCfg.HeaderBlockNum != 0 {
		cCfg.headerBlockNum = C.int(gCfg.HeaderBlockNum)
	}
	if gCfg.HeaderPoolLevel != 0 {
		cCfg.headerPoolLevel = C.int(gCfg.HeaderPoolLevel)
	}
	if gCfg.ResponseBlockNum != 0 {
		cCfg.responseBlockNum = C.int(gCfg.ResponseBlockNum)
	}
	if gCfg.ResponsePoolLevel != 0 {
		cCfg.responsePoolLevel = C.int(gCfg.ResponsePoolLevel)
	}
	if gCfg.WqDepth != 0 {
		cCfg.wqDepth = C.int(gCfg.WqDepth)
	}
	if gCfg.MinCqeNum != 0 {
		cCfg.minCqeNum = C.int(gCfg.MinCqeNum)
	}
	return nil
}

func InitPool(cfg *RdmaPoolConfig) error {
	cCfg := C.getRdmaPoolConfig()
	if err := parseRdmaPoolConfig(cfg, cCfg); err != nil {
		return err
	}
	ret := int(C.initRdmaPool(cCfg))
	if ret == 0 {
		return fmt.Errorf("init pool failed")
	}
	return nil
}

func DestroyPool() {
	C.destroyRdmaPool()
}

func GetHeaderSize() int {
	//return int(C.getHeaderSize())
	return 162
}

func GetResponseSize() int {
	//return int(C.getResponseSize())
	return 216
}
