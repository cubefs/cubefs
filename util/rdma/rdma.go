package rdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm -lrt -pthread
#include "client.h"
#include "server.h"
//#include "rdma.h"
//#include "rdma_proto.h"
*/
import "C"
import (
	errors2 "errors"
	"fmt"
	syslog "log"
	"net"
	"reflect"
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

/*
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
		C.connection_event_cb(unsafe.Pointer(ctx))
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
	if !entry.is_response {
		recvHeaderBuff := CbuffToSlice(entry.header_buff, int(entry.header_len))
		recvDataBuff := CbuffToSlice(entry.data_buff, int(entry.data_len))
		conn.OnRecvCB(recvHeaderBuff, recvDataBuff)
	} else {
		recvResponseBuff := CbuffToSlice(entry.response_buff, int(entry.response_len))
		conn.OnRecvCB(recvResponseBuff)
	}
}

*/

//export DisConnectCallback
func DisConnectCallback(ctx unsafe.Pointer) {
	conn := (*Connection)(ctx)
	atomic.StoreInt32(&conn.state, CONN_ST_CLOSING)
	conn.rFd <- struct{}{}
}

/*
type RecvMsg struct {
	headerPtr   []byte
	dataPtr     []byte
	responsePtr []byte
}
*/

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
	rFd        chan struct{}
	wFd        chan struct{}
	conntype   int
	Ctx        interface{}
	localAddr  *RdmaAddr
	remoteAddr *RdmaAddr
	TargetIp   string
	TargetPort string
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
		return nil, errors2.New("server start failed")
	}
	server.cListener = unsafe.Pointer(cCtx)
	return server, nil
}

func (server *Server) Accept() (*Connection, error) {
	conn := &Connection{}
	cConn := C.get_rdma_server_conn((*C.struct_rdma_listener)(server.cListener))
	if cConn == nil {
		return nil, errors2.New("server accept failed")
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

/*
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
	cCtx := C.Connect(C.CString(client.RemoteIp), C.CString(client.RemotePort))
	if cCtx == nil {
		err = errors2.New("client connect failed")
		return nil, err
	}
	client.cClient = unsafe.Pointer(cCtx)
	return client, nil
}

func (client *Client) Close() (err error) {
	ret := C.CloseClient((*C.struct_RdmaContext)(client.cClient))
	if ret != 1 {
		err = errors2.New("client free failed")
		return err
	}
	return nil
}
*/

func (conn *Connection) Dial(targetIp, targetPort string) error {
	cConn := C.rdma_connect_by_addr(C.CString(targetIp), C.CString(targetPort))
	if cConn == nil {
		return errors2.New("conn dial failed")
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
	conn.SetDeadline(time.Now().Add(200 * time.Millisecond))
	C.set_conn_context(cConn, unsafe.Pointer(conn))
	conn.rFd = make(chan struct{}, 100)
	conn.wFd = make(chan struct{}, 100)
	//conn.recvMsgList = list.New()
}

func (conn *Connection) SetDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED && atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTING {
		return fmt.Errorf("set deadline failed, conn has been closed")
	}
	C.set_send_timeout_us((*C.connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	C.set_recv_timeout_us((*C.connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	return nil
}

func (conn *Connection) SetWriteDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("set deadline failed, conn has been closed")
	}
	C.set_send_timeout_us((*C.connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	return nil
}

func (conn *Connection) SetReadDeadline(t time.Time) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("set deadline failed, conn has been closed")
	}
	C.set_recv_timeout_us((*C.connection)(conn.cConn), (C.int64_t(t.UnixNano()-time.Now().UnixNano()) / 1000))
	return nil
}

/*
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
*/

func (conn *Connection) Close() (err error) {
	syslog.Println("conn app close")
	C.conn_disconnect((*C.connection)(conn.cConn)) //TODO
	/*
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
	*/
	atomic.StoreInt32(&conn.state, CONN_ST_CLOSED)
	return nil
}

func GetDataBuffer(len uint32, timeout_us int) ([]byte, error) {
	var bufferSize C.int64_t

	dataPtr := C.get_data_buffer(C.uint32_t(len), C.int64_t(timeout_us), &bufferSize)
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
	if int(C.release_data_buffer(unsafe.Pointer(&dataBuffer[0]))) != 0 {
		return fmt.Errorf("release data buffer(%p) failed", dataBuffer)
	}
	return nil
}

func (conn *Connection) GetHeaderBuffer(timeout_us int) ([]byte, error) {
	var bufferSize C.int32_t
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil, fmt.Errorf("conn(%p) has been closed", conn)
	}
	buffPtr := C.get_header_buffer((*C.connection)(conn.cConn), C.int64_t(timeout_us), &bufferSize)
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
	if int(C.release_header_buffer((*C.connection)(conn.cConn), unsafe.Pointer(&headerBuffer[0]))) != 0 {
		return fmt.Errorf("client conn(%p) release header buffer(%p) failed", conn, headerBuffer)
	}
	return nil
}

func (conn *Connection) GetResponseBuffer(timeout_us int) ([]byte, error) { //*Buffer
	var bufferSize C.int32_t
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil, fmt.Errorf("conn(%p) has been closed", conn)
	}
	buffPtr := C.get_response_buffer((*C.connection)(conn.cConn), C.int64_t(timeout_us), &bufferSize)
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
	if int(C.release_response_buffer((*C.connection)(conn.cConn), unsafe.Pointer(&responseBuffer[0]))) != 0 {
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
	ret := C.conn_app_write((*C.connection)(conn.cConn), unsafe.Pointer(&dataBuffer[0]), unsafe.Pointer(&headerBuffer[0]), C.int32_t(dataBufferSize))
	if ret != 0 {
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
	entry := C.get_recv_msg_buffer((*C.connection)(conn.cConn))
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
	ret := int(C.rdma_post_recv_header((*C.connection)(conn.cConn), unsafe.Pointer(&headerBuffer[0])))
	if ret != 0 {
		return fmt.Errorf("server conn(%p) post recv header failed", conn)
		//return 0
	}
	return nil
}

func (conn *Connection) SendResp(responseBuffer []byte) (int, error) { //*Buffer
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn has been closed")
	}
	ret := C.conn_app_send_resp((*C.connection)(conn.cConn), unsafe.Pointer(&responseBuffer[0]))
	if ret != 0 {
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
	entry := C.get_recv_response_buffer((*C.connection)(conn.cConn))
	recvResponseBuff := CbuffToSlice(entry.response_buff, int(entry.response_len))
	C.free(unsafe.Pointer(entry))
	return recvResponseBuff, nil
}

func (conn *Connection) RdmaPostRecvResponse(responseBuffer []byte) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("client conn(%p) has been closed", conn)
	}
	ret := int(C.rdma_post_recv_response((*C.connection)(conn.cConn), unsafe.Pointer(&responseBuffer[0])))
	if ret != 0 {
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
	EnableRdmaLog     bool
}

func parseRdmaPoolConfig(gCfg *RdmaPoolConfig, cCfg *C.struct_rdma_pool_config) error {
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
	if gCfg.HeaderBlockNum != 0 {
		cCfg.header_block_num = C.int(gCfg.HeaderBlockNum)
	}
	if gCfg.HeaderPoolLevel != 0 {
		cCfg.header_pool_level = C.int(gCfg.HeaderPoolLevel)
	}
	if gCfg.ResponseBlockNum != 0 {
		cCfg.response_block_num = C.int(gCfg.ResponseBlockNum)
	}
	if gCfg.ResponsePoolLevel != 0 {
		cCfg.response_pool_level = C.int(gCfg.ResponsePoolLevel)
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
	return nil
}

func InitPool(cfg *RdmaPoolConfig) error {
	cCfg := C.get_rdma_pool_config()
	if err := parseRdmaPoolConfig(cfg, cCfg); err != nil {
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

func GetHeaderSize() int {
	//return int(C.getHeaderSize())
	return 162
}

func GetResponseSize() int {
	//return int(C.getResponseSize())
	return 216
}
