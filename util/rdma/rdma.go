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

//const CONNECTED = 1
//const DISCONNECTED = 0

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

//type ConnectionCallback func(int, *Connection)

//type ClientConnCallback func(int, *Buffer, *ClientConnection)
//type ClientConnCallback func(int, *ClientConnection)

//type RecvCallback func(buffer *Buffer) int

//export PrintCallback
func PrintCallback(cstr *C.char) {
	str := C.GoString(cstr)
	println(str)
}

//export EpollAddSendAndRecvEvent
func EpollAddSendAndRecvEvent(fd C.int, ctx unsafe.Pointer) {
	GetEpoll().EpollAdd(int(fd), func() {
		//print("call")
		//println()
		if ok := C.transport_sendAndRecv_event_cb(ctx); ok == 0 {
			// TODO error handler
		}
	})
}

//export EpollAddConnectEvent
func EpollAddConnectEvent(fd C.int, ctx unsafe.Pointer) {
	GetEpoll().EpollAdd(int(fd), func() {
		//println(fd)
		//print("call")
		//println()
		if ok := C.connection_event_cb(unsafe.Pointer(ctx)); ok == 0 {
			//TODO err handler
		}
	})
}

//export EpollDelConnEvent
func EpollDelConnEvent(fd C.int) {
	GetEpoll().EpollDel(int(fd))
}

/*
func GetFieldOffset(ptr unsafe.Pointer, fieldName *C.char) uintptr {
	fieldNameStr := C.GoString(fieldName)
	structPtr := uintptr(ptr)
	structValue := reflect.ValueOf(unsafe.Pointer(structPtr)).Elem()
	field := structValue.FieldByName(fieldNameStr)
	return field.UnsafeAddr()
}
*/

//export RecvHeaderCallback
func RecvHeaderCallback(header unsafe.Pointer, len C.int, fielaName *C.char) C.int {
	//app parse header
	//headerByte := CbuffToSlice(header,int(len))
	//offset := GetFieldOffset(header, fielaName)

	return C.int(145)
}

//export RecvMessageCallback
func RecvMessageCallback(ctx unsafe.Pointer, entry *C.MemoryEntry) { //TODO need to modify
	//data := make([]byte, int(dataLen))
	//ret := C.connAppRead(conn, unsafe.Pointer(&data[0]), addr, dataLen)
	//print("server recv data:")
	//println(string(data))
	//return ret

	conn := (*Connection)(ctx)
	//println("---");
	//println(conn);
	//println("+++");
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

	//C.freeEntry(entry)
}

//export DisConnectCallback
func DisConnectCallback(ctx unsafe.Pointer) {
	conn := (*Connection)(ctx)
	atomic.StoreInt32(&conn.state, CONN_ST_CLOSING)
	C.notify_event(conn.rFd, 0)
	//C.notify_event(&conn.wFd)
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
	//fd         int
	//conn_ev    unsafe.Pointer
	//cb      ConnectionCallback
	cListener unsafe.Pointer
}

func NewRdmaServer(targetIp, targetPort string) (server *Server, err error) { //, memoryPool *MemoryPool, headerPool *ObjectPool, responsePool *ObjectPool
	server = &Server{}
	server.RemoteIp = ""   // TODO
	server.RemotePort = "" // TODO
	server.LocalIp = targetIp
	server.LocalPort = targetPort
	nPort, _ := strconv.Atoi(server.LocalPort)
	serverAddr := strings.Join([]string{server.LocalIp, server.LocalPort}, ":")
	cCtx := C.StartServer(C.CString(server.LocalIp), C.ushort(nPort), C.CString(serverAddr)) //, (*C.MemoryPool)(memoryPool.cMemoryPool),
	//(*C.ObjectPool)(headerPool.cObjectPool), (*C.ObjectPool)(responsePool.cObjectPool), (*C.struct_ibv_pd)(memoryPool.cPd), (*C.struct_ibv_mr)(memoryPool.cMr)
	if cCtx == nil {
		return nil, errors2.New("server start failed")
	}

	/*
		fd := cCtx.listen_id.channel.fd
		GetEpoll().EpollAdd(int(fd), func() {
			if ok := C.connection_event_cb(unsafe.Pointer(cCtx.conn_ev)); ok == 0 {
				//TODO err handler
			}
		})
	*/

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
	//conn.cConn = unsafe.Pointer(cConn)
	//C.setConnContext(cConn, unsafe.Pointer(conn));
	//conn.waitResp = make(map[*C.char]unsafe.Pointer)
	//conn.recvMsgList = list.New()
	//atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)

	//println("server accept")
	return conn
}

func (server *Server) Close() (err error) {
	ret := C.CloseServer((*C.struct_RdmaListener)(server.cListener))
	if ret != 1 {
		err = errors2.New("server free failed")
		return err
	}
	//println("server free success")
	return
}

type Client struct {
	LocalIp    string
	LocalPort  string
	RemoteIp   string
	RemotePort string
	//fd         int
	//conn_ev    unsafe.Pointer
	//cb      ClientConnCallback
	cClient       unsafe.Pointer
	reConnectFlag bool
	//conn       *ClientConnection
}

func NewRdmaClient(targetIp, targetPort string) (client *Client, err error) { //, memoryPool *MemoryPool, headerPool *ObjectPool, responsePool *ObjectPool
	client = &Client{}
	client.RemoteIp = targetIp
	client.RemotePort = targetPort
	targetAddr := strings.Join([]string{targetIp, targetPort}, ":")
	cCtx := C.Connect(C.CString(client.RemoteIp), C.CString(client.RemotePort), C.CString(targetAddr)) //, (*C.MemoryPool)(memoryPool.cMemoryPool),
	//(*C.ObjectPool)(headerPool.cObjectPool), (*C.ObjectPool)(responsePool.cObjectPool), (*C.struct_ibv_pd)(memoryPool.cPd), (*C.struct_ibv_mr)(memoryPool.cMr)
	if cCtx == nil {
		//println("client connect failed")
		err = errors2.New("client connect failed")
		return nil, err
	}

	client.cClient = unsafe.Pointer(cCtx)

	/*
		fd := cCtx.listen_id.channel.fd
		GetEpoll().EpollAdd(int(fd), func() {
			if ok := C.connection_event_cb(unsafe.Pointer(cCtx.conn_ev)); ok == 0 {
				//TODO err handler
			}
		})
	*/

	return client, nil
}

func (client *Client) Dial() (conn *Connection) {
	//println("client dial")
	conn = &Connection{}
	cConn := C.getClientConn((*C.struct_RdmaContext)(client.cClient))
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTING)
	conn.init(cConn)
	conn.conntype = CLIENT_CONN
	conn.Ctx = client
	conn.localAddr = &RdmaAddr{address: C.GoString(&(cConn.local_addr[0])), network: "rdma"}
	conn.remoteAddr = &RdmaAddr{address: C.GoString(&(cConn.remote_addr[0])), network: "rdma"}
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)

	//conn.cConn = unsafe.Pointer(cConn)
	//conn.waitResp = make(map[*C.char]unsafe.Pointer)
	//conn.recvMsgList = list.New()
	//atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	return conn
}

func (client *Client) Close() (err error) {
	ret := C.CloseClient((*C.struct_RdmaContext)(client.cClient))
	if ret != 1 {
		err = errors2.New("client free failed")
		return err
	}
	//client.cClient = nil
	//println("client free success")
	return nil
}

type RecvMsg struct {
	headerPtr   []byte
	dataPtr     []byte
	responsePtr []byte
	//len       int
}

/*
type Buffer struct {
	conn *Connection
	buffer []byte
	btype  int32
}
*/

type Connection struct {
	cConn       unsafe.Pointer
	state       int32
	mu          sync.RWMutex
	recvMsgList *list.List
	rFd         C.int
	wFd         C.int
	//eFd C.int
	conntype int
	//ip string
	//port string
	Ctx interface{}
	//waitResp map[*C.char]unsafe.Pointer
	localAddr  *RdmaAddr
	remoteAddr *RdmaAddr
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
	//atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	//TODO accept和setConnContext之间发生了disconnect
	conn.cConn = unsafe.Pointer(cConn)
	conn.rFd = C.open_event_fd()
	conn.wFd = C.open_event_fd()
	conn.SetDeadline(time.Now().Add(50 * time.Millisecond))
	C.setConnContext(cConn, unsafe.Pointer(conn))
	//println("conn set connContext")

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
	//if conn == nil {
	//	return
	//}

	//if conn.onRecv != nil {
	//	conn.onRecv(conn, buff, recvLen, status)
	//	C.cbrdma_release_recv_buff(conn.connPtr, unsafe.Pointer(&buff[0]))
	//	return
	//}

	//if status < 0 {
	//	conn.onDisconnected(conn)
	//	return
	//}
	//println(conn);

	//if atomic.LoadInt32(&conn.state) == CONN_ST_CLOSE {//if conn has been closed, drop the recv msg
	//	return
	//}

	conn.mu.Lock()
	if len(buffs) == 2 {
		//print("OnRecvCB: recvMsgList: ")
		//println(conn.recvMsgList)
		//println(buffs[0])
		//println(buffs[1])
		conn.recvMsgList.PushBack(&RecvMsg{buffs[0], buffs[1], nil})
	} else {
		conn.recvMsgList.PushBack(&RecvMsg{nil, nil, buffs[0]})
	}

	conn.mu.Unlock()
	C.notify_event(conn.rFd, 0)
	return
}

func (conn *Connection) OnSendCB(sendLen int) {
	//if conn == nil {
	//	return
	//}

	//if conn.onSend != nil {
	//	conn.onSend(conn, sendLen, status)
	//	return
	//}

	//if status < 0 {
	//	conn.onDisconnected(conn)
	//	return
	//}

	C.notify_event(conn.wFd, 0)
	return
}

func (conn *Connection) ReConnect() error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CLOSED {
		return fmt.Errorf("conn has not been closed")
	}
	if ret := int(C.ReConnect((*C.Connection)(conn.cConn))); ret == 0 {
		return fmt.Errorf("conn reconnect failed")
	}
	conn.rFd = C.open_event_fd()
	conn.wFd = C.open_event_fd()
	atomic.StoreInt32(&conn.state, CONN_ST_CONNECTED)
	return nil
}

func (conn *Connection) Close() (err error) {
	//if atomic.LoadInt32(&conn.state) != CONN_ST_CLOSED {
	//	atomic.StoreInt32(&conn.state, CONN_ST_CLOSED)
	//}
	//C.cbrdma_close(conn.connPtr)
	C.DisConnect((*C.Connection)(conn.cConn), false)
	/*
		ret := C.DisConnect((*C.Connection)(conn.cConn))
		if(ret == 0) {//TODO 分情况讨论
			err = errors2.New("conn close failed")
			return err
		}
	*/

	if conn.rFd > 0 {
		C.notify_event(conn.rFd, 0)
		C.close(conn.rFd)
		conn.rFd = -1
	}

	if conn.wFd > 0 {
		C.notify_event(conn.wFd, 0)
		C.close(conn.wFd)
		conn.wFd = -1
	}

	//conn.mu.Lock()
	//defer conn.mu.Unlock()
	if conn.recvMsgList != nil {
		for elem := conn.recvMsgList.Front(); elem != nil; elem = elem.Next() {
			msg := elem.Value.(*RecvMsg)
			//TODO
			//C.release_recv_buff(conn.connPtr, unsafe.Pointer(&msg.dataPatr[0]))
			if msg.headerPtr != nil {
				if err := conn.RdmaPostRecvHeader(msg.headerPtr); err != nil { //TODO error handler
					//return err.errors2.New("")
				}
				if msg.dataPtr != nil {
					ReleaseDataBuffer(msg.dataPtr) //TODO error handler
				}
			} else {
				if msg.responsePtr != nil {
					if err := conn.RdmaPostRecvResponse(msg.responsePtr); err != nil { //TODO error handler
						//return
					}
				}
			}

		}
		conn.recvMsgList.Init()
	}

	atomic.StoreInt32(&conn.state, CONN_ST_CLOSED)
	//if conn.conntype == CLIENT_CONN {
	//	changeAppCloseState((*C.Connection)(conn.cConn))
	//}

	return nil
}

/*
func (buffer *Buffer) GetByte() ([]byte, error) {
	if atomic.LoadInt32(&(buffer.conn.state)) != CONN_ST_CONNECTED {
		return nil, fmt.Errorf("conn(%p) of buffer(%p) has been closed",buffer.conn,buffer)
	}
	return buffer.buffer, nil
}

func (buffer *Buffer) Release() error {
	conn := buffer.conn
	if atomic.LoadInt32(&(buffer.conn.state)) != CONN_ST_CONNECTED {
		return fmt.Errorf("conn(%p) of buffer(%p) has been closed",conn,buffer)
	}
	if atomic.LoadInt32(&(buffer.btype)) == BUFFER_DATA {
		if conn.ReleaseDataBuffer(buffer.buffer) == 0 {
			return fmt.Errorf("conn(%p) release data buffer(%p) failed",conn, buffer)
		}
		return nil
	} else if atomic.LoadInt32(&(buffer.btype)) == BUFFER_HEADER {
		if conn.conntype == CLIENT_CONN {
			if conn.ReleaseHeaderBuffer(buffer.buffer) == 0 {
				return fmt.Errorf("client conn(%p) release header buffer(%p) failed",conn, buffer)
			}
			return nil
		} else {
			if conn.RdmaPostRecvHeader(buffer.buffer)== 0 {
				return fmt.Errorf("server conn(%p) post recv header buffer(%p) failed",conn, buffer)
			}
			return nil
		}
	} else {
		if conn.conntype == CLIENT_CONN {
			if conn.RdmaPostRecvResponse(buffer.buffer) == 0 {
				return fmt.Errorf("client conn(%p) post recv response buffer(%p) failed",conn, buffer)
			}
			return nil
		} else {
			if conn.ReleaseResponseBuffer(buffer.buffer) == 0 {
				return fmt.Errorf("server conn(%p) release response buffer(%p) failed",conn, buffer)
			}
			return nil
		}
	}
}
*/

func GetDataBuffer(len uint32, timeout_us int) ([]byte, error) {
	var bufferSize C.int64_t

	dataPtr := C.getDataBuffer(C.uint32_t(len), C.int64_t(timeout_us), &bufferSize)
	if bufferSize == 0 {
		return nil, fmt.Errorf("get data buffer timeout")
	}
	if bufferSize == -1 { //TODO
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

/*
func (conn *Connection) GetDataBuffer(len uint32, timeout_us int) ([]byte, error) {  //(*Buffer, error)
	var bufferSize C.int64_t
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil,fmt.Errorf("conn(%p) has been closed",conn)
	}
	dataPtr := C.getDataBuffer((*C.Connection)(conn.cConn), C.uint32_t(len), C.int64_t(timeout_us), &bufferSize)

	if bufferSize == 0 {
		return nil,fmt.Errorf("conn(%p) get data buffer timeout", conn)
	}
	if bufferSize == -1 {
		return nil,fmt.Errorf("conn(%p) get data buffer failed, conn has been closed", conn)
	}
	//if dataPtr == C.NULL {
	//	return nil, fmt.Errorf("conn(%p) get send buff failed", conn)
	//}
	dataBuffer := CbuffToSlice(dataPtr, int(bufferSize))
	return dataBuffer, nil
	//buffer := &Buffer{conn: conn, buffer: dataBuffer}
	//atomic.StoreInt32(&buffer.btype, BUFFER_DATA)
	//return buffer, nil
}

func (conn *Connection) ReleaseDataBuffer(dataBuffer []byte) int {
	return int(C.releaseDataBuffer((*C.Connection)(conn.cConn),unsafe.Pointer(&dataBuffer[0])))
}
*/

func (conn *Connection) GetHeaderBuffer(timeout_us int) ([]byte, error) { //*Buffer
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

	headerBuffer := CbuffToSlice(buffPtr, GetHeaderSize()) //GetResponseSize()
	//buffer := &Buffer{conn: conn, buffer: headerBuffer}
	//atomic.StoreInt32(&buffer.btype, BUFFER_HEADER)
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
	//buffer := &Buffer{conn: conn, buffer: respBuffer}
	//atomic.StoreInt32(&buffer.btype, BUFFER_RESPONSE)
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

func (conn *Connection) WriteBuffer(headerBuffer []byte, dataBuffer []byte) (int, error) { //dataBuffer []byte, headerBuffer []byte //,*Buffer *Buffer
	//if conn != dataBuffer.conn || conn != headerBuffer.conn {
	//	return -1,fmt.Errorf("there is no association between conn(%p) and dataBuffer(%p) and headerBuffer(%p)",conn,dataBuffer,headerBuffer)
	//}
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn(%p) has been closed", conn)
	}
	//headerBuffer := buffer[:57+8+40]
	//dataBuffer := buffer[57+8+40:]
	ret := C.connAppWrite((*C.Connection)(conn.cConn), unsafe.Pointer(&dataBuffer[0]), unsafe.Pointer(&headerBuffer[0]), C.int32_t(len(dataBuffer)))
	//println(ret);
	if ret == 0 { //TODO 错误码分情况(only one case),OK
		return -1, fmt.Errorf("conn(%p) write failed", conn)
	}

	//if value := C.wait_event(conn.wFd); value <= 0 {
	//	return fmt.Errorf("send failed")
	//}

	//if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
	//	return -1, fmt.Errorf("conn already closed")
	//}

	return len(dataBuffer), nil
}

func (conn *Connection) Read([]byte) (int, error) { //*Buffer
	//println("server conn read before");
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn(%p) has been closed", conn)
	}
	if value := C.wait_event(conn.rFd); value <= 0 {
		return -1, fmt.Errorf("conn(%p) read failed")
	}

	//println("server conn read before");
	return 0, nil
}

/*
func (conn *Connection) Send(buff []byte, header []byte, length int) (err error) {
	ret := C.connAppWrite((*C.Connection)(conn.cConn), unsafe.Pointer(&buff[0]), unsafe.Pointer(&header[0]), C.int32_t(length))

	if ret <= 0 {
		return fmt.Errorf("conn(%p) send failed", conn)
	}

	//if value := C.wait_event(conn.wFd); value <= 0 {
	//	return fmt.Errorf("send failed")
	//}

	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("conn already closed")
	}

	return nil
}
*/

func (conn *Connection) GetRecvMsgBuffer() ([]byte, []byte, error) { //*Buffer, *Buffer
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil, nil, fmt.Errorf("conn(%p) has been closed", conn)
	}
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.recvMsgList.Len() == 0 {
		return nil, nil, fmt.Errorf("conn(%p) can not get any recv msg", conn)
	}

	msgElem := conn.recvMsgList.Front()
	conn.recvMsgList.Remove(msgElem)
	msg := msgElem.Value.(*RecvMsg)
	//headerBuffer := &Buffer{conn: conn, buffer: msg.headerPtr}
	//dataBuffer := &Buffer{conn: conn, buffer: msg.dataPtr}
	//atomic.StoreInt32(&headerBuffer.btype,BUFFER_HEADER)
	//atomic.StoreInt32(&dataBuffer.btype,BUFFER_DATA)
	return msg.headerPtr, msg.dataPtr, nil
	//return headerBuffer, dataBuffer, nil
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
	//return 1
}

func (conn *Connection) SendResp(responseBuffer []byte) (int, error) { //*Buffer
	//if conn != responseBuffer.conn {
	//	return -1,fmt.Errorf("there is no association between conn(%p) and responseBuffer(%p)",conn,responseBuffer)
	//}
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return -1, fmt.Errorf("conn has been closed")
	}

	//TODO
	//cEntry := (*C.MemoryEntry)(conn.waitResp[addr])
	//C.preSendPesp((*C.Connection)(conn.cConn), cEntry)
	ret := C.connAppSendResp((*C.Connection)(conn.cConn), unsafe.Pointer(&responseBuffer[0]), C.int32_t(len(responseBuffer)))
	//ret := C.connRdmaSendResponse((*C.Connection)(conn.cConn), unsafe.Pointer(&resp[0]),C.int32_t(len(resp))) //TODO need to modify ()
	if ret <= 0 {
		return -1, fmt.Errorf("conn(%p) send response failed", conn)
	}

	//println("send response");
	return len(responseBuffer), nil
}

func (conn *Connection) RecvResp([]byte) error { // //TODO *Buffer
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("conn(%p) has been closed", conn)
	}

	if value := C.wait_event(conn.rFd); value <= 0 {
		return fmt.Errorf("conn(%p) recv resp failed", conn)
	}

	return nil
}

func (conn *Connection) GetRecvResponseBuffer() ([]byte, error) { //*Buffer
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return nil, fmt.Errorf("conn(%p) has been closed", conn)
	}
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.recvMsgList.Len() == 0 {
		return nil, fmt.Errorf("conn(%p) can not get any recv msg", conn)
	}

	msgElem := conn.recvMsgList.Front()
	conn.recvMsgList.Remove(msgElem)
	msg := msgElem.Value.(*RecvMsg)
	//responseBuffer := &Buffer{conn: conn, buffer: msg.responsePtr}
	//atomic.StoreInt32(&(responseBuffer.btype),BUFFER_RESPONSE)
	return msg.responsePtr, nil
	//return responseBuffer, nil
}

func (conn *Connection) RdmaPostRecvResponse(responseBuffer []byte) error {
	if atomic.LoadInt32(&conn.state) != CONN_ST_CONNECTED {
		return fmt.Errorf("client conn(%p) has been closed", conn)
		//return 0
	}
	ret := int(C.rdmaPostRecvResponse((*C.Connection)(conn.cConn), unsafe.Pointer(&responseBuffer[0])))
	if ret == 0 {
		return fmt.Errorf("client conn(%p) post recv response failed", conn)
		//return 0
	}
	return nil
	//return 1;
}

type RdmaPoolConfig struct {
	MemBlockNum  int
	MemBlockSize int
	MemPoolLevel int

	HeaderBlockNum  int
	HeaderPoolLevel int

	ResponseBlockNum  int
	ResponsePoolLevel int

	WqDepth   int
	MinCqeNum int
}

func parseRdmaPoolConfig(gCfg *RdmaPoolConfig, cCfg *C.struct_RdmaPoolConfig) error {
	if cCfg == nil { //C.NULL map to nil
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
	//modify pool config
	//C.initRdmaPoolConfig(config)
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
	return int(C.getHeaderSize())
}

func GetResponseSize() int {
	return int(C.getResponseSize())
}

/*
type MemoryPool struct { //TODO pd?
	cMemoryPool unsafe.Pointer
	//cMr         unsafe.Pointer
	//cPd         unsafe.Pointer
}

func NewMemoryPool(blockSize, blockNum, level int) (memoryPool *MemoryPool) {
	memoryPool = &MemoryPool{}
	//new memory pool and buddy allocation
	//TODO need to deal the case when pool/pd/mr is NULL
	//pool := C.InitMemoryPool(C.int(blockNum), C.int(blockSize), C.int(level))
	//pd := C.alloc_pd()
	//mr := C.regist_mr(pool, pd)
	memoryPool.cMemoryPool = unsafe.Pointer(C.InitMemoryPool(C.int(blockNum), C.int(blockSize), C.int(level)))
	if uintptr(memoryPool.cMemoryPool) == 0 {
		return nil;
	}
	//memoryPool.cPd = unsafe.Pointer(pd)
	//memoryPool.cMr = unsafe.Pointer(mr)
	return memoryPool
}

func (memoryPool *MemoryPool) Close() {
	C.CloseMemoryPool((*C.MemoryPool)(memoryPool.cMemoryPool))
}

type ObjectPool struct {
	cObjectPool unsafe.Pointer
}

func NewObjectPool(blockSize, blockNum, level int) (objectPool *ObjectPool) {
	objectPool = &ObjectPool{}
	//pool := C.InitObjectPool(C.int(blockNum), C.int(blockSize), C.int(level))
	objectPool.cObjectPool = unsafe.Pointer(C.InitObjectPool(C.int(blockNum), C.int(blockSize), C.int(level)))
	if uintptr(objectPool.cObjectPool) == 0 {
		return nil;
	}
	return objectPool
}

func (objectPool *ObjectPool) Close() {
	C.CloseObjectPool((*C.ObjectPool)(objectPool.cObjectPool))
}

*/
