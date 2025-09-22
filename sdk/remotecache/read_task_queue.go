package remotecache

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	DefaultReadQueueMaxCount = 4096
	DefaultMaxSize           = 512 * 1024 // 512K
	DefaultTaskWorkers       = 64
	DefaultConnTasksBuffer   = 200000
)

type ReadTaskQueue struct {
	mu           sync.RWMutex
	tasks        chan *ReadTaskItem
	waitTime     time.Duration
	activateTime time.Duration
	currentSize  int64
	maxSize      int64
	lastSend     time.Time
	flashAddr    string
	rc           *RemoteCacheClient
	stopC        chan struct{}
	connTasks    chan *ReadTaskWithPacket
	tasksChan    chan []*ReadTaskItem
	taskPool     *util.GTaskPool
}

type ReadTaskItem struct {
	req      *proto.BatchReadItem
	res      *proto.ReadResult
	done     chan struct{}
	deadline int64
	closed   int32
}

func (rti *ReadTaskItem) Done(res *proto.ReadResult) {
	if atomic.CompareAndSwapInt32(&rti.closed, 0, 1) {
		rti.res = res
		close(rti.done)
	}
}

type ReadTaskWithPacket struct {
	tasks  []*ReadTaskItem
	packet *proto.Packet
	finish int32
}

func NewReadTaskQueue(flashAddr string, waitTime time.Duration, rc *RemoteCacheClient) *ReadTaskQueue {
	rq := &ReadTaskQueue{
		tasks:        make(chan *ReadTaskItem, DefaultReadQueueMaxCount*8),
		waitTime:     waitTime,
		activateTime: rc.activateTime,
		lastSend:     time.Now(),
		flashAddr:    flashAddr,
		rc:           rc,
		stopC:        make(chan struct{}),
		maxSize:      DefaultMaxSize,
		connTasks:    make(chan *ReadTaskWithPacket, DefaultConnTasksBuffer),
		tasksChan:    make(chan []*ReadTaskItem, DefaultReadQueueMaxCount*4),
		taskPool:     util.NewGTaskPool(rc.connWorkers),
	}

	go rq.timerLoop()
	for i := 0; i < rc.connWorkers; i++ {
		go rq.connTaskWorker()
	}
	for i := 0; i < DefaultTaskWorkers; i++ {
		go rq.taskWorker()
	}
	return rq
}

func (rq *ReadTaskQueue) Enqueue(task *ReadTaskItem) {
	rq.mu.RLock()
	select {
	case rq.tasks <- task:
		atomic.AddInt64(&rq.currentSize, int64(task.req.Size_))
		if rq.shouldBatch(false) {
			rq.mu.RUnlock()
			rq.mu.Lock()
			if rq.shouldBatch(true) {
				rq.batchRead()
			} else {
				rq.mu.Unlock()
			}
		} else {
			rq.mu.RUnlock()
		}
	default:
		rq.mu.RUnlock()
		rq.mu.Lock()
		rq.batchRead()
		rq.Enqueue(task)
	}
}

func (rq *ReadTaskQueue) shouldBatch(change bool) bool {
	if atomic.LoadInt64(&rq.currentSize) >= rq.maxSize {
		return true
	}
	if time.Since(rq.lastSend) >= rq.activateTime {
		if change {
			rq.lastSend = time.Now()
		}
		return true
	}
	return false
}

func (rq *ReadTaskQueue) timerLoop() {
	ticker := time.NewTicker(rq.waitTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rq.mu.RLock()
			timeout := time.Since(rq.lastSend) >= rq.waitTime
			rq.mu.RUnlock()
			if timeout {
				rq.mu.Lock()
				rq.batchRead()
			}
		case <-rq.stopC:
			return
		case <-rq.rc.stopC:
			return
		}
	}
}

func (rq *ReadTaskQueue) Stop() {
	close(rq.stopC)
	rq.taskPool.Close()
}

func (rq *ReadTaskQueue) connTaskWorker() {
	for {
		select {
		case taskWithPacket := <-rq.connTasks:
			timeOut := rq.rc.calculateRetryTimeout(0)
			bg := stat.BeginStat()
			for i := 0; i < DefaultMaxRetryCount && atomic.LoadInt32(&taskWithPacket.finish) == 0; i++ {
				done := make(chan struct{})
				isLast := DefaultMaxRetryCount == i+1
				connTimeOut := rq.rc.calculateConnTimeout(i)
				_ = rq.taskPool.AsyncRunNoHang(func() {
					rq.batchSmallObjectRead(taskWithPacket, isLast, connTimeOut)
					close(done)
				})
				select {
				case <-done:
				case <-time.After(time.Duration(timeOut) * time.Millisecond):
					timeOut = rq.rc.calculateRetryTimeout(i + 1)
					if log.EnableDebug() {
						log.LogDebugf("connTaskWorker: timeout on retry (%d/%d), addr(%v)", i+1, DefaultMaxRetryCount, rq.flashAddr)
					}
				}
			}
			stat.EndStat("loadSmallData", nil, bg, 1)
		case <-rq.stopC:
			return
		case <-rq.rc.stopC:
			return
		}
	}
}

func (rq *ReadTaskQueue) taskWorker() {
	for {
		select {
		case tasks := <-rq.tasksChan:
			_ = rq.processConnTask(tasks)
		case <-rq.stopC:
			return
		case <-rq.rc.stopC:
			return
		}
	}
}

func (rq *ReadTaskQueue) batchSmallObjectRead(taskWithPacket *ReadTaskWithPacket, isLast bool, connTimeOut int) {
	var (
		err  error
		conn *net.TCPConn
	)
	bg := stat.BeginStat()
	var getResult bool
	defer func() {
		if err != nil && (isLast || getResult) {
			for _, task := range taskWithPacket.tasks {
				task.Done(&proto.ReadResult{
					ReqId:      task.req.ReqId,
					ResultCode: uint32(proto.OpErr),
					Data:       []byte(err.Error()),
				})
			}
		}
		stat.EndStat("batchSmallObjectRead", err, bg, 1)
	}()

	var replyPacket *proto.Packet
	if conn, err = rq.rc.conns.GetConnect(rq.flashAddr); err != nil {
		getResult = true
		log.LogWarnf("batchSmallObjectRead: failed to get connection, firstPacktTime(%v) addr(%v) err(%v)", rq.rc.firstPacketTimeout, rq.flashAddr, err)
		return
	}
	conn.SetWriteDeadline(time.Now().Add(time.Duration(connTimeOut) * time.Millisecond))
	if err = taskWithPacket.packet.WriteToNoDeadLineConn(conn); err != nil {
		rq.rc.connPutChan <- &ConnPutTask{conn: conn, forceClose: true}
		log.LogWarnf("batchSmallObjectRead: failed to write, firstPacktTime(%v) addr(%v) err(%v)", rq.rc.firstPacketTimeout, rq.flashAddr, err)
		return
	}

	replyPacket = NewFlashCacheReply()
	if err = replyPacket.ReadFromConnExt(conn, connTimeOut); err != nil {
		rq.rc.connPutChan <- &ConnPutTask{conn: conn, forceClose: true}
		log.LogWarnf("batchSmallObjectRead: failed to read, firstPacktTime(%v) addr(%v) err(%v)", rq.rc.firstPacketTimeout, rq.flashAddr, err)
		return
	}
	getResult = true
	atomic.StoreInt32(&taskWithPacket.finish, 1)
	rq.rc.connPutChan <- &ConnPutTask{conn: conn, forceClose: false}
	if replyPacket.ResultCode != proto.OpOk {
		err = fmt.Errorf(string(replyPacket.Data))
		log.LogWarnf("batchSmallObjectRead: ResultCode NOK, replyPacket(%v), addr(%v), ResultCode(%v) err(%v)", replyPacket, rq.flashAddr, replyPacket.ResultCode, err.Error())
		return
	}
	resp := new(proto.BatchReadResp)
	if err = replyPacket.UnmarshalDataPb(resp); err != nil {
		log.LogWarnf("batchSmallObjectRead: failed to Unmarshal response, err(%v)", err)
		return
	}
	resultMap := make(map[uint64]*proto.ReadResult)
	for _, result := range resp.Results {
		resultMap[result.ReqId] = result
	}
	for _, task := range taskWithPacket.tasks {
		if result, found := resultMap[task.req.ReqId]; found {
			task.Done(result)
		} else {
			task.Done(&proto.ReadResult{
				ReqId:      task.req.ReqId,
				ResultCode: uint32(proto.OpErr),
				Data:       []byte("result not found in response"),
			})
		}
	}
	if log.EnableDebug() {
		log.LogDebugf("batchSmallObjectRead: success, addr(%v) taskCount(%v)", rq.flashAddr, len(taskWithPacket.tasks))
	}
}

func (rq *ReadTaskQueue) batchRead() {
	var tasks []*ReadTaskItem
	for {
		select {
		case task := <-rq.tasks:
			tasks = append(tasks, task)
		default:
			goto done
		}
	}
done:
	rq.lastSend = time.Now()
	atomic.StoreInt64(&rq.currentSize, 0)
	rq.mu.Unlock()
	if len(tasks) == 0 {
		return
	}
	select {
	case rq.tasksChan <- tasks:
	case <-rq.stopC:
		for _, task := range tasks {
			task.Done(&proto.ReadResult{
				ReqId:      task.req.ReqId,
				ResultCode: uint32(proto.OpErr),
				Data:       []byte("read task queue stopped"),
			})
		}
	}
}

func (rq *ReadTaskQueue) processConnTask(tasks []*ReadTaskItem) (err error) {
	if len(tasks) == 0 {
		return nil
	}
	bg := stat.BeginStat()
	defer func() {
		if err != nil {
			for _, task := range tasks {
				task.Done(&proto.ReadResult{
					ReqId:      task.req.ReqId,
					ResultCode: uint32(proto.OpErr),
					Data:       []byte(err.Error()),
				})
			}
		}
		stat.EndStat("processConnTask", err, bg, 1)
	}()
	req := &proto.BatchReadReq{
		Items: make([]*proto.BatchReadItem, 0, len(tasks)),
	}
	var deadline int64
	for _, task := range tasks {
		req.Items = append(req.Items, task.req)
		if task.deadline > deadline {
			deadline = task.deadline
		}
	}
	req.Deadline = uint64(deadline)
	reqPacket := proto.NewPacketReqID()
	reqPacket.Opcode = proto.OpFlashNodeBatchReadObject
	if err = reqPacket.MarshalDataPb(req); err != nil {
		log.LogWarnf("processConnTask: failed to MarshalData err(%v)", err)
		return
	}
	taskWithPacket := &ReadTaskWithPacket{
		tasks:  tasks,
		packet: reqPacket,
	}
	select {
	case rq.connTasks <- taskWithPacket:
	case <-rq.stopC:
		err = fmt.Errorf("read task queue stopped")
	}
	return
}
