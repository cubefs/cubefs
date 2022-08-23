package lcnode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

//type ScanDentry struct {
//	DelInode bool   //if Type is file, and DelInode is true, then Inode and Dentry(ParentId, Name) is to be deleted
//	ParentId uint64 // FileID value of the parent inode.
//	Name     string // Name of the current dentry.
//	Inode    uint64 // FileID value of the current inode.
//	Type     uint32
//}

// startServer binds and listens to the specified port.
func (l *LcNode) startServer() (err error) {
	log.LogInfo("Start: startServer")
	addr := fmt.Sprintf(":%v", l.listen)
	listener, err := net.Listen("tcp", addr)
	log.LogDebugf("action[startServer] listen tcp address(%v).", addr)
	if err != nil {
		log.LogError("failed to listen, err:", err)
		return
	}
	go func(stopC chan bool) {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			log.LogDebugf("action[startServer] accept connection from %s.", conn.RemoteAddr().String())
			select {
			case <-stopC:
				return
			default:
			}
			if err != nil {
				log.LogErrorf("action[startServer] failed to accept, err:%s", err.Error())
				continue
			}
			go l.serveConn(conn, stopC)
		}
	}(l.stopC)
	return
}

func (l *LcNode) stopServer() {
	if l.stopC != nil {
		defer func() {
			if r := recover(); r != nil {
				log.LogErrorf("action[StopTcpServer],err:%v", r)
			}
		}()

		l.StopScanners()
		close(l.stopC)
	}
}

func (l *LcNode) StopScanners() {
	for _, s := range l.scanners {
		s.Stop()
		delete(l.scanners, s.ID)
	}
}

// Read data from the specified tcp connection until the connection is closed by the remote or the tcp service is down.
func (l *LcNode) serveConn(conn net.Conn, stopC chan bool) {
	defer conn.Close()
	c := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	remoteAddr := conn.RemoteAddr().String()
	for {
		select {
		case <-stopC:
			return
		default:
		}
		p := &proto.Packet{}
		if err := p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
			if err != io.EOF {
				log.LogError("serve LcNode: ", err.Error())
			}
			return
		}
		if err := l.handlePacket(conn, p, remoteAddr); err != nil {
			log.LogErrorf("serve handlePacket fail: %v", err)
		}
	}
}

func (l *LcNode) handlePacket(conn net.Conn, p *proto.Packet, remoteAddr string) (err error) {
	log.LogInfof("HandleMetadataOperation input info op (%s), remote %s", p.String(), remoteAddr)
	switch p.Opcode {
	case proto.OpLcNodeHeartbeat:
		err = l.opMasterHeartbeat(conn, p, remoteAddr)
	case proto.OpLcNodeScan:
		err = l.opLcScan(conn, p, remoteAddr)
	default:
		err = fmt.Errorf("%s unknown Opcode: %d, reqId: %d", remoteAddr,
			p.Opcode, p.GetReqID())
	}
	if err != nil {
		err = errors.NewErrorf("%s [%s] req: %d - %s", remoteAddr, p.GetOpMsg(),
			p.GetReqID(), err.Error())
	}
	return
}

// Reply operation results to the master.
func (l *LcNode) respondToMaster(task *proto.AdminTask) (err error) {
	// handle panic
	defer func() {
		if r := recover(); r != nil {
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()
	if err = l.mc.NodeAPI().ResponseLcNodeTask(task); err != nil {
		err = errors.Trace(err, "try respondToMaster failed")
	}
	return
}

type fileDentries struct {
	sync.RWMutex
	dentries []*proto.ScanDentry
}

func newFileDentries() *fileDentries {
	return &fileDentries{
		dentries: make([]*proto.ScanDentry, 0),
	}
}

func (f *fileDentries) GetDentries() []*proto.ScanDentry {
	f.RLock()
	defer f.RUnlock()
	return f.dentries
}

func (f *fileDentries) Append(dentry *proto.ScanDentry) {
	f.Lock()
	defer f.Unlock()
	f.dentries = append(f.dentries, dentry)
}

func (f *fileDentries) Len() int {
	f.RLock()
	defer f.RUnlock()
	return len(f.dentries)
}

func (f *fileDentries) Clear() {
	f.Lock()
	defer f.Unlock()
	f.dentries = f.dentries[:0]
}

func (l *LcNode) scanning() bool {
	l.scannerMutex.Lock()
	defer l.scannerMutex.Unlock()
	return len(l.scanners) > 0
}

func (l *LcNode) startScan(adminTask *proto.AdminTask) (err error) {

	request := adminTask.Request.(*proto.RuleTaskRequest)
	log.LogInfof("startScan: scan task(%v) of routine(%v) received!", request.Task, request.RoutineID)
	resp := &proto.RuleTaskResponse{}
	adminTask.Response = resp

	defer func() {
		if err != nil {
			resp.Status = proto.TaskFailed
			resp.Result = err.Error()
			adminTask.Request = nil
			l.respondToMaster(adminTask)
		}
	}()

	l.scannerMutex.RLock()
	if _, ok := l.scanners[request.Task.Id]; ok {
		log.LogInfof("startScan: scan task(%v) of routine(%v) is already running!", request.Task, request.RoutineID)
		l.scannerMutex.RUnlock()
		return nil
	}
	l.scannerMutex.RUnlock()

	var scanner *TaskScanner
	scanner, err = NewTaskScanner(request.Task, request.RoutineID, l.masters)
	if err != nil {
		log.LogErrorf("startScan: NewTaskScanner err(%v)", err)
		return err
	}

	perfixInode, mode, err := scanner.FindPrefixInode()
	if err != nil {
		log.LogWarnf("startScan: node path(%v) found in volume(%v), err(%v), scanning done!",
			scanner.filter.Prefix, request.Task.VolName, err)
		t := time.Now()
		resp.EndTime = &t
		resp.Status = proto.TaskSucceeds

		l.scannerMutex.Lock()
		delete(l.scanners, scanner.ID)
		l.scannerMutex.Unlock()

		l.respondToMaster(adminTask)
		return nil
	}

	go scanner.scan()

	prefixDentry := &proto.ScanDentry{
		Inode: perfixInode,
		Type:  mode,
	}

	t := time.Now()
	resp.StartTime = &t
	if os.FileMode(mode).IsDir() {
		log.LogDebugf("startScan: first dir entry(%v) in!", prefixDentry)
		scanner.dirChan.In <- prefixDentry
	} else {
		log.LogDebugf("startScan: first file entry(%v) in!", prefixDentry)
		scanner.fileChan.In <- prefixDentry
	}

	if scanner.abortFilter != nil {
		scanMultipart := func() {
			scanner.incompleteMultiPartScan(resp)
		}
		_, _ = scanner.fileRPoll.Submit(scanMultipart)
	}

	go scanner.checkScanning(adminTask, resp, l)

	l.scannerMutex.Lock()
	l.scanners[scanner.ID] = scanner
	l.scannerMutex.Unlock()

	return err
}

func (l *LcNode) opLcScan(conn net.Conn, p *proto.Packet, remoteAddr string) (err error) {
	go func() {
		p.PacketOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()
	data := p.Data
	var (
		req       = &proto.RuleTaskRequest{}
		resp      = &proto.RuleTaskResponse{}
		adminTask = &proto.AdminTask{
			Request: req,
		}
	)

	decoder := json.NewDecoder(bytes.NewBuffer(data))
	decoder.UseNumber()
	if err = decoder.Decode(adminTask); err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		//adminTask.Request = nil
		adminTask.Response = resp
		_ = l.respondToMaster(adminTask)
	} else {
		err = l.startScan(adminTask)
	}

	return
}

func (l *LcNode) opMasterHeartbeat(conn net.Conn, p *proto.Packet, remoteAddr string) (err error) {
	go func() {
		p.PacketOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()

	data := p.Data
	var (
		req  = &proto.HeartBeatRequest{}
		resp = &proto.LcNodeHeartbeatResponse{
			ScanningTasks: make(map[string]*proto.ScanInfo, 0),
		}
		adminTask = &proto.AdminTask{
			Request: req,
		}
	)

	go func() {
		start := time.Now()
		decode := json.NewDecoder(bytes.NewBuffer(data))
		decode.UseNumber()
		if err = decode.Decode(adminTask); err != nil {
			resp.Status = proto.TaskFailed
			resp.Result = err.Error()
			goto end
		}
		//collect status?

		l.scannerMutex.RLock()
		for _, scanner := range l.scanners {
			info := &proto.ScanInfo{
				ScanTaskInfo: proto.ScanTaskInfo{
					Id:        scanner.ID,
					RoutineId: scanner.RoutineID,
				},
				TaskStatistics: proto.TaskStatistics{
					Volume:                        scanner.Volume,
					Prefix:                        scanner.filter.Prefix,
					TotalInodeScannedNum:          atomic.LoadInt64(&scanner.currentStat.TotalInodeScannedNum),
					FileScannedNum:                atomic.LoadInt64(&scanner.currentStat.FileScannedNum),
					DirScannedNum:                 atomic.LoadInt64(&scanner.currentStat.DirScannedNum),
					ExpiredNum:                    atomic.LoadInt64(&scanner.currentStat.ExpiredNum),
					ErrorSkippedNum:               atomic.LoadInt64(&scanner.currentStat.ErrorSkippedNum),
					AbortedIncompleteMultipartNum: atomic.LoadInt64(&scanner.currentStat.AbortedIncompleteMultipartNum),
				},
			}
			resp.ScanningTasks[scanner.ID] = info
		}
		l.scannerMutex.RUnlock()

		resp.Status = proto.TaskSucceeds
	end:
		adminTask.Request = nil
		adminTask.Response = resp
		l.respondToMaster(adminTask)

		data, _ := json.Marshal(resp)
		log.LogInfof("%s pkt %s, resp success req:%v; respAdminTask: %v, resp: %v, cost %s",
			remoteAddr, p.String(), req, adminTask, string(data), time.Since(start).String())
	}()
	l.lastHeartbeat = time.Now()
	return
}
