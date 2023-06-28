package datanode

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/exporter"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/util/log"

	"github.com/cubefs/cubefs/storage"
)

var (
	ErrIllegalFragmentLength    = errors.New("illegal issue data fragment length")
	ErrBrokenIssueFragmentsFile = errors.New("broken issue fragments file")
	ErrBrokenIssueFragmentData  = errors.New("broken issue fragment data")
	ErrCannotFixFragment        = errors.New("can not fix issue fragment")
	ErrPendingFixFragment       = errors.New("pending fix issue fragment")
)

const (
	issueFragmentsFilename    = "ISSUE_FRAGMENTS"
	issueFragmentsTmpFilename = ".ISSUE_FRAGMENTS"
	issueFragmentBinaryLength = 28
)

type dataRef struct {
	data []byte
	ref  int
}

// innerError 类型是 IssueProcessor 内部使用的错误类型，用来对检查及修复数据时发生的错误进行收敛归类，以分辨提取数据时是无数据还是被拒绝。
type innerError int

const (
	innerErrorFetchDoData innerError = iota
	innerErrorFetchRejected
)

func (i innerError) Error() string {
	switch i {
	case innerErrorFetchDoData:
		return "inner error: fetch no data"
	case innerErrorFetchRejected:
		return "inner error: fetch rejected"
	}
	return "inner error: unknown"
}

type GetRemoteHostsFunc func() []string

type IssueFragment struct {
	extentID uint64
	offset   uint64
	size     uint64
}

func (f *IssueFragment) Overlap(extentID, offset, size uint64) bool {
	return extentID == f.extentID && offset+size > f.offset && f.offset+f.size > offset
}

func (f *IssueFragment) Covered(extentID, offset, size uint64) bool {
	return extentID == f.extentID && offset <= f.offset && offset+size >= f.offset+f.size
}

func (f *IssueFragment) EncodeTo(b []byte) (err error) {
	if len(b) < issueFragmentBinaryLength {
		return ErrIllegalFragmentLength
	}
	binary.BigEndian.PutUint64(b[:8], f.extentID)
	binary.BigEndian.PutUint64(b[8:16], f.offset)
	binary.BigEndian.PutUint64(b[16:24], f.size)
	binary.BigEndian.PutUint32(b[24:28], crc32.ChecksumIEEE(b[:24]))
	return nil
}

func (f *IssueFragment) EncodeLength() int {
	return issueFragmentBinaryLength
}

func (f *IssueFragment) Equals(o *IssueFragment) bool {
	return f.extentID == o.extentID && f.offset == o.offset && f.size == o.size
}

func (f *IssueFragment) DecodeFrom(b []byte) error {
	if len(b) < issueFragmentBinaryLength {
		return ErrIllegalFragmentLength
	}
	if crc32.ChecksumIEEE(b[:24]) != binary.BigEndian.Uint32(b[24:28]) {
		return ErrBrokenIssueFragmentData
	}
	f.extentID = binary.BigEndian.Uint64(b[:8])
	f.offset = binary.BigEndian.Uint64(b[8:16])
	f.size = binary.BigEndian.Uint64(b[16:24])
	return nil
}

// IssueProcessor 是用于系统级宕机引起的数据检查及损坏修复的处理器。
// 它具备以下几个功能:
// 1. 注册可能有损坏的数据区域
// 2. 判断给定数据区域是否在已注册的疑似损坏数据区域内
// 3. 检查并尝试修复疑似被损坏的数据区域.
type IssueProcessor struct {
	path           string
	partitionID    uint64
	fragments      []*IssueFragment
	mu             sync.RWMutex
	storage        *storage.ExtentStore
	getRemoteHosts func() []string
	persistSyncCh  chan struct{}
	stopCh         chan struct{}
	stopOnce       sync.Once
}

func (p *IssueProcessor) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
	return
}

func (p *IssueProcessor) isStopped() bool {
	select {
	case <-p.stopCh:
		return true
	default:
	}
	return false
}

func (p *IssueProcessor) lockPersist() (release func()) {
	p.persistSyncCh <- struct{}{}
	release = func() {
		<-p.persistSyncCh
	}
	return
}

func (p *IssueProcessor) worker() {
	var err error
	var pendingTimer = time.NewTimer(0)
	for p.getIssueFragmentCount() > 0 {
		select {
		case <-p.stopCh:
			return
		case <-pendingTimer.C:
		}
		var fragments = p.copyIssueFragments()
		if len(fragments) > 0 {
			log.LogWarnf("IssueProcessor: Partition(%v) start to check and fix %v issue fragments: %v",
				p.partitionID, p.getIssueFragmentCount(),
				func() string {
					var sb = strings.Builder{}
					for _, fragment := range fragments {
						if sb.Len() > 0 {
							sb.WriteString(", ")
						}
						sb.WriteString(fmt.Sprintf("ExtentID(%v)_Offset(%v)_Size(%v)",
							fragment.extentID, fragment.offset, fragment.size))
					}
					return sb.String()
				}())
		}

		err = p.checkAndFixFragments(p.copyIssueFragments())
		if err != nil {
			log.LogErrorf("IssueProcessor: Partition(%v) failed on check and fix fragments and will be retry after 30s. ", p.partitionID)
			pendingTimer.Reset(time.Second * 30)
			continue
		}
		if p.getIssueFragmentCount() > 0 {
			pendingTimer.Reset(time.Second * 10)
		}
	}
}

func (p *IssueProcessor) copyIssueFragments() []*IssueFragment {
	var fragments []*IssueFragment
	p.mu.RLock()
	defer p.mu.RUnlock()
	fragments = make([]*IssueFragment, len(p.fragments))
	copy(fragments, p.fragments)
	return fragments
}

func (p *IssueProcessor) getIssueFragmentCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.fragments)
}

func (p *IssueProcessor) fetchLocal(extentID, offset, size uint64) ([]byte, error) {
	var err error
	var buf = make([]byte, size)
	if proto.IsTinyExtent(extentID) {
		var newOffset int64
		newOffset, _, err = p.storage.TinyExtentAvaliOffset(extentID, int64(offset))
		if err != nil {
			return nil, err
		}
		if newOffset > int64(offset) { // This page is a hole
			return nil, innerErrorFetchDoData
		}
	}
	if _, err = p.storage.Read(extentID, int64(offset), int64(size), buf, false); err != nil {
		return nil, err
	}
	return buf, nil
}

func (p *IssueProcessor) fetchRemote(host string, extentID, offset, size uint64, force bool) ([]byte, error) {
	var err error
	var packet = repl.NewExtentRepairReadPacket(nil, p.partitionID, extentID, int(offset), int(size), force)
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(host)
	if err != nil {
		return nil, err
	}
	defer gConnPool.PutConnect(conn, true)

	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return nil, err
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		return nil, err
	}
	if packet.ResultCode != proto.OpOk {
		var msg = string(packet.Data[:packet.Size])
		switch {
		case strings.Contains(msg, proto.ExtentNotFoundError.Error()), strings.Contains(msg, io.EOF.Error()):
			return nil, innerErrorFetchDoData
		case strings.Contains(msg, proto.ErrOperationDisabled.Error()):
			return nil, innerErrorFetchRejected
		default:
		}
		return nil, errors.New(string(packet.Data[:packet.Size]))
	}
	return packet.Data[:packet.Size], nil
}

func (p *IssueProcessor) fetchRemoteTiny(host string, extentID, offset, size uint64, force bool) ([]byte, error) {
	var err error
	var packet = repl.NewTinyExtentRepairReadPacket(nil, p.partitionID, extentID, int(offset), int(size), force)
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(host)
	if err != nil {
		return nil, err
	}
	defer gConnPool.PutConnect(conn, true)

	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return nil, err
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		return nil, err
	}
	if packet.ResultCode != proto.OpOk {
		var msg = string(packet.Data[:packet.Size])
		switch {
		case strings.Contains(msg, proto.ExtentNotFoundError.Error()), strings.Contains(msg, io.EOF.Error()):
			return nil, innerErrorFetchDoData
		case strings.Contains(msg, proto.ErrOperationDisabled.Error()):
			return nil, innerErrorFetchRejected
		default:
		}
		return nil, errors.New(string(packet.Data[:packet.Size]))
	}
	if isEmptyResponse := len(packet.Arg) > 0 && packet.Arg[0] == EmptyResponse; isEmptyResponse {
		return nil, nil
	}
	return packet.Data[:packet.Size], nil
}

func (p *IssueProcessor) checkAndFixFragments(fragments []*IssueFragment) error {
	var err error
	for _, fragment := range fragments {
		if p.isStopped() {
			return nil
		}
		err = p.checkAndFixFragment(fragment)
		switch err {
		case nil:
			// 将修复成功和无法修复的片段移除待修复列表
			if p.removeFragment(fragment) {
				if err = p.persistIssueFragments(); err != nil {
					return err
				}
			}
		case ErrCannotFixFragment:
			// 该数据片段无法修复，进行报警
			log.LogCriticalf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) can not fix", p.partitionID, fragment.extentID, fragment.offset, fragment.size)
			exporter.Warning(fmt.Sprintf("CAN NOT FIX BROKEN EXTENT!\n"+
				"Found issue data fragment cause server fault and can not fix it.\n"+
				"Partition: %v\n"+
				"Extent: %v\n"+
				"Offset: %v\n"+
				"Size: %v",
				p.partitionID, fragment.extentID, fragment.offset, fragment.size))
		default:
			log.LogErrorf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) can not fix temporary and will be retry later", p.partitionID, fragment.extentID, fragment.offset, fragment.size)
		}
	}
	return nil
}

func (p *IssueProcessor) checkAndFixFragment(fragment *IssueFragment) error {
	var (
		err error

		extentID = fragment.extentID
		offset   = fragment.offset
		size     = fragment.size
	)
	var (
		localData []byte
		localCrc  uint32
	)
	if localData, err = p.fetchLocal(extentID, offset, fragment.size); err != nil {
		log.LogErrorf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) fetch local data failed: %v",
			p.partitionID, extentID, offset, size, err)
		return ErrPendingFixFragment
	}
	if len(localData) != 0 {
		localCrc = crc32.ChecksumIEEE(localData)
	}

	// Step 1.
	// 首先使用可信数据机制进行修复
	// 可信数据机制，由于远端已经组织了对疑似损坏的数据的读请求，所以成功读到的任何一个副本数据都可以认为是正确的。
	// 只需要和本地数据比较确认数据覆盖以及进行报警即可.
	var remoteHosts = p.getRemoteHosts()
	var (
		noDataErrorCnt  int
		disableErrorCnt int
		otherErrorCnt   int
	)
	for _, remote := range remoteHosts {
		var (
			remoteData []byte
			fetchErr   error
		)
		if proto.IsTinyExtent(fragment.extentID) {
			remoteData, fetchErr = p.fetchRemoteTiny(remote, extentID, offset, size, false)
		} else {
			remoteData, fetchErr = p.fetchRemote(remote, extentID, offset, size, false)
		}
		switch {
		case fetchErr == nil && len(remoteData) > 0:
			// 成功获取到有效的可信副本数据
			var remoteCrc = crc32.ChecksumIEEE(remoteData)
			if remoteCrc != localCrc {
				// 本地数据与可信副本数据存在差异，使用可信副本数据覆盖本地数据。
				if err = p.storage.Write(context.Background(), extentID, int64(offset), int64(size), remoteData, remoteCrc, storage.RandomWriteType, true); err != nil {
					// 覆盖本地数据时出错，延迟修复
					log.LogErrorf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) fix CRC(%v -> %v) failed: %v", p.partitionID, extentID, offset, size, localCrc, remoteCrc, err)
					return ErrPendingFixFragment
				}
				log.LogWarnf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) fix CRC(%v -> %v) success", p.partitionID, extentID, offset, size, localCrc, remoteCrc)
				return nil
			}
			// 本地数据和可信副本数据一致，不需要修改。直接通知上层数据完成修复。
			log.LogWarnf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) no need to fix cause CRC same with safety remote(%v)", p.partitionID, extentID, offset, size, remote)
			return nil
		case fetchErr == nil && len(remoteData) == 0, fetchErr == innerErrorFetchDoData:
			log.LogWarnf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) does not fetched enough data from remote(%v) ", p.partitionID, extentID, offset, size, remote)
			noDataErrorCnt++
		case fetchErr == innerErrorFetchRejected:
			log.LogWarnf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) fetch data rejected by remote(%v)", p.partitionID, extentID, offset, size, remote)
			disableErrorCnt++
		default:
			log.LogWarnf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) fetch data failed from remote(%v): %v", p.partitionID, extentID, offset, size, remote, fetchErr)
			otherErrorCnt++
		}
	}
	switch {
	case noDataErrorCnt == len(remoteHosts):
		// 除本地外的所有副本均汇报无该段数据，则认定本地数据无需修复
		log.LogWarnf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) no need to fix cause all remote report no exists", p.partitionID, extentID, offset, size)
		return nil
	case otherErrorCnt > 0:
		// 存在暂时无法响应的副本, 延迟重试
		return ErrPendingFixFragment
	default:
	}

	// Step 2.
	// 所有副本均响应但均无法响应有效数据，进入Quorum比对数据机制，通过强制拉取副本数据并汇总复制组数据版本，比较出超半数版本，以超半数版本进行修复。
	var dataRefs = make(map[uint32]*dataRef)
	dataRefs[localCrc] = &dataRef{
		data: localData,
		ref:  1,
	}
	for _, remote := range remoteHosts {
		var (
			remoteData []byte
			fetchErr   error
		)
		if proto.IsTinyExtent(fragment.extentID) {
			remoteData, fetchErr = p.fetchRemoteTiny(remote, extentID, offset, size, true)
		} else {
			remoteData, fetchErr = p.fetchRemote(remote, extentID, offset, size, true)
		}
		switch {
		case fetchErr == nil:
		case fetchErr == innerErrorFetchDoData:
			remoteData = nil
		case fetchErr != nil:
			return ErrPendingFixFragment
		}
		var remoteCrc uint32
		if len(remoteData) > 0 {
			remoteCrc = crc32.ChecksumIEEE(remoteData)
		}
		ref, found := dataRefs[remoteCrc]
		if !found {
			dataRefs[remoteCrc] = &dataRef{
				data: remoteData,
				ref:  1,
			}
		} else {
			ref.ref++
		}
	}

	log.LogWarnf("checkAndFixFragment: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) collected data ref: %v",
		p.partitionID, extentID, offset, size, func() string {
			var sb = strings.Builder{}
			for crc, ref := range dataRefs {
				if sb.Len() > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(fmt.Sprintf("CRC(%v)_Ref(%v)", crc, ref.ref))
			}
			return sb.String()
		}())

	var quorum = (len(remoteHosts)+1)/2 + 1
	for crc, ref := range dataRefs {
		if ref.ref >= quorum {
			// 找到了超半数版本, 使用该版本数据
			if crc != 0 && crc != localCrc {
				// 仅在目标数据非空洞且有效长度超过本地数据的情况下进行修复
				if err = p.storage.Write(context.Background(), extentID, int64(offset), int64(size), ref.data[:size], crc, storage.RandomWriteType, true); err != nil {
					// 覆盖本地数据时出错，延迟修复
					log.LogErrorf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) fix CRC(%v -> %v) failed: %v", p.partitionID, extentID, offset, size, localCrc, crc, err)
					return ErrPendingFixFragment
				}
			}
			log.LogWarnf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) skip fix cause quorum version same as local or empty.", p.partitionID, extentID, offset, size)
			return nil
		}
	}
	log.LogErrorf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) can not determine correct data by quorum",
		p.partitionID, extentID, offset, size)
	return ErrCannotFixFragment
}

func (p *IssueProcessor) loadIssueFragments() (err error) {
	var originalFilepath = path.Join(p.path, issueFragmentsFilename)
	var originalFile *os.File
	originalFile, err = os.OpenFile(originalFilepath, os.O_RDONLY, os.ModePerm)
	switch {
	case os.IsNotExist(err):
		err = nil
		return
	case err != nil:
		return
	default:
	}
	defer func() {
		_ = originalFile.Close()
	}()

	var info os.FileInfo
	if info, err = originalFile.Stat(); err != nil || info.Size() == 0 {
		return
	}
	var fragments = make([]*IssueFragment, 0)
	var bufR = bufio.NewReader(originalFile)
	var buf = make([]byte, issueFragmentBinaryLength)
	var n int
	for {
		n, err = bufR.Read(buf)
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return
		}
		if n != issueFragmentBinaryLength {
			err = ErrBrokenIssueFragmentsFile
			return
		}
		var fragment = new(IssueFragment)
		if err = fragment.DecodeFrom(buf[:n]); err != nil {
			return
		}
		fragments = append(fragments, fragment)
	}
	p.fragments = fragments
	return
}

func (p *IssueProcessor) persistIssueFragments() (err error) {
	var release = p.lockPersist()
	defer release()
	var (
		fragments        = p.copyIssueFragments()
		tmpFilepath      = path.Join(p.path, issueFragmentsTmpFilename)
		originalFilepath = path.Join(p.path, issueFragmentsFilename)
	)
	if len(fragments) == 0 {
		_ = os.Remove(originalFilepath)
		return
	}
	var tmpFile *os.File
	if tmpFile, err = os.OpenFile(tmpFilepath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm); err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpFilepath)
		}
	}()
	var bufW = bufio.NewWriter(tmpFile)
	var buf = make([]byte, issueFragmentBinaryLength)
	for _, fragment := range fragments {
		_ = fragment.EncodeTo(buf)
		if _, err = bufW.Write(buf); err != nil {
			return
		}
	}
	if err = bufW.Flush(); err != nil {
		return
	}
	if err = tmpFile.Sync(); err != nil {
		return
	}
	_ = tmpFile.Close()
	err = os.Rename(tmpFilepath, originalFilepath)
	return
}

func (p *IssueProcessor) RemoveByExtent(extentID uint64) error {
	if p.removeExtent(extentID) {
		if err := p.persistIssueFragments(); err != nil {
			return err
		}
	}
	return nil
}

func (p *IssueProcessor) RemoveByRange(extentID, offset, size uint64) error {
	if p.removeCovered(extentID, offset, size) {
		if err := p.persistIssueFragments(); err != nil {
			return err
		}
	}
	return nil
}

func (p *IssueProcessor) removeExtent(extentID uint64) (removed bool) {
	if len(p.fragments) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var i = 0
	for i < len(p.fragments) {
		if p.fragments[i].extentID != extentID {
			i++
			continue
		}
		switch {
		case len(p.fragments) == i+1:
			p.fragments = p.fragments[:i]
		case i == 0:
			p.fragments = p.fragments[1:]
		default:
			p.fragments = append(p.fragments[:i], p.fragments[i+1:]...)
		}
		removed = true
	}
	return
}

func (p *IssueProcessor) removeCovered(extentID, offset, size uint64) (removed bool) {
	if len(p.fragments) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var i = 0
	for i < len(p.fragments) {
		if !p.fragments[i].Covered(extentID, offset, size) {
			i++
			continue
		}
		switch {
		case len(p.fragments) == i+1:
			p.fragments = p.fragments[:i]
		case i == 0:
			p.fragments = p.fragments[1:]
		default:
			p.fragments = append(p.fragments[:i], p.fragments[i+1:]...)
		}
		removed = true
	}
	return
}

func (p *IssueProcessor) addFragment(fragment *IssueFragment) (added bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 0; i < len(p.fragments); i++ {
		if p.fragments[i].Equals(fragment) {
			return
		}
	}
	p.fragments = append(p.fragments, fragment)
	added = true
	return
}

func (p *IssueProcessor) removeFragment(fragment *IssueFragment) (removed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var i = 0
	for i < len(p.fragments) {
		if !p.fragments[i].Equals(fragment) {
			i++
			continue
		}
		switch {
		case len(p.fragments) == i+1:
			p.fragments = p.fragments[:i]
		case i == 0:
			p.fragments = p.fragments[1:]
		default:
			p.fragments = append(p.fragments[:i], p.fragments[i+1:]...)
		}
		return true
	}
	return false
}

func (p *IssueProcessor) FindOverlap(extentID, offset, size uint64) bool {
	if len(p.fragments) == 0 {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for i := 0; i < len(p.fragments); i++ {
		if p.fragments[i].Overlap(extentID, offset, size) {
			return true
		}
	}
	return false
}

func NewIssueProcessor(partitionID uint64, path string, storage *storage.ExtentStore, getRemotes func() []string, fragments []*IssueFragment) (*IssueProcessor, error) {
	var p = &IssueProcessor{
		partitionID:    partitionID,
		path:           path,
		storage:        storage,
		getRemoteHosts: getRemotes,
		persistSyncCh:  make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}
	var err error
	if err = p.loadIssueFragments(); err != nil {
		return nil, err
	}
	if len(fragments) > 0 {
		var added bool
		for _, fragment := range fragments {
			if p.addFragment(fragment) {
				added = true
			}
		}
		if added {
			if err = p.persistIssueFragments(); err != nil {
				return nil, err
			}
		}
	}

	if p.getIssueFragmentCount() > 0 {
		go p.worker()
	}
	return p, nil
}
