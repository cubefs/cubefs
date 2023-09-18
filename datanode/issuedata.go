package datanode

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/async"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

var (
	ErrIllegalFragmentLength    = errors.New("illegal issue data fragment length")
	ErrBrokenIssueFragmentsFile = errors.New("broken issue fragments file")
	ErrBrokenIssueFragmentData  = errors.New("broken issue fragment data")
)

const (
	issueFragmentsFilename    = "ISSUE_FRAGMENTS"
	issueFragmentsTmpFilename = ".ISSUE_FRAGMENTS"
	issueFragmentBinaryLength = 28

	fixedIssueFragmentsFilename = "FIXED_ISSUE_FRAGMENTS"
)

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

		p.checkAndFixFragments(p.copyIssueFragments())
		if p.getIssueFragmentCount() > 0 {
			pendingTimer.Reset(time.Second * 10)
		}
	}
}

func (p *IssueProcessor) handleWorkerPanic(i interface{}) {
	// Worker 发生panic，进行报警
	var callstack = string(debug.Stack())
	log.LogCriticalf("IssueProcessor: Partition(%v) fix worker occurred panic and stopped: %v\n"+
		"Callstack: %v\n", p.partitionID, i, callstack)
	exporter.Warning(fmt.Sprintf("ISSUE PROCESSOR WORKER PANIC!\n"+
		"Fix worker occurred panic and stopped:\n"+
		"Partition: %v\n"+
		"Message  : %v\n",
		p.partitionID, i))
	return
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

func (p *IssueProcessor) computeLocalCRC(fragment *IssueFragment) (crc uint32, err error) {
	var (
		extentID   = fragment.extentID
		offset     = fragment.offset
		size       = fragment.size
		buf        = make([]byte, unit.BlockSize)
		remain     = int64(size)
		ieee       = crc32.NewIEEE()
		readOffset = int64(offset)
	)

	for remain > 0 {
		var readSize = int64(math.Min(float64(remain), float64(unit.BlockSize)))
		_, err = p.storage.Read(extentID, int64(offset), readSize, buf[:readSize], false)
		switch {
		case err == proto.ExtentNotFoundError,
			os.IsNotExist(err),
			err == io.EOF,
			err != nil && strings.Contains(err.Error(), "parameter mismatch"):
			return 0, nil
		case err != nil:
			return 0, err
		default:
		}
		if _, err = ieee.Write(buf[:readSize]); err != nil {
			return 0, err
		}
		readOffset += readSize
		remain -= readSize
	}
	return ieee.Sum32(), nil
}

func (p *IssueProcessor) fetchRemoteDataToLocalFile(f *os.File, host string, extentID, offset, size uint64, force bool) (crc uint32, rejected bool, err error) {

	var readOffset = int(offset)
	var readSize = int(size)
	var remain = int64(size)
	request := repl.NewExtentRepairReadPacket(context.Background(), p.partitionID, extentID, readOffset, readSize, force)
	if proto.IsTinyExtent(extentID) {
		request = repl.NewTinyExtentRepairReadPacket(context.Background(), p.partitionID, extentID, readOffset, readSize, force)
	}
	var conn *net.TCPConn
	if conn, err = gConnPool.GetConnect(host); err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)

	if err = request.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return
	}
	var fileOffset int64 = 0

	var buf = make([]byte, unit.BlockSize)
	var getReplyDataBuffer = func(size uint32) []byte {
		if int(size) > cap(buf) {
			return make([]byte, size)
		}
		return buf[:size]
	}

	var ieee = crc32.NewIEEE()
	for remain > 0 {
		reply := repl.NewPacket(context.Background())
		if err = reply.ReadFromConnWithSpecifiedDataBuffer(conn, 60, getReplyDataBuffer); err != nil {
			return
		}

		if reply.ResultCode != proto.OpOk {
			var msg = string(reply.Data[:reply.Size])
			switch {
			case strings.Contains(msg, proto.ExtentNotFoundError.Error()),
				strings.Contains(msg, io.EOF.Error()),
				strings.Contains(msg, "parameter mismatch"):
				return 0, false, nil
			case strings.Contains(msg, proto.ErrOperationDisabled.Error()):
				return 0, true, nil
			default:
			}
			return 0, false, errors.New(msg)
		}

		// Write it to local extent file
		var writeSize = int64(reply.Size)
		if proto.IsTinyExtent(extentID) {
			if isEmptyResponse := len(reply.Arg) > 0 && reply.Arg[0] == EmptyResponse; isEmptyResponse {
				if reply.KernelOffset > 0 && reply.KernelOffset != uint64(crc32.ChecksumIEEE(reply.Arg)) {
					return 0, false, errors.New("CRC mismatch")
				}
				writeSize = int64(binary.BigEndian.Uint64(reply.Arg[1:9]))
				if err = f.Truncate(fileOffset + writeSize); err != nil {
					return
				}
				for i := int64(0); i < writeSize; i++ {
					if _, err = ieee.Write([]byte{0}); err != nil {
						return 0, false, err
					}
				}
				fileOffset += writeSize
				remain -= writeSize
				continue
			}
		}
		if _, err = f.WriteAt(reply.Data[:reply.Size], fileOffset); err != nil {
			return 0, false, err
		}
		if _, err = ieee.Write(reply.Data[:reply.Size]); err != nil {
			return 0, false, err
		}
		fileOffset += int64(reply.Size)
		remain -= int64(reply.Size)
	}
	return ieee.Sum32(), false, nil
}

func (p *IssueProcessor) checkAndFixFragments(fragments []*IssueFragment) {
	for _, fragment := range fragments {
		if p.isStopped() {
			return
		}
		var success, err = p.checkAndFixFragment(fragment)
		if err != nil {
			log.LogErrorf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) can not fix temporary and will be retry later", p.partitionID, fragment.extentID, fragment.offset, fragment.size)
			continue
		}
		if !success {
			// 该数据片段无法修复，进行报警
			log.LogCriticalf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) can not fix", p.partitionID, fragment.extentID, fragment.offset, fragment.size)
			exporter.Warning(fmt.Sprintf("CAN NOT FIX BROKEN EXTENT!\n"+
				"Found issue data fragment cause server fault and can not fix it.\n"+
				"Partition: %v\n"+
				"Extent: %v\n"+
				"Offset: %v\n"+
				"Size: %v",
				p.partitionID, fragment.extentID, fragment.offset, fragment.size))
			continue
		}
		if p.removeFragment(fragment) {
			if err = p.recordFixedFragment(fragment); err != nil {
				log.LogWarnf("IssueProcessor: record fixed fragment failed: %v", err)
			}
			if err = p.persistIssueFragments(); err != nil {
				log.LogErrorf("IssueProcessor: persist issue fragments failed: %v", err)
			}
		}
	}
	return
}

// 使用可信副本策略对指定数据片段进行修复
func (p *IssueProcessor) checkAndFixFragmentByTrustVersionPolicy(hosts []string, localCrc uint32, fragment *IssueFragment) (crc uint32, success bool, err error) {
	var (
		extentID         = fragment.extentID
		offset           = fragment.offset
		size             = fragment.size
		rejects          int
		failures         []error
		tempFiles        = make([]*os.File, 0, len(hosts))
		registerTempFile = func(f *os.File) {
			tempFiles = append(tempFiles, f)
		}
	)

	defer func() {
		for _, tempFile := range tempFiles {
			_ = tempFile.Close()
			_ = os.Remove(tempFile.Name())
		}
	}()
	for _, host := range hosts {
		var tempFile *os.File
		if tempFile, err = p.createRepairTmpFile(host, extentID, offset, size); err != nil {
			return 0, false, err
		}
		registerTempFile(tempFile)
		var remoteCRC, rejected, fetchedErr = p.fetchRemoteDataToLocalFile(tempFile, host, extentID, offset, size, false)
		if fetchedErr != nil {
			failures = append(failures, fmt.Errorf("fetch extent data (partition=%v, extent=%v, offset=%v, size=%v) from %v failed: %v",
				p.partitionID, extentID, offset, size, host, fetchedErr))
			continue
		}
		if rejected {
			rejects++
			continue
		}
		if remoteCRC == 0 {
			continue
		}
		if remoteCRC != localCrc {
			if err = p.applyTempFileToExtent(tempFile, extentID, offset, size); err != nil {
				return 0, false, err
			}
		}
		return remoteCRC, true, nil
	}
	if len(failures) > 0 {
		// 存在失败远端，稍后重试
		return 0, false, failures[0]
	}
	if rejects == 0 {
		// 所有远端均汇报无数据, 无需修复
		return 0, true, nil
	}
	// 当前修复策略无法认定数据是否安全及进行修复, 由下一个策略继续处理
	return 0, false, nil
}

// 使用超半数版本策略对制定数据片段进行修复
func (p *IssueProcessor) checkAndFixFragmentByQuorumVersionPolicy(hosts []string, localCrc uint32, fragment *IssueFragment) (crc uint32, success bool, err error) {
	var (
		extentID         = fragment.extentID
		offset           = fragment.offset
		size             = fragment.size
		failures         int
		versions         = make(map[uint32][]*os.File) // crc -> temp files
		tempFiles        = make([]*os.File, 0, len(hosts))
		registerTempFile = func(f *os.File) {
			tempFiles = append(tempFiles, f)
		}
	)

	defer func() {
		for _, tempFile := range tempFiles {
			_ = tempFile.Close()
			_ = os.Remove(tempFile.Name())
		}
	}()
	// 从远端收集数据
	for _, host := range hosts {
		var tempFile *os.File
		if tempFile, err = p.createRepairTmpFile(host, extentID, offset, size); err != nil {
			return 0, false, err
		}
		registerTempFile(tempFile)
		var remoteCRC, _, fetchedErr = p.fetchRemoteDataToLocalFile(tempFile, host, extentID, offset, size, true)
		if fetchedErr != nil {
			failures++
			continue
		}
		versions[remoteCRC] = append(versions[remoteCRC], tempFile)
	}
	var quorum = (len(hosts)+1)/2 + 1
	for crc, files := range versions {
		if len(files) >= quorum {
			// 找到了超半数版本, 使用该版本数据
			if crc != 0 && crc != localCrc {
				// 仅在目标数据非空洞且有效长度超过本地数据的情况下进行修复
				if err = p.applyTempFileToExtent(files[0], extentID, offset, size); err != nil {
					// 覆盖本地数据时出错，延迟修复
					log.LogErrorf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) fix CRC(%v -> %v) failed: %v", p.partitionID, extentID, offset, size, localCrc, crc, err)
					return 0, false, err
				}
			}
			log.LogWarnf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) skip fix cause quorum version same as local or empty.", p.partitionID, extentID, offset, size)
			return crc, true, nil
		}
	}
	log.LogErrorf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) can not determine correct data by quorum",
		p.partitionID, extentID, offset, size)
	return 0, false, nil
}

func (p *IssueProcessor) applyTempFileToExtent(f *os.File, extentID, offset, size uint64) (err error) {

	var (
		tempFileOffset int64
		extentOffset   = int64(offset)
		buf            = make([]byte, unit.BlockSize)
		remain         = int64(size)
	)
	for remain > 0 {
		var readSize = remain
		if proto.IsTinyExtent(extentID) {
			var nextDataOff int64
			if nextDataOff, err = p.getFileNextDataPos(f, tempFileOffset); err != nil {
				return
			}
			if nextDataOff != tempFileOffset {
				var holeSize = nextDataOff - tempFileOffset
				remain -= holeSize
				tempFileOffset += holeSize
				extentOffset += holeSize
				continue
			}
			var nextHoleOff int64
			if nextHoleOff, err = p.getFileNextHolePos(f, tempFileOffset); err != nil {
				return
			}
			if nextHoleOff != tempFileOffset {
				readSize = int64(math.Min(float64(readSize), float64(nextHoleOff-tempFileOffset)))
			}
		}
		readSize = int64(math.Min(float64(readSize), float64(unit.BlockSize)))
		if _, err = f.ReadAt(buf[:readSize], tempFileOffset); err != nil {
			return
		}
		var crc = crc32.ChecksumIEEE(buf[:readSize])
		if err = p.storage.Write(context.Background(), extentID, extentOffset, readSize, buf[:readSize], crc, storage.RandomWriteType, true); err != nil {
			return
		}
		remain -= readSize
		tempFileOffset += readSize
		extentOffset += readSize
	}
	return
}

func (p *IssueProcessor) getFileNextDataPos(f *os.File, offset int64) (nextDataOffset int64, err error) {
	const (
		SEEK_DATA = 3
	)
	nextDataOffset, err = f.Seek(offset, SEEK_DATA)
	defer func() {
		if err != nil && strings.Contains(err.Error(), syscall.ENXIO.Error()) {
			nextDataOffset = offset
			err = nil
		}
	}()
	if err != nil {
		return
	}
	return
}

func (p *IssueProcessor) getFileNextHolePos(f *os.File, offset int64) (nextHoleOffset int64, err error) {
	const (
		SEEK_HOLE = 4
	)
	nextHoleOffset, err = f.Seek(offset, SEEK_HOLE)
	defer func() {
		if err != nil && strings.Contains(err.Error(), syscall.ENXIO.Error()) {
			nextHoleOffset = offset
			err = nil
		}
	}()
	if err != nil {
		return
	}
	return
}

func (p *IssueProcessor) createRepairTmpFile(host string, extentID, offset, size uint64) (f *os.File, err error) {
	var repairTempPath = path.Join(p.path, ".temp")
	if err = os.MkdirAll(repairTempPath, 0777); err != nil {
		return
	}
	var repairTempFilepath = path.Join(repairTempPath, fmt.Sprintf("%v_%v_%v_%v", extentID, offset, size, host))
	if f, err = os.OpenFile(repairTempFilepath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666); err != nil {
		return
	}
	return
}

func (p *IssueProcessor) checkAndFixFragment(fragment *IssueFragment) (success bool, err error) {
	var localCrc uint32
	localCrc, err = p.computeLocalCRC(fragment)
	if err != nil {
		log.LogErrorf("IssueProcessor: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) compute local CRC failed: %v",
			p.partitionID, fragment.extentID, fragment.offset, fragment.size, err)
		return false, err
	}
	if localCrc == 0 {
		// 本地数据不存在, 跳过修复
		return true, nil
	}

	type Policy struct {
		name    string
		handler func(hosts []string, localCrc uint32, fragment *IssueFragment) (crc uint32, success bool, err error)
	}

	var policies = []Policy{
		{name: "TrustVersion", handler: p.checkAndFixFragmentByTrustVersionPolicy},
		{name: "QuorumVersion", handler: p.checkAndFixFragmentByQuorumVersionPolicy},
	}

	var remoteHosts = p.getRemoteHosts()
	for _, policy := range policies {
		var crc uint32
		if crc, success, err = policy.handler(remoteHosts, localCrc, fragment); err != nil {
			log.LogErrorf("IssueProcessor: Policy(%v) fixes Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) failed: %v",
				policy.name, p.partitionID, fragment.extentID, fragment.offset, fragment.size, err)
			return false, err
		}
		if success {
			log.LogWarnf("IssueProcessor: Policy(%v) fixed Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) success, CRC(%v -> %v)",
				policy.name, p.partitionID, fragment.extentID, fragment.offset, fragment.size, localCrc, crc)
			return true, nil
		}
	}

	// 所有策略均无法修复目标
	return false, nil
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
		n, err = io.ReadFull(bufR, buf)
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

func (p *IssueProcessor) recordFixedFragment(fragment *IssueFragment) (err error) {
	var recordFilename = path.Join(p.path, fixedIssueFragmentsFilename)
	var file *os.File
	if file, err = os.OpenFile(recordFilename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666); err != nil {
		return
	}
	defer func() {
		_ = file.Close()
	}()
	if _, err = file.Write([]byte(fmt.Sprintf("%v_%v_%v\n", fragment.extentID, fragment.offset, fragment.size))); err != nil {
		return
	}
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.fragments) == 0 {
		return
	}
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.fragments) == 0 {
		return
	}
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
		async.RunWorker(p.worker, p.handleWorkerPanic)
	}
	return p, nil
}
