package storage

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/async"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

var (
	regexpRecordFile          = regexp.MustCompile("^DeletionRecord.(\\d){10}(.current)*$")
	recordFileTimestampLayout = "2006010215"
	recordEncodedLength       = 36 // crc=4,ino=8,extent=8,offset=8,Size=8

	ErrIllegalRecordLength     = errors.New("illegal DeletionRecord length")
	ErrIllegalRecordData       = errors.New("illegal DeletionRecord data")
	ErrReleasorStopped         = errors.New("extent releasor stopped")
	ErrInvalidReleasorInstance = errors.New("invalid extent releasor instance")
	ErrInvalidRecordFile       = errors.New("invalid record file")

	recordPool = sync.Pool{
		New: func() interface{} {
			return &record{}
		},
	}
	allocateRecord = func() *record {
		return recordPool.Get().(*record)
	}
	returnRecord = func(r *record) {
		recordPool.Put(r)
	}
)

type recordFileName string

func (r recordFileName) valid() bool {
	return regexpRecordFile.MatchString(string(r))
}

func (r recordFileName) isArchived() bool {
	return len(strings.Split(string(r), ".")) == 2
}

func (r recordFileName) isCurrent() bool {
	return len(strings.Split(string(r), ".")) == 3
}

func (r recordFileName) toArchive() recordFileName {
	if r.isCurrent() {
		return recordFileName(strings.Join(strings.Split(string(r), ".")[:2], "."))
	}
	return r
}

func (r recordFileName) original() string {
	return string(r)
}

func (r recordFileName) timestamp() time.Time {
	ts, _ := time.ParseInLocation(recordFileTimestampLayout, strings.Split(string(r), ".")[1], time.Local)
	return ts
}

func (r recordFileName) equals(o recordFileName) bool {
	return string(r) == string(o)
}

func parseRecordFileName(raw string) (rfn recordFileName, is bool) {
	if is = regexpRecordFile.MatchString(raw); !is {
		return
	}
	_, err := time.ParseInLocation(recordFileTimestampLayout, strings.Split(raw, ".")[1], time.Local)
	if is = err == nil; !is {
		return
	}
	rfn = recordFileName(raw)
	return
}

func newRecordFileName(timestamp time.Time, isCurrent bool) recordFileName {
	parts := []string{
		"DeletionRecord",
		timestamp.Format(recordFileTimestampLayout),
	}
	if isCurrent {
		parts = append(parts, "current")
	}
	return recordFileName(strings.Join(parts, "."))
}

type record struct {
	ino    uint64
	extent uint64
	offset uint64
	size   uint64
}

func (r *record) encodeLength() int {
	return recordEncodedLength
}

func (r *record) encodeTo(b []byte) (err error) {
	if len(b) < r.encodeLength() {
		return ErrIllegalRecordLength
	}
	binary.BigEndian.PutUint64(b[4:12], r.ino)
	binary.BigEndian.PutUint64(b[12:20], r.extent)
	binary.BigEndian.PutUint64(b[20:28], r.offset)
	binary.BigEndian.PutUint64(b[28:36], r.size)
	binary.BigEndian.PutUint32(b[0:4], crc32.ChecksumIEEE(b[4:36]))
	return
}

func (r *record) decodeFrom(b []byte) (err error) {
	if len(b) < r.encodeLength() {
		return ErrIllegalRecordLength
	}
	if binary.BigEndian.Uint32(b[0:4]) != crc32.ChecksumIEEE(b[4:36]) {
		return ErrIllegalRecordData
	}
	r.ino = binary.BigEndian.Uint64(b[4:12])
	r.extent = binary.BigEndian.Uint64(b[12:20])
	r.offset = binary.BigEndian.Uint64(b[20:28])
	r.size = binary.BigEndian.Uint64(b[28:36])
	return
}

type ExtentReleasor struct {
	path        string
	storage     *ExtentStore
	archives    []recordFileName
	archivesMu  sync.RWMutex
	current     recordFileName
	propc       chan *record
	stopc       chan struct{}
	stopOnce    sync.Once
	workerWg    sync.WaitGroup
	interceptor Interceptor
	autoApply   bool
	applyLockc  chan struct{}
}

func (r *ExtentReleasor) Stop() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		close(r.stopc)
		r.workerWg.Wait()
	})
}

func (r *ExtentReleasor) start() (err error) {
	var entries []os.DirEntry
	if entries, err = os.ReadDir(r.path); err != nil {
		return
	}
	var archives = make([]recordFileName, 0)
	var current recordFileName
	for _, entry := range entries {
		rfn, is := parseRecordFileName(entry.Name())
		if !is {
			continue
		}
		if rfn.isCurrent() {
			if time.Now().Sub(rfn.timestamp()) < time.Hour {
				current = rfn
				continue
			}
			archived := rfn.toArchive()
			oldpath := path.Join(r.path, rfn.original())
			newpath := path.Join(r.path, archived.original())
			if err = os.Rename(oldpath, newpath); err != nil {
				return
			}
			rfn = archived
		}
		archives = append(archives, rfn)
	}
	r.archives = archives
	if !current.valid() {
		current = newRecordFileName(time.Now(), true)
	}
	r.current = current
	r.workerWg.Add(1)
	async.RunWorker(r.recordWorker, r.workerPanicHandler)
	if r.autoApply {
		async.RunWorker(r.autoApplyWorker, r.workerPanicHandler)
	}
	return
}

func (r *ExtentReleasor) openRecordFile(rfn recordFileName) (*os.File, error) {
	if !rfn.valid() {
		return nil, ErrInvalidRecordFile
	}
	var filepath = path.Join(r.path, rfn.original())
	var flag int
	if rfn.isCurrent() {
		flag = os.O_CREATE | os.O_RDWR | os.O_APPEND
	} else {
		flag = os.O_RDONLY
	}
	return os.OpenFile(filepath, flag, 0666)
}

func (r *ExtentReleasor) recordWorker() {
	const (
		maxBatchSize = 128
	)

	defer r.workerWg.Done()

	var (
		err              error
		currentFp        *os.File
		currentWr        *bufio.Writer
		buf              = make([]byte, recordEncodedLength)
		checkRotateTimer *time.Timer
	)

	defer func() {
		if currentWr != nil {
			_ = currentWr.Flush()
		}
		if currentFp != nil {
			_ = currentFp.Close()
		}
	}()

	if !r.current.valid() {
		r.current = newRecordFileName(time.Now(), true)
	}

	if currentFp, err = r.openRecordFile(r.current); err != nil {
		panic(fmt.Sprintf("open record file failed: %v", err))
	}

	currentWr = bufio.NewWriter(currentFp)

	var checkRotate = func(ts time.Time) error {
		var err error
		var prev recordFileName
		if !r.current.valid() {
			r.current = newRecordFileName(ts, true)
		}
		if ts.Sub(r.current.timestamp()) >= time.Hour {
			prev = r.current
			if currentFp != nil {
				if err = currentWr.Flush(); err != nil {
					return err
				}
				if err = currentFp.Close(); err != nil {
					return err
				}
				currentFp, currentWr = nil, nil
			}
			r.current = newRecordFileName(ts, true)
		}
		if prev.valid() {
			// 结转已触发
			archived := prev.toArchive()
			if err = os.Rename(path.Join(r.path, prev.original()), path.Join(r.path, archived.original())); err != nil {
				return err
			}
			r.archivesMu.Lock()
			r.archives = append(r.archives, archived)
			r.archivesMu.Unlock()
		}
		if currentFp == nil {
			if currentFp, err = r.openRecordFile(r.current); err != nil {
				return err
			}
			currentWr = bufio.NewWriter(currentFp)
		}
		return nil
	}

	var writeRecord = func(record *record) error {
		defer func() {
			returnRecord(record)
		}()
		if cap(buf) < record.encodeLength() {
			buf = make([]byte, record.encodeLength())
		}
		if err = record.encodeTo(buf[:record.encodeLength()]); err != nil {
			return errors.New(fmt.Sprintf("encode record to bytes failed: %v", err))
		}
		if _, err = currentWr.Write(buf[:record.encodeLength()]); err != nil {
			return errors.New(fmt.Sprintf("write encoded record failed: %v", err))
		}
		return nil
	}

	var now = time.Now()
	if err = checkRotate(now); err != nil {
		panic(fmt.Sprintf("check rotate failed: %v", err))
	}

	// 计算距离下一个整小时还有多长时间, 并初始化负责唤起结转检查的定时器, 让下一个小时整点唤起结转检查.
	var (
		secsInHour           int64 = 60 * 60
		nextCheckRotateTsSec       = (now.Unix()/secsInHour + 1) * secsInHour
		nextCheckRotateTime        = time.Unix(nextCheckRotateTsSec, 0)
		elapse                     = nextCheckRotateTime.Sub(now)
	)
	checkRotateTimer = time.NewTimer(elapse)

	for {
		select {
		case <-r.stopc:
			return
		case ts := <-checkRotateTimer.C:
			if err = checkRotate(ts); err != nil {
				panic(fmt.Sprintf("check rotate failed: %v", err))
			}
			checkRotateTimer.Reset(time.Hour)
		case record := <-r.propc:
			// 每次批量处理尽可能多的记录，减少IO次数.
			if err = writeRecord(record); err != nil {
				panic(fmt.Sprintf("handle record failed: %v", err))
			}
			for i := 1; i < maxBatchSize; i++ {
				select {
				case record := <-r.propc:
					if err = writeRecord(record); err != nil {
						panic(fmt.Sprintf("handle record failed: %v", err))
					}
					continue
				default:
				}
				break
			}
			if err = currentWr.Flush(); err != nil {
				panic(fmt.Sprintf("handle record failed: %v", err))
			}
		}
	}
}

func (r *ExtentReleasor) autoApplyWorker() {
	defer r.workerWg.Done()
	var deletionTimer = time.NewTimer(time.Minute)
	for {
		select {
		case <-r.stopc:
			return
		case <-deletionTimer.C:
			var ctx context.Context = nil
			if r.interceptor != nil {
				var abort bool
				if ctx, abort = r.interceptor.Before(); abort {
					// Retry after 10s
					deletionTimer.Reset(time.Second * 10)
					continue
				}
			}
			if err := r.processArchives(1); err != nil {
				log.LogErrorf("ExtentRelaser: process archive failed: %v", err)
			}
			if r.interceptor != nil {
				r.interceptor.After(ctx)
			}
			deletionTimer.Reset(time.Minute)
		}
	}
}

func (r *ExtentReleasor) lockApply() (release func()) {
	r.applyLockc <- struct{}{}
	return func() {
		<-r.applyLockc
	}
}

func (r *ExtentReleasor) processArchives(maxArchives int) (err error) {
	var release = r.lockApply()
	defer release()
	for i := 0; i < maxArchives; i++ {
		var archived, ok = r.pickFirstArchived()
		if !ok {
			return
		}
		if !archived.valid() {
			_ = r.removeArchived(archived)
			continue
		}
		if err = r.processRecordFile(archived); err != nil {
			return
		}
		if err = r.removeArchived(archived); err != nil {
			return
		}
	}
	return
}

func (r *ExtentReleasor) processRecordFile(rf recordFileName) (err error) {
	var file *os.File
	var filepath = path.Join(r.path, rf.original())
	file, err = os.OpenFile(filepath, os.O_RDONLY, 0666)
	if os.IsNotExist(err) {
		err = nil
		return
	}
	defer func() {
		_ = file.Close()
	}()
	defer func() {
		if err != nil {
			err = fmt.Errorf("process record file %v failed: %v", filepath, err)
		}
	}()
	var bufR = bufio.NewReader(file)
	var record = allocateRecord()
	defer returnRecord(record)
	var buf = make([]byte, recordEncodedLength)
	for {
		_, err = io.ReadFull(bufR, buf[:recordEncodedLength])
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return
		}
		if err = record.decodeFrom(buf[:recordEncodedLength]); err != nil {
			return
		}
		// 忽略执行删除时的错误
		_ = r.storage.MarkDelete(record.extent, int64(record.offset), int64(record.size))
	}
	return
}

func (r *ExtentReleasor) pickFirstArchived() (rf recordFileName, ok bool) {
	r.archivesMu.RLock()
	defer r.archivesMu.RUnlock()
	if len(r.archives) == 0 {
		return
	}
	rf = r.archives[0]
	ok = true
	return
}

func (r *ExtentReleasor) removeArchived(rf recordFileName) error {
	if !rf.valid() || rf.isCurrent() {
		return nil
	}
	r.archivesMu.Lock()
	var archives = make([]recordFileName, 0)
	for _, archived := range r.archives {
		if archived.equals(rf) {
			continue
		}
		archives = append(archives, archived)
	}
	r.archives = archives
	r.archivesMu.Unlock()
	err := os.Remove(path.Join(r.path, rf.original()))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (r *ExtentReleasor) workerPanicHandler(i interface{}) {
	log.LogCritical("ER[%v] occurred panic: %v", r.path, i)
}

func (r *ExtentReleasor) Submit(ctx context.Context, ino, extent, offset, size uint64) (err error) {
	if r == nil {
		return ErrInvalidReleasorInstance
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var record = allocateRecord()
	record.ino = ino
	record.extent = extent
	record.offset = offset
	record.size = size
	select {
	case <-r.stopc:
		return ErrReleasorStopped
	case <-ctx.Done():
		return ctx.Err()
	case r.propc <- record:
	}
	return nil
}

func (r *ExtentReleasor) Apply(maxArchives int) error {
	if r == nil {
		return nil
	}
	return r.processArchives(maxArchives)
}

func NewExtentReleasor(path string, storage *ExtentStore, autoApply bool, interceptor Interceptor) (releasor *ExtentReleasor, err error) {
	if err = os.Mkdir(path, 0777); err != nil && !os.IsExist(err) {
		return
	}
	var r = &ExtentReleasor{
		path:        path,
		storage:     storage,
		propc:       make(chan *record, 128),
		stopc:       make(chan struct{}),
		interceptor: interceptor,
		autoApply:   autoApply,
		applyLockc:  make(chan struct{}, 1),
	}
	if err = r.start(); err != nil {
		return
	}
	releasor = r
	return
}
