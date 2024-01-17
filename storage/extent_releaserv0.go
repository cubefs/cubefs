package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/errors"
)

var (
	ExtentReleasorDirName     = "releasor"
	regexpRecordFileV0        = regexp.MustCompile("^DeletionRecord.(\\d){10}.((current)|(\\d){4})$")
	regexpRecordFileArchive   = regexp.MustCompile("^DeletionRecord.(\\d){10}.(\\d){4}$")
	regexpRecordFileCurrent   = regexp.MustCompile("^DeletionRecord.(\\d){10}.current$")
	recordFileTimestampLayout = "2006010215"
	recordEncodedLength       = 36 // crc=4,ino=8,extent=8,offset=8,Size=8

	ErrIllegalRecordLengthV0 = errors.New("illegal DeletionRecord length")
	ErrIllegalRecordData     = errors.New("illegal DeletionRecord data")

	recordPoolV0 = sync.Pool{
		New: func() interface{} {
			return &recordV0{}
		},
	}
	allocateRecord = func() *recordV0 {
		return recordPoolV0.Get().(*recordV0)
	}
	returnRecordV0 = func(r *recordV0) {
		recordPoolV0.Put(r)
	}
)

type recordFileName string

func (r recordFileName) valid() bool {
	return regexpRecordFileV0.MatchString(string(r))
}

func (r recordFileName) isArchived() bool {
	return regexpRecordFileArchive.MatchString(string(r))
}

func (r recordFileName) archivedNo() int {
	if r.isArchived() {
		var no, _ = strconv.Atoi(strings.Split(string(r), ".")[2])
		return no
	}
	return -1
}

func (r recordFileName) isCurrent() bool {
	return regexpRecordFileCurrent.MatchString(string(r))
}

func (r recordFileName) toArchive(no int) recordFileName {
	if r.valid() {
		parts := strings.Split(string(r), ".")[:2]
		parts = append(parts, fmt.Sprintf("%04d", no))
		return recordFileName(strings.Join(parts, "."))
	}
	return ""
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
	if is = regexpRecordFileV0.MatchString(raw); !is {
		return
	}
	_, err := time.ParseInLocation(recordFileTimestampLayout, strings.Split(raw, ".")[1], time.Local)
	if is = err == nil; !is {
		return
	}
	rfn = recordFileName(raw)
	return
}

type recordV0 struct {
	ino    uint64
	extent uint64
	offset uint64
	size   uint64
}

func (r *recordV0) decodeFrom(b []byte) (err error) {
	if len(b) < recordEncodedLength {
		return ErrIllegalRecordLengthV0
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

type ExtentReleaserV0 struct {
	path       string
	storage    *ExtentStore
	archives   []recordFileName
	archivesMu sync.RWMutex
	applyLockc chan struct{}
}

func (r *ExtentReleaserV0) start() (err error) {
	var entries []os.DirEntry
	if entries, err = os.ReadDir(r.path); err != nil {
		return
	}
	var archives = make([]recordFileName, 0)
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	for _, entry := range entries {
		rfn, is := parseRecordFileName(entry.Name())
		if !is {
			continue
		}
		if rfn.isCurrent() {
			var no = 0
			if len(archives) > 0 && archives[len(archives)-1].timestamp() == rfn.timestamp() {
				no = archives[len(archives)-1].archivedNo() + 1
			}
			archived := rfn.toArchive(no)
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
	return
}

func (r *ExtentReleaserV0) lockApply() (release func()) {
	r.applyLockc <- struct{}{}
	return func() {
		<-r.applyLockc
	}
}

func (r *ExtentReleaserV0) processArchives(maxArchives int) (err error) {
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

func (r *ExtentReleaserV0) processRecordFile(rf recordFileName) (err error) {
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
	defer returnRecordV0(record)
	var buf = make([]byte, recordEncodedLength)
	var batch = BatchMarker(1000)
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
		batch.Add(record.ino, record.extent, int64(record.offset), int64(record.size))
		if batch.Len()%1000 == 0 {
			_ = r.storage.MarkDelete(batch)
			batch = BatchMarker(1000)
		}
	}
	if batch.Len()%1000 != 0 {
		_ = r.storage.MarkDelete(batch)
	}
	return
}

func (r *ExtentReleaserV0) pickFirstArchived() (rf recordFileName, ok bool) {
	r.archivesMu.RLock()
	defer r.archivesMu.RUnlock()
	if len(r.archives) == 0 {
		return
	}
	rf = r.archives[0]
	ok = true
	return
}

func (r *ExtentReleaserV0) removeArchived(rf recordFileName) error {
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

func (r *ExtentReleaserV0) TransferDelete() (archives int, err error) {
	if r == nil {
		return
	}
	archives = len(r.archives)
	if archives == 0 {
		return 0, nil
	}
	err = r.processArchives(archives)
	if err != nil {
		return
	}
	return archives, nil
}

func NewExtentReleaserV0(path string, storage *ExtentStore) (releaser *ExtentReleaserV0, err error) {
	var r = &ExtentReleaserV0{
		path:       path,
		storage:    storage,
		applyLockc: make(chan struct{}, 1),
	}
	if err = r.start(); err != nil {
		return
	}
	releaser = r
	return
}
