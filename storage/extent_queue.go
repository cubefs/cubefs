package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

var (
	regexpRecordFile = regexp.MustCompile("^REC.(\\d){20}$")

	ErrInvalidRecordFilename = errors.New("invalid record filename")
	ErrIllegalRecordLength   = errors.New("illegal record binary length")
	ErrIllegalRecordData     = errors.New("illegal record data")
	ErrNilInstance           = errors.New("nil pointer instance")

	recordPool = sync.Pool{
		New: func() interface{} {
			return &record{}
		},
	}

	getRecord = func() *record {
		return recordPool.Get().(*record)
	}

	returnRecord = func(r *record) {
		recordPool.Put(r)
	}

	recordCodecBytesPool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, queueRecordCodecLength)
		},
	}

	getRecordCodecBytes = func() []byte {
		return recordCodecBytesPool.Get().([]byte)
	}

	returnRecordCodecBytes = func(b []byte) {
		if len(b) == queueRecordCodecLength || cap(b) == queueRecordCodecLength {
			recordCodecBytesPool.Put(b[:queueRecordCodecLength])
		}
	}
)

const (
	queueRecordFileReaderOpenFlag = os.O_RDONLY
	queueRecordFileWriterOpenFlag = os.O_CREATE | os.O_RDWR | os.O_APPEND
	queueRecordFileSeekWhence     = 0x0
	queueRecordCodecLength        = 44 // crc=4,ino=8,extent=8,offset=8,size=8,timestamp=8
	queueRecordFilePerm           = 0666
	queueRecordFilenamePrefix     = "REC"
	queueRecordWriterMaxIdleSec   = 5 * 60 // If a writer do not active for more than 5 minutes
	queueDataDirPerm              = 0777
	queueMetaFilename             = "META"
	queueMetaFilePerm             = 0666
	queueDefaultFilesize          = 4 * 1024 * 1024 // 4MB
)

type RetainFiles int

func (retain RetainFiles) String() string {
	if retain.Unlimited() {
		return "Unlimited"
	}
	return strconv.Itoa(int(retain))
}

func (retain RetainFiles) Unlimited() bool {
	return retain < 0
}

type recordFilename string

func (r recordFilename) valid() bool {
	return regexpRecordFile.MatchString(string(r))
}

func (r recordFilename) seq() (n uint64) {
	if r.valid() {
		n, _ = strconv.ParseUint(strings.Split(string(r), ".")[1], 10, 64)
	}
	return
}

func (r recordFilename) next() recordFilename {
	return newRecordFilename(r.seq() + 1)
}

func (r recordFilename) original() string {
	return string(r)
}

func (r recordFilename) equals(o recordFilename) bool {
	return string(r) == string(o)
}

func parseRecordFilename(raw string) (rfn recordFilename, is bool) {
	if is = regexpRecordFile.MatchString(raw); !is {
		return
	}
	rfn = recordFilename(raw)
	return
}

func newRecordFilename(number uint64) recordFilename {
	parts := []string{
		queueRecordFilenamePrefix,
		fmt.Sprintf("%020d", number),
	}
	return recordFilename(strings.Join(parts, "."))
}

type record struct {
	ino       uint64
	extent    uint64
	offset    int64
	size      int64
	timestamp int64
}

func (r *record) encodeTo(b []byte) (err error) {
	if len(b) < queueRecordCodecLength {
		return ErrIllegalRecordLength
	}
	binary.BigEndian.PutUint64(b[4:12], r.ino)
	binary.BigEndian.PutUint64(b[12:20], r.extent)
	binary.BigEndian.PutUint64(b[20:28], uint64(r.offset))
	binary.BigEndian.PutUint64(b[28:36], uint64(r.size))
	binary.BigEndian.PutUint64(b[36:44], uint64(r.timestamp))
	binary.BigEndian.PutUint32(b[0:4], crc32.ChecksumIEEE(b[4:44]))
	return
}

func (r *record) decodeFrom(b []byte) (err error) {
	if len(b) < queueRecordCodecLength {
		return ErrIllegalRecordLength
	}
	if binary.BigEndian.Uint32(b[0:4]) != crc32.ChecksumIEEE(b[4:44]) {
		return ErrIllegalRecordData
	}
	r.ino = binary.BigEndian.Uint64(b[4:12])
	r.extent = binary.BigEndian.Uint64(b[12:20])
	r.offset = int64(binary.BigEndian.Uint64(b[20:28]))
	r.size = int64(binary.BigEndian.Uint64(b[28:36]))
	r.timestamp = int64(binary.BigEndian.Uint64(b[36:44]))
	return
}

type recordFile struct {
	rfn   recordFilename
	_seq  uint64
	_size int64
}

func (rf *recordFile) seq() uint64 {
	return rf._seq
}

func (rf *recordFile) size() int64 {
	return rf._size
}

func (rf *recordFile) incSize(inc int64) {
	rf._size += inc
}

func (rf *recordFile) setSize(size int64) {
	rf._size = size
}

func (rf *recordFile) valid() bool {
	return rf.rfn.valid() && rf.rfn.seq() == rf._seq
}

func newRecordFile(rfn recordFilename, size int64) *recordFile {
	return &recordFile{
		rfn:   rfn,
		_seq:  rfn.seq(),
		_size: size,
	}
}

type recordFiles []*recordFile

func (rfs *recordFiles) first() (rf *recordFile) {
	rf = (*rfs)[0]
	return
}

func (rfs *recordFiles) last() (rf *recordFile) {
	rf = (*rfs)[len(*rfs)-1]
	return
}

func (rfs *recordFiles) new() (rf *recordFile) {
	var rfn recordFilename
	if len(*rfs) == 0 {
		rfn = newRecordFilename(0)
	} else {
		rfn = (*rfs)[len(*rfs)-1].rfn.next()
	}
	rf = &recordFile{
		rfn:   rfn,
		_seq:  rfn.seq(),
		_size: 0,
	}
	*rfs = append(*rfs, rf)
	return
}

func (rfs *recordFiles) append(rf *recordFile) {
	*rfs = append(*rfs, rf)
}

func (rfs *recordFiles) truncateFront(seq uint64) (truncated recordFiles) {
	var to = -1
	for i := 0; i < len(*rfs); i++ {
		if (*rfs)[i]._seq >= seq {
			break
		}
		to = i
	}
	if to == -1 {
		return
	}
	truncated = make([]*recordFile, to+1)
	copy(truncated, (*rfs)[:to+1])
	if to+1 == len(*rfs) {
		*rfs = (*rfs)[:0]
		return
	}
	*rfs = (*rfs)[to+1:]
	return
}

func (rfs *recordFiles) walk(visitor func(*recordFile) bool) {
	for _, r := range *rfs {
		if !visitor(r) {
			break
		}
	}
}

func (rfs *recordFiles) clone() recordFiles {
	var dst = make([]*recordFile, len(*rfs))
	copy(dst, *rfs)
	return dst
}

func (rfs *recordFiles) len() int {
	return len(*rfs)
}

func (rfs *recordFiles) sort() {
	sort.SliceStable(*rfs, func(i, j int) bool {
		return (*rfs)[i]._seq < (*rfs)[j]._seq
	})
}

func (rfs *recordFiles) remainFrom(index recordIndex) (count int) {
	if rfs.len() == 0 || rfs.last().seq() == index.seq && rfs.last().size() == int64(index.off) {
		return
	}

	rfs.walk(func(rf *recordFile) bool {
		switch {
		case rf.seq() == index.seq:
			count += int((rf.size() - int64(index.off)) / int64(queueRecordCodecLength))
		case rf.seq() > index.seq:
			count += int(rf.size() / int64(queueRecordCodecLength))
		default:
		}
		return true
	})
	return
}

type recordFileWriter struct {
	rf   *recordFile
	fp   *os.File
	bufW *bufio.Writer
	lat  int64 // Last active time (unix seconds)
}

func (r *recordFileWriter) WriteRecord(rec *record) (err error) {
	var b = getRecordCodecBytes()
	defer returnRecordCodecBytes(b)
	if err = rec.encodeTo(b); err != nil {
		return
	}
	var n int
	n, err = r.bufW.Write(b)
	r.rf.incSize(int64(n))
	return
}

func (r *recordFileWriter) Flush() error {
	r.lat = time.Now().Unix()
	return r.bufW.Flush()

}

func (r *recordFileWriter) LastActive() (unixSec int64) {
	return r.lat
}

func (r *recordFileWriter) FileSize() int64 {
	return r.rf.size()
}

func (r *recordFileWriter) Close() (err error) {
	if err = r.bufW.Flush(); err != nil {
		return
	}
	err = r.fp.Close()
	return
}

type recordFileReader struct {
	rf     *recordFile
	fp     *os.File
	offset int64
	bufR   *bufio.Reader
}

func (r *recordFileReader) Reset(offset int64) (err error) {
	if r.offset == offset {
		return
	}
	if _, err = r.fp.Seek(offset, queueRecordFileSeekWhence); err != nil {
		return
	}
	r.bufR.Reset(r.fp)
	return
}

func (r *recordFileReader) Offset() int64 {
	return r.offset
}

func (r *recordFileReader) ReadRecord(rec *record) (err error) {
	var b = getRecordCodecBytes()
	defer returnRecordCodecBytes(b)
	var n int
	n, err = io.ReadFull(r.bufR, b)
	r.offset += int64(n)
	if err != nil {
		return
	}
	err = rec.decodeFrom(b)
	return
}

func (r *recordFileReader) Close() error {
	return r.fp.Close()
}

type recordIndex struct {
	seq uint64
	off uint64
}

func (i recordIndex) String() string {
	return fmt.Sprintf("%v-%v", i.seq, i.off)
}

func (i recordIndex) next() recordIndex {
	return recordIndex{
		seq: i.seq,
		off: i.off + queueRecordCodecLength,
	}
}

type BatchProducer struct {
	recs    []*record
	produce func(...*record) error
}

func (p *BatchProducer) Size() int {
	return len(p.recs)
}

func (p *BatchProducer) Add(ino, extent uint64, offset, size, timestamp int64) {
	var rec = getRecord()
	rec.ino = ino
	rec.extent = extent
	rec.offset = offset
	rec.size = size
	rec.timestamp = timestamp
	p.recs = append(p.recs, rec)
}

func (p *BatchProducer) Submit() (err error) {
	err = p.produce(p.recs...)
	for _, rec := range p.recs {
		returnRecord(rec)
	}
	p.recs = nil
	return
}

type queueMetafile struct {
	path     string
	consumed recordIndex
	mu       sync.RWMutex
}

func (mf *queueMetafile) advanceConsumed(index recordIndex) (advanced bool, err error) {
	mf.mu.Lock()
	defer mf.mu.Unlock()
	if index.seq > mf.consumed.seq || (index.seq == mf.consumed.seq && index.off > mf.consumed.off) {
		mf.consumed = index
		var buf = make([]byte, 20)
		binary.BigEndian.PutUint64(buf[0:8], mf.consumed.seq)
		binary.BigEndian.PutUint64(buf[8:16], mf.consumed.off)
		binary.BigEndian.PutUint32(buf[16:20], crc32.ChecksumIEEE(buf[0:16]))
		err = ioutil.WriteFile(mf.path, buf, queueMetaFilePerm)
		advanced = true
	}
	return
}

func (mf *queueMetafile) getConsumed() recordIndex {
	mf.mu.RLock()
	defer mf.mu.RUnlock()
	return mf.consumed
}

func openQueueMetafile(name string) (mf *queueMetafile, err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(name); err != nil && !os.IsNotExist(err) {
		return
	}
	err = nil
	mf = &queueMetafile{
		path: name,
	}
	if len(buf) >= 20 && crc32.ChecksumIEEE(buf[0:16]) == binary.BigEndian.Uint32(buf[16:20]) {
		mf.consumed.seq = binary.BigEndian.Uint64(buf[0:8])
		mf.consumed.off = binary.BigEndian.Uint64(buf[8:16])
	}
	return
}

type WalkRange int8

const (
	WalkAll       WalkRange = 0
	WalkUnconsume WalkRange = 1
)

type QueueVisitor func(ino, extent uint64, offset, size, timestamp int64) (goon bool, err error)

func (v QueueVisitor) visit(ino, extent uint64, offset, size, timestamp int64) (goon bool, err error) {
	if v == nil {
		return false, nil
	}
	return v(ino, extent, offset, size, timestamp)
}

type recordVisitor func(idx recordIndex, ino, extent uint64, offset, size, timestamp int64) (goon bool, err error)

func (v recordVisitor) visit(idx recordIndex, ino, extent uint64, offset, size, timestamp int64) (goon bool, err error) {
	if v == nil {
		return false, nil
	}
	return v(idx, ino, extent, offset, size, timestamp)
}

type ExtentQueue struct {
	path        string
	rfs         recordFiles
	rfsMu       sync.RWMutex
	writer      *recordFileWriter
	writeMu     sync.Mutex
	maxFilesize int64
	retainFiles RetainFiles
	mf          *queueMetafile
	closeOnce   sync.Once
}

func (q *ExtentQueue) statRecordFile(rfn recordFilename) (rf *recordFile, err error) {
	var info os.FileInfo
	if info, err = os.Stat(path.Join(q.path, rfn.original())); err != nil {
		return
	}
	rf = newRecordFile(rfn, info.Size())
	return
}

func (q *ExtentQueue) openRecordFileReader(rf *recordFile, offset int64) (reader *recordFileReader, err error) {
	if !rf.valid() {
		err = ErrInvalidRecordFilename
		return
	}
	var filepath = path.Join(q.path, rf.rfn.original())
	var fp *os.File
	if fp, err = os.OpenFile(filepath, queueRecordFileReaderOpenFlag, queueRecordFilePerm); err != nil {
		return
	}
	if offset != 0 {
		if _, err = fp.Seek(offset, queueRecordFileSeekWhence); err != nil {
			return
		}
	}
	reader = &recordFileReader{
		rf:     rf,
		fp:     fp,
		offset: offset,
		bufR:   bufio.NewReader(fp),
	}
	return
}

func (q *ExtentQueue) openRecordFileWriter(rf *recordFile) (writer *recordFileWriter, err error) {
	if !rf.valid() {
		err = ErrInvalidRecordFilename
		return
	}
	var filepath = path.Join(q.path, rf.rfn.original())
	var fp *os.File
	if fp, err = os.OpenFile(filepath, queueRecordFileWriterOpenFlag, queueRecordFilePerm); err != nil {
		return
	}
	var info os.FileInfo
	if info, err = fp.Stat(); err != nil {
		return
	}
	var filesize = info.Size()
	if filesize%queueRecordCodecLength != 0 {
		filesize = (filesize / queueRecordCodecLength) * queueRecordCodecLength
		if err = fp.Truncate(filesize); err != nil {
			return
		}
	}
	rf.setSize(filesize) // update file size
	writer = &recordFileWriter{
		rf:   rf,
		fp:   fp,
		bufW: bufio.NewWriter(fp),
	}
	return
}

func (q *ExtentQueue) Close() {
	if q == nil {
		return
	}
	var doClose = func() {
		q.writeMu.Lock()
		if q.writer != nil {
			_ = q.writer.Close()
		}
		q.writer = nil
		q.writeMu.Unlock()
	}
	q.closeOnce.Do(doClose)
}

func (q *ExtentQueue) open() (err error) {

	var mf *queueMetafile
	if mf, err = openQueueMetafile(path.Join(q.path, queueMetaFilename)); err != nil {
		return
	}

	var dirFp *os.File
	if dirFp, err = os.Open(q.path); err != nil {
		return
	}
	var names []string
	if names, err = dirFp.Readdirnames(-1); err != nil {
		return
	}
	_ = dirFp.Close()
	var rfs recordFiles
	sort.Strings(names)
	for _, name := range names {
		var rfn, is = parseRecordFilename(name)
		if !is {
			continue
		}
		var rf *recordFile
		rf, err = q.statRecordFile(rfn)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return
		}
		rfs.append(rf)
	}
	rfs.sort()

	q.mf = mf
	q.rfs = rfs
	q.writer = nil

	return
}

func (q *ExtentQueue) closeInactiveWriter() (err error) {
	q.writeMu.Lock()
	defer q.writeMu.Unlock()
	if q.writer != nil && time.Now().Unix()-q.writer.LastActive() > queueRecordWriterMaxIdleSec {
		err = q.writer.Close()
		q.writer = nil
	}
	return
}

func (q *ExtentQueue) __produce(recs ...*record) (err error) {
	if q == nil {
		return ErrNilInstance
	}
	q.writeMu.Lock()
	defer q.writeMu.Unlock()
	if q.writer == nil {
		q.rfsMu.Lock()
		if q.rfs.len() == 0 || q.rfs.last().size() >= q.maxFilesize {
			q.rfs.new()
		}
		var last = q.rfs.last()
		q.rfsMu.Unlock()
		var writer *recordFileWriter
		if writer, err = q.openRecordFileWriter(last); err != nil {
			return
		}
		q.writer = writer
	}
	for _, rec := range recs {
		if err = q.writer.WriteRecord(rec); err != nil {
			return
		}
		// Check rotate
		if q.writer.FileSize() >= q.maxFilesize {
			if err = q.writer.Flush(); err != nil {
				return
			}
			if err = q.writer.Close(); err != nil {
				return
			}
			q.rfsMu.Lock()
			var last = q.rfs.new()
			if q.writer, err = q.openRecordFileWriter(last); err != nil {
				q.rfsMu.Unlock()
				return
			}
			q.rfsMu.Unlock()
		}
	}
	err = q.writer.Flush()
	return
}

func (q *ExtentQueue) Produce(ino, extent uint64, offset, size, timestamp int64) (err error) {
	if q == nil {
		return ErrNilInstance
	}
	var rec = getRecord()
	defer returnRecord(rec)
	rec.ino = ino
	rec.extent = extent
	rec.offset = offset
	rec.size = size
	rec.timestamp = timestamp
	err = q.__produce(rec)
	return
}

func (q *ExtentQueue) BatchProduce(batchsize int) (producer *BatchProducer, err error) {
	if q == nil {
		err = ErrNilInstance
		return
	}
	producer = &BatchProducer{
		recs:    make([]*record, 0, batchsize),
		produce: q.__produce,
	}
	return
}

// Remain returns number of records that have not been consumed yet.
func (q *ExtentQueue) Remain() (count int) {
	q.rfsMu.RLock()
	count = q.rfs.remainFrom(q.mf.getConsumed())
	q.rfsMu.RUnlock()
	return
}

// __walk (inner private function) used to traverse the records in the access queue from the given index to the end.
func (q *ExtentQueue) __walk(si recordIndex, visitor recordVisitor) (err error) {
	if q == nil {
		err = ErrNilInstance
		return
	}
	if visitor == nil {
		return
	}
	q.rfsMu.RLock()
	if q.rfs.remainFrom(si) == 0 {
		q.rfsMu.RUnlock()
		return
	}
	var rfs = q.rfs.clone()
	q.rfsMu.RUnlock()

	var reader *recordFileReader
	var rec = getRecord()
	defer returnRecord(rec)
	rfs.walk(func(rf *recordFile) bool {
		var seq = rf.seq()
		if seq < si.seq {
			return true
		}
		var off int64
		if seq == si.seq {
			if int64(si.off) >= rf.size() {
				return true
			}
			off = int64(si.off)
		}
		reader, err = q.openRecordFileReader(rf, off)
		if os.IsNotExist(err) || err == io.EOF {
			_ = reader.Close()
			err = nil
			return true
		}
		if err != nil {
			_ = reader.Close()
			return false
		}
		defer func() {
			_ = reader.Close()
		}()
		var idx = recordIndex{
			seq: seq,
			off: uint64(reader.Offset()),
		}
		for {
			err = reader.ReadRecord(rec)
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return false
			}
			var goon bool
			goon, err = visitor.visit(idx, rec.ino, rec.extent, rec.offset, rec.size, rec.timestamp)
			if err != nil || !goon {
				return false
			}
			idx = idx.next()
		}
		return true
	})
	return
}

// Walk used to traverse access records in given range.
// Parameter:
//   * r (WalkRange):
//      WalkAll: all records stored in queue.
//      WalkUnconsume: records that have not consumed yet.
func (q *ExtentQueue) Walk(r WalkRange, visitor QueueVisitor) (err error) {
	var idx recordIndex
	switch r {
	case WalkAll:
		idx = recordIndex{
			seq: 0,
			off: 0,
		}
	case WalkUnconsume:
		idx = q.mf.getConsumed()
	default:
		err = errors.New("unknown walk range")
		return
	}
	err = q.__walk(idx, func(idx recordIndex, ino, extent uint64, offset, size, timestamp int64) (goon bool, err error) {
		return visitor.visit(ino, extent, offset, size, timestamp)
	})
	return
}

// Consume used to access records that have not been consumed yet and advance consume index after visited without any error.
func (q *ExtentQueue) Consume(visitor QueueVisitor) (err error) {
	defer func() {
		_ = q.closeInactiveWriter()
	}()
	var si = q.mf.getConsumed()
	var count int
	err = q.__walk(si, func(idx recordIndex, ino, extent uint64, offset, size, timestamp int64) (goon bool, err error) {
		goon, err = visitor.visit(ino, extent, offset, size, timestamp)
		if err == nil {
			si = idx.next()
			count++
		}
		return
	})
	if count == 0 {
		return
	}

	var advanced, advanceErr = q.mf.advanceConsumed(si)
	if advanceErr != nil {
		log.LogErrorf("extent queue advance consumed index failed: path=%v, error=%v", q.path, si)
		return
	}
	if !advanced {
		return
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("extent queue advanced consumed index: path=%v, index=%v", q.path, si)
	}
	_ = q.cleanup()
	return
}

func (q *ExtentQueue) cleanup() (err error) {
	if q.retainFiles.Unlimited() {
		return
	}
	var consumed = q.mf.getConsumed()
	if consumed.seq <= uint64(q.retainFiles) {
		return
	}
	q.rfsMu.Lock()
	var truncated = q.rfs.truncateFront(consumed.seq - uint64(q.retainFiles))
	q.rfsMu.Unlock()
	if truncated.len() > 0 {
		truncated.walk(func(rf *recordFile) bool {
			var filepath = path.Join(q.path, rf.rfn.original())
			err = os.Remove(filepath)
			if os.IsNotExist(err) {
				err = nil
			}
			if err != nil {
				log.LogErrorf("remove consumed record file failed: path=%v, file=%v, error=%v", q.path, filepath, err)
				return false
			}
			return true
		})
	}
	return
}

func OpenExtentQueue(path string, filesize int64, retain RetainFiles) (queue *ExtentQueue, err error) {
	if err = os.Mkdir(path, queueDataDirPerm); err != nil && !os.IsExist(err) {
		return
	}
	if filesize < 0 {
		filesize = queueDefaultFilesize
	}
	var r = &ExtentQueue{
		path:        path,
		maxFilesize: filesize,
		retainFiles: retain,
	}
	if err = r.open(); err != nil {
		return
	}
	queue = r
	return
}
