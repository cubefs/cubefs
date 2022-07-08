// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package largefile

import (
	"errors"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultFileItemBucketNum = 10
	seperateChar             = "-"
)

var (
	defaultNoopFile     = &noopFile{}
	defaultNoopStatInfo = &noopStatInfo{}
)

type LargeFile interface {
	// Write append data into large file
	Write(b []byte) (n int, err error)
	// ReadAt allow to read any offset of large file if there is no any rotate
	// return EOF error when file has been deleted
	ReadAt(buf []byte, off int64) (n int, err error)
	// FsizeOf return total file size of all large files
	// Notice: it's expensive for every call about FsizeOf
	FsizeOf() (fsize int64, err error)
	// Rotate will rotate new file without fulfill the current file
	// the FsizeOf will return new file size after rotate
	Rotate() error
	Close() error
}

type IFile interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	Stat() (os.FileInfo, error)
}

type Config struct {
	Path string `json:"path"`
	// FileChunkSizeBits specified single file size (2<<file_chunk_bits)
	FileChunkSizeBits uint   `json:"file_chunk_size_bits"`
	Backup            int    `json:"backup"`
	Suffix            string `json:"suffix"`
}

type largeFile struct {
	off       int64
	fhs       [defaultFileItemBucketNum]*fileItem
	fis       []os.FileInfo
	latestIdx int64
	// fisM is a search map for fis
	fisM map[int64]os.FileInfo
	mux  sync.RWMutex
	Config
}

type noopStatInfo struct{}

func (s *noopStatInfo) Name() string       { return "" }
func (s *noopStatInfo) Size() int64        { return 0 }
func (s *noopStatInfo) Mode() os.FileMode  { return 0o777 }
func (s *noopStatInfo) ModTime() time.Time { return time.Now() }
func (s *noopStatInfo) IsDir() bool        { return false }
func (s *noopStatInfo) Sys() interface{}   { return nil }

type noopFile struct{}

func (r *noopFile) ReadAt(b []byte, off int64) (n int, err error)  { return 0, io.EOF }
func (r *noopFile) WriteAt(b []byte, off int64) (n int, err error) { return 0, io.EOF }
func (r *noopFile) Stat() (os.FileInfo, error)                     { return defaultNoopStatInfo, nil }
func (r *noopFile) Close() error                                   { return nil }

type fileItem struct {
	f    IFile
	off  int64
	idx  int64
	size int64
}

func (fi *fileItem) Size(latestIdx int64) (int64, error) {
	if size := atomic.LoadInt64(&fi.size); size != 0 {
		return size, nil
	}
	stat, err := fi.f.Stat()
	if err != nil {
		return 0, err
	}
	// set file size cache when current file has been written full and became backup file
	if latestIdx > fi.idx {
		atomic.StoreInt64(&fi.size, stat.Size())
	}
	return stat.Size(), nil
}

func OpenLargeFile(cfg Config) (LargeFile, error) {
	if strings.Contains(cfg.Suffix, "-") {
		return nil, errors.New("log suffix can not contain '-'")
	}
	_, err := os.Stat(cfg.Path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err := os.MkdirAll(cfg.Path, 0o755); err != nil {
			return nil, err
		}
	}

	l := &largeFile{
		Config: cfg,
	}

	fis := make([]os.FileInfo, 0)
	fisM := make(map[int64]os.FileInfo)
	err = l.rangeDir(func(fi os.FileInfo) error {
		fis = append(fis, fi)
		idx, _, err := l.decodeFileName(fi.Name())
		if err != nil {
			return err
		}
		fisM[idx] = fi
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(fis, func(i, j int) bool {
		seq1, _, err := l.decodeFileName(fis[i].Name())
		if err != nil {
			return false
		}
		seq2, _, err := l.decodeFileName(fis[j].Name())
		if err != nil {
			return false
		}
		return seq1 < seq2
	})
	l.fis = fis
	l.fisM = fisM
	if len(fis) > 0 {
		l.latestIdx, _, _ = l.decodeFileName(fis[len(fis)-1].Name())
	}
	fsize, err := l.FsizeOf()
	if err != nil {
		return nil, err
	}
	l.off = fsize

	return l, nil
}

func (l *largeFile) Write(b []byte) (n int, err error) {
	len := int64(len(b))
	off := atomic.AddInt64(&l.off, len)
	n, err = l.writeAt(b, off-len)
	return
}

func (l *largeFile) writeAt(buf []byte, off int64) (n int, err error) {
	fh, _, err := l.getFileHandler(off, false)
	if err != nil {
		return 0, err
	}
	return fh.f.WriteAt(buf, off-fh.off)
}

func (l *largeFile) ReadAt(buf []byte, off int64) (n int, err error) {
	if atomic.LoadInt64(&l.off) <= off {
		return 0, io.EOF
	}

	if len(buf) > 1<<l.FileChunkSizeBits {
		return 0, errors.New("file chunk size bits too small or read buffer too large")
	}

	fh, _, err := l.getFileHandler(off, false)
	if err != nil {
		return 0, err
	}

	size, err := fh.Size(atomic.LoadInt64(&l.latestIdx))
	if err != nil {
		return 0, err
	}

	roff := off - fh.off
	left := 1<<l.FileChunkSizeBits - roff
	if size > 0 {
		left = size - roff
	}

	if int64(len(buf)) <= left {
		return fh.f.ReadAt(buf, roff)
	}
	n, err = fh.f.ReadAt(buf[:left], roff)
	if err != nil {
		return
	}
	n2, err := l.ReadAt(buf[left:], off+left)
	if err != nil {
		return
	}
	n += n2
	return
}

func (l *largeFile) FsizeOf() (fsize int64, err error) {
	if size := atomic.LoadInt64(&l.off); size > 0 {
		return size, nil
	}
	return l.fsizeOf()
}

func (l *largeFile) fsizeOf() (fsize int64, err error) {
	maxIdx := int64(0)
	maxIdxSize := int64(0)
	maxIdxOff := int64(0)
	err = l.rangeDir(func(fi os.FileInfo) error {
		idx, off, err := l.decodeFileName(fi.Name())
		if err != nil {
			return err
		}
		if idx >= maxIdx {
			maxIdx = idx
			maxIdxOff = off
			maxIdxSize = fi.Size()
		}
		return nil
	})
	fsize += maxIdxOff + maxIdxSize
	return
}

// Rotate will rotate new file without fulfill the current file
func (l *largeFile) Rotate() error {
	fsize, err := l.FsizeOf()
	if err != nil {
		return err
	}
	if fsize == 0 {
		return nil
	}
	idx := fsize >> l.FileChunkSizeBits
	newpos := (idx + 1) << l.FileChunkSizeBits
	_, _, err = l.getFileHandler(newpos, true)
	if err != nil {
		return err
	}
	fsize, err = l.fsizeOf()
	if err != nil {
		return err
	}
	atomic.StoreInt64(&l.off, fsize)
	return nil
}

func (l *largeFile) Close() error {
	l.mux.Lock()
	defer l.mux.Unlock()
	for _, fh := range l.fhs {
		if fh != nil {
			fh.f.Close()
		}
	}
	return nil
}

func (l *largeFile) getFileHandler(off int64, rotateNew bool) (fh *fileItem, isOpenNewFile bool, err error) {
	idx := off >> l.FileChunkSizeBits
	bucketIdx := idx % defaultFileItemBucketNum

	l.mux.RLock()
	fh = l.fhs[bucketIdx]
	l.mux.RUnlock()
	if fh != nil && fh.idx == idx {
		return
	}

	l.mux.Lock()
	defer l.mux.Unlock()

	fh = l.fhs[bucketIdx]
	if fh != nil && fh.idx == idx {
		return
	}

	// if try to open an deleted file, return noop file
	if len(l.fis) > 0 {
		oldestIdx, _, decodeErr := l.decodeFileName(l.fis[0].Name())
		if decodeErr != nil {
			err = decodeErr
			return
		}
		if oldestIdx > idx {
			fh = &fileItem{f: defaultNoopFile, idx: idx}
			return
		}
	}

	var (
		f            *os.File
		fi           os.FileInfo
		fileStartOff int64
	)
	if _, ok := l.fisM[idx]; !ok {
		// get previous file info and calculate next open file's start offset
		prev := l.fisM[idx-1]
		if prev == nil {
			fileStartOff = 0
		} else {
			prev, err = os.Stat(l.getAbsoluteFileName(prev.Name()))
			if err != nil {
				return
			}
			l.fisM[idx-1] = prev
			_, lastOffset, _ := l.decodeFileName(prev.Name())
			fileStartOff = lastOffset
			if rotateNew && prev.Size() < 1<<l.FileChunkSizeBits {
				fileStartOff += 1 << l.FileChunkSizeBits
			} else {
				fileStartOff += prev.Size()
			}
		}
		f, err = os.OpenFile(l.encodeFileName(idx, fileStartOff), os.O_RDWR|os.O_CREATE, 0o666)
		isOpenNewFile = true
	} else {
		f, err = os.OpenFile(l.getAbsoluteFileName(l.fisM[idx].Name()), os.O_RDWR, 0o666)
		_, fileStartOff, _ = l.decodeFileName(l.fisM[idx].Name())
	}
	if err != nil {
		return
	}
	runtime.SetFinalizer(f, func(f *os.File) {
		f.Close()
	})

	fh = &fileItem{
		f:   f,
		idx: idx,
		off: fileStartOff,
	}
	fi, err = f.Stat()
	if err != nil {
		return
	}

	l.fhs[bucketIdx] = fh
	// new file add into fis
	if isOpenNewFile {
		l.fis = append(l.fis, fi)
		l.fisM[idx] = fi
		atomic.StoreInt64(&l.latestIdx, idx)
	}

	if err = l.checkRotate(); err != nil {
		return
	}
	return
}

// check rotate will check if need to remove oldest backup file
func (l *largeFile) checkRotate() error {
	if l.Backup <= 0 {
		return nil
	}
	num := len(l.fis) - l.Backup
	for i := 0; i < num; i++ {
		oldestFileName := l.fis[0].Name()
		oldestIdx, _, err := l.decodeFileName(oldestFileName)
		if err != nil {
			return err
		}
		bucketIdx := oldestIdx % defaultFileItemBucketNum
		if l.fhs[bucketIdx] != nil && l.fhs[bucketIdx].idx == oldestIdx {
			if err = l.fhs[bucketIdx].f.Close(); err != nil {
				return err
			}
			l.fhs[bucketIdx] = nil
		}
		if err = os.Remove(l.getAbsoluteFileName(oldestFileName)); err != nil {
			return err
		}
		l.fis = l.fis[1:]
		delete(l.fisM, oldestIdx)
	}
	return nil
}

func (l *largeFile) rangeDir(f func(fi os.FileInfo) error) error {
	dir, err := os.Open(l.Path)
	if err != nil {
		return err
	}
	defer dir.Close()
	fis, err := dir.Readdir(-1)
	if err != nil {
		return err
	}
	for _, fi := range fis {
		if !strings.HasSuffix(fi.Name(), l.Suffix) {
			continue
		}
		if !strings.Contains(fi.Name(), seperateChar) {
			continue
		}
		if err := f(fi); err != nil {
			return err
		}
	}
	return nil
}

func (l *largeFile) encodeFileName(idx int64, startOff int64) string {
	return l.Path + "/" + strconv.FormatInt(idx, 36) + seperateChar + strconv.FormatInt(startOff, 36) + l.Suffix
}

func (l *largeFile) decodeFileName(name string) (idx int64, startOff int64, err error) {
	arrs := strings.Split(strings.Replace(name, l.Suffix, "", 1), seperateChar)
	if len(arrs) != 2 {
		err = errors.New("invalid file name")
		return
	}
	if idx, err = strconv.ParseInt(arrs[0], 36, 64); err != nil {
		return
	}
	if startOff, err = strconv.ParseInt(arrs[1], 36, 64); err != nil {
		return
	}
	return
}

func (l *largeFile) getAbsoluteFileName(name string) string {
	return l.Path + "/" + name
}
