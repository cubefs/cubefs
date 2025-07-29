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
	"path/filepath"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/remotecache/flashnode/cachengine"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/bytespool"
)

type LocalFS struct {
	cachePath string
	fsType    string
	testPath  string
	verify    bool
}

const HeaderSize = cachengine.HeaderSize

func NewLocalFS(basePath, testPath, fsType string, verify bool) *LocalFS {
	os.MkdirAll(basePath, 0o755)
	return &LocalFS{cachePath: basePath, fsType: fsType, testPath: testPath, verify: verify}
}

func (fs *LocalFS) Name() string { return fs.fsType }

func (fs *LocalFS) Put(ctx context.Context, reqId, key string, r io.Reader, length int64) (err error) {
	pDir := cachengine.MapKeyToDirectory(key)
	filePath := filepath.Join(fs.cachePath, path.Join(pDir, key))
	allocSize, _ := cachengine.CalcAllocSizeV2(int(length))

	if _, err = os.Stat(path.Dir(filePath)); err != nil {
		if !os.IsNotExist(err.(*os.PathError)) {
			return fmt.Errorf("initFilePath stat directory[%v] failed: %s", path.Dir(filePath), err.Error())
		}
		if err = os.Mkdir(path.Dir(filePath), 0o755); err != nil {
			if !os.IsExist(err) {
				return
			}
		}
	}

	// clear the context if File is exist
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create File %v failed %v\n", filePath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("create File %v failed %v\n", file, closeErr)
		}
	}()
	buf := bytespool.Alloc(proto.CACHE_BLOCK_PACKET_SIZE)
	defer bytespool.Free(buf)
	crcBuf := bytespool.Alloc(proto.CACHE_BLOCK_CRC_SIZE)
	defer bytespool.Free(crcBuf)

	var (
		totalWritten int64
		n            int
	)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		readSize := proto.CACHE_BLOCK_PACKET_SIZE
		if totalWritten+int64(readSize) > length {
			readSize = int(length - totalWritten)
		}
		n, err = r.Read(buf[:readSize])
		if n != readSize {
			return fmt.Errorf("expected to read %d bytes, but only read %d", readSize, n)
		}
		if n > 0 {
			// write data
			if _, err = file.WriteAt(buf[:proto.CACHE_BLOCK_PACKET_SIZE], totalWritten+HeaderSize); err != nil {
				return fmt.Errorf("write to File %v failed %v", filePath, err)
			}
			crcOffset := totalWritten/proto.CACHE_BLOCK_PACKET_SIZE*proto.CACHE_BLOCK_CRC_SIZE + int64(allocSize) + HeaderSize
			binary.BigEndian.PutUint32(crcBuf, crc32.ChecksumIEEE(buf[:proto.CACHE_BLOCK_PACKET_SIZE]))
			// write crc
			if _, err = file.WriteAt(crcBuf[:proto.CACHE_BLOCK_CRC_SIZE], crcOffset); err != nil {
				return fmt.Errorf("write to filecrc %v failed %v", filePath, err)
			}
			totalWritten += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read from File %v failed %v", filePath, err)
		}
		if totalWritten >= length {
			break
		}
	}
	if totalWritten != length {
		return fmt.Errorf("unexpected data length: expected %d bytes, got %d", length, totalWritten)
	}
	// write header
	if _, err = file.Seek(0, 0); err != nil {
		return
	}
	var reserved uint64 = 0
	if err = binary.Write(file, binary.BigEndian, reserved); err != nil {
		return
	}
	if err = binary.Write(file, binary.BigEndian, reserved); err != nil {
		return
	}
	if err = binary.Write(file, binary.BigEndian, int64(allocSize)); err != nil {
		return
	}
	if err = binary.Write(file, binary.BigEndian, totalWritten); err != nil {
		return
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync  File %v failed %v", filePath, err)
	}
	if fs.verify {
		return fs.CheckFile(file)
	}
	return nil
}

func (fs *LocalFS) CheckFile(file *os.File) error {
	var (
		err                  error
		stat                 os.FileInfo
		reserved1, reserved2 uint64
		allocSize, usedSize  int64
	)
	if _, err = file.Seek(0, 0); err != nil {
		return fmt.Errorf("seek  failed: %v", err)
	}
	if stat, err = file.Stat(); err != nil {
		return err
	}
	if err = binary.Read(file, binary.BigEndian, &reserved1); err != nil {
		return fmt.Errorf("read reserved1  failed: %v", err)
	}
	if err = binary.Read(file, binary.BigEndian, &reserved2); err != nil {
		return fmt.Errorf("read reserved2  failed: %v", err)
	}
	if err = binary.Read(file, binary.BigEndian, &allocSize); err != nil {
		return fmt.Errorf("read allocSize  failed: %v", err)
	}
	if err = binary.Read(file, binary.BigEndian, &usedSize); err != nil {
		return fmt.Errorf("read usedSize  failed: %v", err)
	}
	if usedSize == 0 {
		err = fmt.Errorf("usedSize is zero")
		return err
	}
	crcSize := (usedSize + proto.CACHE_BLOCK_PACKET_SIZE - 1) / proto.CACHE_BLOCK_PACKET_SIZE * proto.CACHE_BLOCK_CRC_SIZE
	if allocSize+HeaderSize+crcSize != stat.Size() {
		err = fmt.Errorf("allocSize + headerSize + crsSize [%v] != File real size[%v]", allocSize+HeaderSize+crcSize, stat.Size())
		return err
	}
	// verify block
	return verifyFileBlocks(file, int(crcSize)/proto.CACHE_BLOCK_CRC_SIZE)
}

func (fs *LocalFS) Get(ctx context.Context, reqId, key string, from, to int64) (r io.ReadCloser, length int64, shouldCache bool, err error) {
	pDir := cachengine.MapKeyToDirectory(key)
	filePath := filepath.Join(fs.cachePath, path.Join(pDir, key))
	// TODO: verify from to
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, 0, false, err
	}
	var (
		reserved1, reserved2 uint64
		allocSize, usedSize  int64
	)
	if err = binary.Read(file, binary.BigEndian, &reserved1); err != nil {
		return nil, 0, false, fmt.Errorf("read reserved1  failed: %v", err)
	}
	if err = binary.Read(file, binary.BigEndian, &reserved2); err != nil {
		return nil, 0, false, fmt.Errorf("read reserved2  failed: %v", err)
	}
	if err = binary.Read(file, binary.BigEndian, &allocSize); err != nil {
		return nil, 0, false, fmt.Errorf("read allocSize  failed: %v", err)
	}
	if err = binary.Read(file, binary.BigEndian, &usedSize); err != nil {
		return nil, 0, false, fmt.Errorf("read usedSize  failed: %v", err)
	}
	if from/proto.CACHE_OBJECT_BLOCK_SIZE != to/proto.CACHE_OBJECT_BLOCK_SIZE {
		return nil, 0, false, fmt.Errorf("invalid range offset(%v) size(%v)", from, to-from)
	}

	if usedSize < to {
		return nil, 0,
			false, fmt.Errorf("block is not ready, usedSize(%v) offset(%v) size(%v)", usedSize, from, to-from)
	}
	return NewLocalFSReader(file, from, to-from, allocSize, usedSize, filePath), to - from, false, nil
}

func verifyFileBlocks(file *os.File, blockCount int) error {
	dataBuf := bytespool.Alloc(proto.CACHE_BLOCK_PACKET_SIZE)
	defer bytespool.Free(dataBuf)
	crcBuf := bytespool.Alloc(proto.CACHE_BLOCK_CRC_SIZE)
	defer bytespool.Free(crcBuf)

	for i := 0; i < blockCount; i++ {
		dataOffset := HeaderSize + int64(i)*proto.CACHE_BLOCK_PACKET_SIZE
		_, err := file.ReadAt(dataBuf, dataOffset)
		if err != nil {
			return fmt.Errorf("read data block %d failed: %v", i, err)
		}

		crcOffset := HeaderSize + int64(blockCount)*proto.CACHE_BLOCK_PACKET_SIZE + int64(i)*proto.CACHE_BLOCK_CRC_SIZE
		_, err = file.ReadAt(crcBuf, crcOffset)
		if err != nil {
			return fmt.Errorf("read crc %d failed: %v", i, err)
		}

		expectedCrc := binary.BigEndian.Uint32(crcBuf)
		actualCrc := crc32.ChecksumIEEE(dataBuf)

		if expectedCrc != actualCrc {
			return fmt.Errorf("crc mismatch at block %d: expected %x, got %x", i, expectedCrc, actualCrc)
		}
	}
	return nil
}

type LocalFSReader struct {
	reader    *bufio.Reader
	file      *os.File
	offset    int64
	size      int64
	allocSize int64
	usedSize  int64
	fullPath  string
	curOffset int64
}

func (l *LocalFSReader) Read(p []byte) (n int, err error) {
	buf := bytespool.Alloc(proto.CACHE_BLOCK_PACKET_SIZE)
	defer bytespool.Free(buf)
	crcBuf := bytespool.Alloc(proto.CACHE_BLOCK_CRC_SIZE)
	defer bytespool.Free(crcBuf)
	end := l.offset + l.size
	alignedOffset := l.curOffset / proto.CACHE_BLOCK_PACKET_SIZE * proto.CACHE_BLOCK_PACKET_SIZE
	if alignedOffset >= end {
		return 0, io.EOF
	}
	// read 32k from file
	if n, err = l.file.ReadAt(buf, alignedOffset+HeaderSize); err != nil {
		return 0, fmt.Errorf("read %v offset %v failed: %v", l.fullPath, alignedOffset+HeaderSize, err)
	}
	// read crc from file
	sliceIndex := l.curOffset / proto.CACHE_BLOCK_PACKET_SIZE
	crcOffset := l.allocSize + HeaderSize + sliceIndex*proto.CACHE_BLOCK_CRC_SIZE
	if _, err = l.file.ReadAt(crcBuf, crcOffset); err != nil {
		return 0, fmt.Errorf("read %v crc offset  %v failed: %v", l.fullPath, crcOffset, err)
	}
	// check crc for 32k
	crc := binary.BigEndian.Uint32(crcBuf)
	actualCrc := crc32.ChecksumIEEE(buf)
	if crc != actualCrc {
		return 0, fmt.Errorf("inconsistent CRC, file %v  expected %v crc actual %v", l.fullPath, crc, actualCrc)
	}
	// copy the real buf
	expectLen := util.Min(int(proto.CACHE_BLOCK_PACKET_SIZE-l.curOffset+alignedOffset), int(end-l.curOffset))
	copy(p[:expectLen], buf[l.curOffset-alignedOffset:l.curOffset-alignedOffset+int64(expectLen)])
	// move index
	l.curOffset = alignedOffset + proto.CACHE_BLOCK_PACKET_SIZE
	return expectLen, nil
}

func (l *LocalFSReader) Close() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func NewLocalFSReader(file *os.File, offset, size, allocSize, usedSize int64, fullPath string) *LocalFSReader {
	r := &LocalFSReader{
		reader:    bufio.NewReaderSize(file, proto.CACHE_BLOCK_PACKET_SIZE),
		file:      file,
		offset:    offset,
		size:      size,
		allocSize: allocSize,
		usedSize:  usedSize,
		fullPath:  fullPath,
		curOffset: offset,
	}
	return r
}

func (fs *LocalFS) LoadTestFiles() error {
	// TODO: maybe to much handle
	return nil
}

func (fs *LocalFS) Stop() {
}
