package objectnode

import (
	"context"
	"fmt"
	"io"

	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

type FSFileReader struct {
	volume string
	info   *FSFileInfo
	ec     *data.ExtentClient
}

func (r *FSFileReader) FileInfo() *FSFileInfo {
	return r.info
}

func (r *FSFileReader) Read(b []byte, offset uint64, size int) (n int, hole bool, err error) {
	n, hole, err = r.ec.Read(context.Background(), r.info.Inode, b, offset, size)
	return
}

func (r *FSFileReader) WriteTo(writer io.Writer, offset, size uint64) (int64, error) {
	var err error

	var fileSize = uint64(r.info.Size)
	if offset >= fileSize {
		return 0, nil
	}
	var upper = size + offset
	if upper > fileSize {
		upper = fileSize
	}

	var n int
	var buffer, bufferSize = r.getStreamReadBuffer(size)

	var totalWriteNumBytes int64
	for {
		var rest = upper - offset
		if rest == 0 {
			break
		}
		var readSize = bufferSize
		if uint64(readSize) > rest {
			readSize = int(rest)
		}
		n, _, err = r.ec.Read(context.Background(), r.info.Inode, buffer, offset, readSize)
		if err != nil && err != io.EOF {
			log.LogErrorf("FSFileReader: data read fail: volume(%v) inode(%v) offset(%v) size(%v) err(%v)",
				r.volume, r.info.Inode, offset, size, err)
			exporter.Warning(fmt.Sprintf("read data fail: volume(%v) inode(%v) offset(%v) size(%v) err(%v)",
				r.volume, r.info.Inode, offset, readSize, err))
			return 0, err
		}
		if n > 0 {
			var num int
			if num, err = writer.Write(buffer[:n]); err != nil {
				return totalWriteNumBytes, err
			}
			offset += uint64(n)
			totalWriteNumBytes += int64(num)
		}
		if n == 0 || err == io.EOF {
			break
		}
	}
	return totalWriteNumBytes, nil
}

func (r *FSFileReader) getStreamReadBuffer(readSize uint64) ([]byte, int) {
	var bufferSize = 2 * unit.BlockSize
	if readSize < uint64(bufferSize) {
		bufferSize = int(readSize)
	}
	return make([]byte, bufferSize), bufferSize
}

func (r *FSFileReader) Close() error {
	return r.ec.CloseStream(context.Background(), r.info.Inode)
}

func NewFSFileReader(volume string, info *FSFileInfo, ec *data.ExtentClient) (reader *FSFileReader, err error) {
	if err = ec.OpenStream(info.Inode, false, false); err != nil {
		return
	}
	if currFileSize, _, _ := ec.FileSize(info.Inode); currFileSize < uint64(info.Size) {
		if err = ec.RefreshExtentsCache(context.Background(), info.Inode); err != nil {
			return
		}
	}
	reader = &FSFileReader{
		volume: volume,
		info:   info,
		ec:     ec,
	}
	return
}
