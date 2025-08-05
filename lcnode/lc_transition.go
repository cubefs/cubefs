// Copyright 2023 The CubeFS Authors.
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

package lcnode

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

type ExtentApi interface {
	OpenStream(inode uint64, openForWrite, isCache bool, fullPath string) error
	CloseStream(inode uint64) error
	Read(inode uint64, data []byte, offset int, size int, storageClass uint32, isMigration bool) (read int, err error)
	Write(inode uint64, offset int, data []byte, flags int, checkFunc func() error, storageClass uint32, isMigration bool) (write int, err error)
	Flush(inode uint64) error
	Close() error
}

type EbsApi interface {
	Put(ctx context.Context, volName string, f io.Reader, size uint64) (oek []proto.ObjExtentKey, md5 [][]byte, err error)
	Get(ctx context.Context, volName string, offset uint64, size uint64, oek proto.ObjExtentKey) (body io.ReadCloser, err error)
}

type TransitionMgr struct {
	volume    string
	ec        ExtentApi // extent client for read
	ecForW    ExtentApi // extent client for write
	ebsClient EbsApi
}

func (t *TransitionMgr) migrate(e *proto.ScanDentry) (err error) {
	if e.Size == 0 {
		log.LogInfof("skip migration, size=0, inode(%v)", e.Inode)
		return
	}
	if err = t.ec.OpenStream(e.Inode, false, false, ""); err != nil {
		log.LogWarnf("migrate: ec OpenStream fail, inode(%v) err: %v", e.Inode, err)
		return
	}
	defer func() {
		if closeErr := t.ec.CloseStream(e.Inode); closeErr != nil {
			log.LogWarnf("migrate: ec CloseStream fail, inode(%v) err: %v", e.Inode, closeErr)
		}
	}()
	if err = t.ecForW.OpenStream(e.Inode, false, false, ""); err != nil {
		log.LogWarnf("migrate: ecForW OpenStream fail, inode(%v) err: %v", e.Inode, err)
		return
	}
	defer func() {
		if closeErr := t.ecForW.CloseStream(e.Inode); closeErr != nil {
			log.LogWarnf("migrate: ecForW CloseStream fail, inode(%v) err: %v", e.Inode, closeErr)
		}
	}()

	var (
		md5Hash     = md5.New()
		md5Value    string
		readN       int
		writeN      int
		readOffset  int
		writeOffset int
		readSize    int
		rest        int
		buf         = make([]byte, 2*util.BlockSize)
	)

	for {
		if rest = int(e.Size) - readOffset; rest <= 0 {
			break
		}
		readSize = len(buf)
		if rest < len(buf) {
			readSize = rest
		}
		buf = buf[:readSize]

		readN, err = t.ec.Read(e.Inode, buf, readOffset, readSize, e.StorageClass, false)
		if err != nil && err != io.EOF {
			err = fmt.Errorf("read source file err(%v)", err)
			log.LogWarnf("migrate: inode(%v) readOffset(%v) storageClass(%v): %v",
				e.Inode, readOffset, e.StorageClass, err)
			return
		}
		if readN > 0 {
			writeN, err = t.ecForW.Write(e.Inode, writeOffset, buf[:readN], 0, nil, proto.OpTypeToStorageType(e.Op), true)
			if err != nil {
				err = fmt.Errorf("write dst file err(%v)", err)
				log.LogWarnf("migrate: inode(%v), writeOffset(%v): %v", e.Inode, writeOffset, err)
				return
			}
			readOffset += readN
			writeOffset += writeN
			// copy to md5 buffer, and then write to md5
			md5Hash.Write(buf[:readN])
		}
		if err == io.EOF {
			err = nil
			break
		}
	}

	if err = t.ecForW.Flush(e.Inode); err != nil {
		err = fmt.Errorf("write dst file flush err(%v)", err)
		log.LogWarnf("migrate: inode(%v): %v", e.Inode, err)
		return
	}

	md5Value = hex.EncodeToString(md5Hash.Sum(nil))
	log.LogInfof("migrate file finished, inode(%v), md5Value: %v", e.Inode, md5Value)

	// check read from src extent
	srcMd5Hash := md5.New()
	err = t.readFromExtentClient(e, srcMd5Hash, false, 0, 0)
	if err != nil {
		err = fmt.Errorf("check src file err(%v)", err)
		log.LogWarnf("check: inode(%v): %v", e.Inode, err)
		return
	}
	srcMd5 := hex.EncodeToString(srcMd5Hash.Sum(nil))
	log.LogInfof("check: read src file finished, inode(%v), srcmd5: %v", e.Inode, srcMd5)

	if srcMd5 != md5Value {
		err = fmt.Errorf("check src md5 inconsistent, inode(%v), srcMd5(%v), md5Value(%v)", e.Inode, srcMd5, md5Value)
		return
	}

	// check read from dst migration extent
	dstMd5Hash := md5.New()
	err = t.readFromExtentClient(e, dstMd5Hash, true, 0, 0)
	if err != nil {
		err = fmt.Errorf("check dst file err(%v)", err)
		log.LogWarnf("check: inode(%v): %v", e.Inode, err)
		return
	}
	dstMd5 := hex.EncodeToString(dstMd5Hash.Sum(nil))
	log.LogInfof("check: read dst file finished, inode(%v), dstMd5: %v", e.Inode, dstMd5)

	if dstMd5 != md5Value {
		err = fmt.Errorf("check dst md5 inconsistent, inode(%v), dstMd5(%v), md5Value(%v)", e.Inode, dstMd5, md5Value)
		return
	}

	log.LogInfof("migrate and check md5 finished, vol(%v) inode(%v) path(%v), will do UpdateExtentKeyAfterMigration",
		t.volume, e.Inode, e.Path)
	return
}

func (t *TransitionMgr) readFromExtentClient(e *proto.ScanDentry, writer io.Writer, isMigrationExtent bool, from, size int) (err error) {
	var (
		readN      int
		readOffset int
		readSize   int
		rest       int
		buf        = make([]byte, 2*util.BlockSize)
	)

	if size > 0 {
		readOffset = from
	} else {
		size = int(e.Size)
	}

	for {
		if rest = (from + size) - readOffset; rest <= 0 {
			break
		}
		readSize = len(buf)
		if rest < len(buf) {
			readSize = rest
		}
		buf = buf[:readSize]

		if isMigrationExtent {
			readN, err = t.ecForW.Read(e.Inode, buf, readOffset, readSize, e.StorageClass, isMigrationExtent)
		} else {
			readN, err = t.ec.Read(e.Inode, buf, readOffset, readSize, e.StorageClass, isMigrationExtent)
		}

		if err != nil && err != io.EOF {
			return
		}
		if readN > 0 {
			readOffset += readN
			if _, er := writer.Write(buf[:readN]); er != nil {
				return er
			}
		}
		if err == io.EOF {
			err = nil
			break
		}
	}
	return
}

func (t *TransitionMgr) migrateToEbs(e *proto.ScanDentry) (oek []proto.ObjExtentKey, err error) {
	if e.Size == 0 {
		log.LogInfof("skip migration, size=0, inode(%v)", e.Inode)
		return
	}
	if err = t.ec.OpenStream(e.Inode, false, false, ""); err != nil {
		log.LogWarnf("migrate blobstore: OpenStream fail, inode(%v) err: %v", e.Inode, err)
		return
	}
	defer func() {
		if closeErr := t.ec.CloseStream(e.Inode); closeErr != nil {
			log.LogWarnf("migrate blobstore: CloseStream fail, inode(%v) err: %v", e.Inode, closeErr)
		}
	}()

	var srcErr error
	var dstErr error
	r, w := io.Pipe()
	go func() {
		srcErr = t.readFromExtentClient(e, w, false, 0, 0)
		if srcErr != nil {
			srcErr = fmt.Errorf("read src file err(%v)", srcErr)
			log.LogWarnf("migrate blobstore: inode(%v): %v", e.Inode, srcErr)
		}
		w.CloseWithError(srcErr)
	}()

	ctx := context.Background()
	oek, _md5, dstErr := t.ebsClient.Put(ctx, t.volume, r, e.Size)
	if dstErr != nil {
		dstErr = fmt.Errorf("write dst file err(%v)", dstErr)
		log.LogWarnf("migrate blobstore: inode(%v): %v", e.Inode, dstErr)
	}
	r.Close()

	if srcErr != nil || dstErr != nil {
		err = fmt.Errorf("srcErr(%v), dstErr(%v)", srcErr, dstErr)
		return
	}

	var md5Value []string
	for _, m := range _md5 {
		md5Value = append(md5Value, hex.EncodeToString(m))
	}
	log.LogInfof("migrate blobstore finished, inode(%v), oek: %v, md5Value: %v", e.Inode, oek, md5Value)

	// check read from extent
	var srcMd5 []string
	var from int
	var part uint64 = util.ExtentSize
	rest := e.Size
	for rest > 0 {
		var getSize uint64
		if rest > part {
			getSize = part
		} else {
			getSize = rest
		}
		rest -= getSize
		srcMd5Hash := md5.New()
		err = t.readFromExtentClient(e, srcMd5Hash, false, from, int(getSize))
		if err != nil {
			err = fmt.Errorf("check src file err(%v)", err)
			log.LogWarnf("migrate blobstore: inode(%v) check err: %v", e.Inode, err)
			return
		}
		srcMd5 = append(srcMd5, hex.EncodeToString(srcMd5Hash.Sum(nil)))
		from += int(getSize)
	}
	log.LogInfof("migrate blobstore check finished, inode(%v), srcmd5: %v", e.Inode, srcMd5)

	if strings.Join(srcMd5, ",") != strings.Join(md5Value, ",") {
		err = fmt.Errorf("migrate blobstore check md5 inconsistent, inode(%v), srcMd5: %v, md5Value: %v", e.Inode, srcMd5, md5Value)
		return
	}
	log.LogInfof("migrate blobstore and check finished, inode(%v)", e.Inode)
	return
}
