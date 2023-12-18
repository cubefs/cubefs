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
	"io"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type ExtentApi interface {
	OpenStream(inode uint64, openForWrite, isCache bool) error
	CloseStream(inode uint64) error
	Read(inode uint64, data []byte, offset int, size int, storageClass uint32, isMigration bool) (read int, err error)
	Write(inode uint64, offset int, data []byte, flags int, checkFunc func() error, storageClass uint32, isMigration bool) (write int, err error)
	Flush(inode uint64) error
}

type EbsApi interface {
	Put(ctx context.Context, volName string, f io.Reader, size uint64) (oek proto.ObjExtentKey, md5 []byte, err error)
	Get(ctx context.Context, volName string, offset uint64, size uint64, oek proto.ObjExtentKey) (body io.ReadCloser, err error)
}

type TransitionMgr struct {
	volume    string
	ec        ExtentApi
	ebsClient EbsApi
}

func (t *TransitionMgr) migrate(e *proto.ScanDentry) (err error) {
	if err = t.ec.OpenStream(e.Inode, false, false); err != nil {
		log.LogErrorf("migrate: OpenStream fail, inode(%v) err: %v", e.Inode, err)
		return
	}
	defer func() {
		if closeErr := t.ec.CloseStream(e.Inode); err != nil {
			log.LogErrorf("migrate: CloseStream fail, inode(%v) err: %v", e.Inode, closeErr)
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
		hashBuf     = make([]byte, 2*util.BlockSize)
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
			return
		}
		if readN > 0 {
			writeN, err = t.ec.Write(e.Inode, writeOffset, buf[:readN], 0, nil, proto.OpTypeToStorageType(e.Op), true)
			if err != nil {
				log.LogErrorf("migrate: ebs write err: %v, inode(%v), target offset(%v)", err, e.Inode, writeOffset)
				return
			}
			readOffset += readN
			writeOffset += writeN
			// copy to md5 buffer, and then write to md5
			copy(hashBuf, buf[:readN])
			md5Hash.Write(hashBuf[:readN])
		}
		if err == io.EOF {
			err = nil
			break
		}
	}

	if err = t.ec.Flush(e.Inode); err != nil {
		log.LogErrorf("migrate: ec flush err: %v, inode(%v)", err, e.Inode)
		return
	}

	md5Value = hex.EncodeToString(md5Hash.Sum(nil))
	log.LogDebugf("migrate file finished, inode(%v), md5Value: %v", e.Inode, md5Value)

	//check read from src extent
	srcMd5Hash := md5.New()
	err = t.readFromExtentClient(e, srcMd5Hash, false)
	if err != nil {
		log.LogErrorf("check: read from src extent err: %v, inode(%v)", err, e.Inode)
		return
	}
	srcMd5 := hex.EncodeToString(srcMd5Hash.Sum(nil))
	log.LogDebugf("check: read src file finished, inode(%v), srcmd5: %v", e.Inode, srcMd5)

	if srcMd5 != md5Value {
		err = errors.NewErrorf("check src md5 inconsistent, srcMd5: %v, md5Value: %v", srcMd5, md5Value)
		return
	}

	//check read from dst migration extent
	dstMd5Hash := md5.New()
	err = t.readFromExtentClient(e, dstMd5Hash, true)
	if err != nil {
		log.LogErrorf("check: read from dst extent err: %v, inode(%v)", err, e.Inode)
		return
	}
	dstMd5 := hex.EncodeToString(dstMd5Hash.Sum(nil))
	log.LogDebugf("check: read dst file finished, inode(%v), dstMd5: %v", e.Inode, dstMd5)

	if dstMd5 != md5Value {
		err = errors.NewErrorf("check dst md5 inconsistent, dstMd5: %v, md5Value: %v", dstMd5, md5Value)
		return
	}

	log.LogInfof("migrate and check finished, inode(%v)", e.Inode)
	return
}

func (t *TransitionMgr) readFromExtentClient(e *proto.ScanDentry, writer io.Writer, isMigrationExtent bool) (err error) {
	var (
		readN      int
		readOffset int
		readSize   int
		rest       int
		buf        = make([]byte, 2*util.BlockSize)
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

		readN, err = t.ec.Read(e.Inode, buf, readOffset, readSize, e.StorageClass, isMigrationExtent)
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

func (t *TransitionMgr) migrateToEbs(e *proto.ScanDentry) (oeks []proto.ObjExtentKey, err error) {
	if err = t.ec.OpenStream(e.Inode, false, false); err != nil {
		log.LogErrorf("migrateToEbs: OpenStream fail, inode(%v) err: %v", e.Inode, err)
		return
	}
	defer func() {
		if closeErr := t.ec.CloseStream(e.Inode); err != nil {
			log.LogErrorf("migrateToEbs: CloseStream fail, inode(%v) err: %v", e.Inode, closeErr)
		}
	}()

	r, w := io.Pipe()
	go func() {
		err = t.readFromExtentClient(e, w, false)
		if err != nil {
			log.LogErrorf("migrateToEbs: read from extent err: %v, inode(%v)", err, e.Inode)
		}
		w.CloseWithError(err)
	}()

	ctx := context.Background()
	oek, m, err := t.ebsClient.Put(ctx, t.volume, r, e.Size)
	if err != nil {
		log.LogErrorf("migrateToEbs: ebs put err: %v, inode(%v)", err, e.Inode)
		r.Close()
		return
	}
	r.Close()
	md5Value := hex.EncodeToString(m)
	log.LogDebugf("migrateToEbs finished, inode(%v), oek: %v, md5Value: %v", e.Inode, oek, md5Value)

	//check read from extent
	srcMd5Hash := md5.New()
	err = t.readFromExtentClient(e, srcMd5Hash, false)
	if err != nil {
		log.LogErrorf("migrateToEbs: check err: %v, inode(%v)", err, e.Inode)
		return
	}
	srcMd5 := hex.EncodeToString(srcMd5Hash.Sum(nil))
	log.LogDebugf("migrateToEbs check finished, inode(%v), srcmd5: %v", e.Inode, srcMd5)

	if srcMd5 != md5Value {
		err = errors.NewErrorf("migrateToEbs check md5 inconsistent, srcMd5: %v, md5Value: %v", srcMd5, md5Value)
		return
	}
	oeks = []proto.ObjExtentKey{oek}
	log.LogInfof("migrateToEbs and check finished, inode(%v)", e.Inode)
	return
}
