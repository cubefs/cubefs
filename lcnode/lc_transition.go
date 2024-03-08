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
	"strings"

	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"io"
	"sync"
)

type ExtentApi interface {
	OpenStream(inode uint64, openForWrite, isCache bool) error
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

type TransitionInterface interface {
	migrate(e *proto.ScanDentry) (err error)
	migrateToEbs(e *proto.ScanDentry) (oeks []proto.ObjExtentKey, err error)
}

type RemoteTransitionMgr struct {
	volume    string
	ec        ExtentApi // extent client for read
	ecForW    ExtentApi // extent client for write
	ebsClient EbsApi
}

func (rtm *RemoteTransitionMgr) migrate(e *proto.ScanDentry) (err error) {
	if e.Size == 0 {
		log.LogInfof("skip migration, size=0, inode(%v)", e.Inode)
		return
	}
	if err = rtm.ec.OpenStream(e.Inode, false, false); err != nil {
		log.LogErrorf("migrate: ec OpenStream fail, inode(%v) err: %v", e.Inode, err)
		return
	}
	defer func() {
		if closeErr := rtm.ec.CloseStream(e.Inode); closeErr != nil {
			log.LogErrorf("migrate: ec CloseStream fail, inode(%v) err: %v", e.Inode, closeErr)
		}
	}()
	if err = rtm.ecForW.OpenStream(e.Inode, false, false); err != nil {
		log.LogErrorf("migrate: ecForW OpenStream fail, inode(%v) err: %v", e.Inode, err)
		return
	}
	defer func() {
		if closeErr := rtm.ecForW.CloseStream(e.Inode); closeErr != nil {
			log.LogErrorf("migrate: ecForW CloseStream fail, inode(%v) err: %v", e.Inode, closeErr)
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

		readN, err = rtm.ec.Read(e.Inode, buf, readOffset, readSize, e.StorageClass, false)
		if err != nil && err != io.EOF {
			return
		}
		if readN > 0 {
			writeN, err = rtm.ecForW.Write(e.Inode, writeOffset, buf[:readN], 0, nil, proto.OpTypeToStorageType(e.Op), true)
			if err != nil {
				log.LogErrorf("migrate: ecForW write err: %v, inode(%v), target offset(%v)", err, e.Inode, writeOffset)
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

	if err = rtm.ecForW.Flush(e.Inode); err != nil {
		log.LogErrorf("migrate: ecForW flush err: %v, inode(%v)", err, e.Inode)
		return
	}

	md5Value = hex.EncodeToString(md5Hash.Sum(nil))
	log.LogDebugf("migrate file finished, inode(%v), md5Value: %v", e.Inode, md5Value)

	//check read from src extent
	srcMd5Hash := md5.New()
	err = rtm.readFromExtentClient(e, srcMd5Hash, false, 0, 0)
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
	err = rtm.readFromExtentClient(e, dstMd5Hash, true, 0, 0)
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

func (rtm *RemoteTransitionMgr) readFromExtentClient(e *proto.ScanDentry, writer io.Writer, isMigrationExtent bool, from, size int) (err error) {
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
			readN, err = rtm.ecForW.Read(e.Inode, buf, readOffset, readSize, e.StorageClass, isMigrationExtent)
		} else {
			readN, err = rtm.ec.Read(e.Inode, buf, readOffset, readSize, e.StorageClass, isMigrationExtent)
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

func (rtm *RemoteTransitionMgr) migrateToEbs(e *proto.ScanDentry) (oek []proto.ObjExtentKey, err error) {
	if e.Size == 0 {
		log.LogInfof("skip migration, size=0, inode(%v)", e.Inode)
		return
	}
	if err = rtm.ec.OpenStream(e.Inode, false, false); err != nil {
		log.LogErrorf("migrateToEbs: OpenStream fail, inode(%v) err: %v", e.Inode, err)
		return
	}
	defer func() {
		if closeErr := rtm.ec.CloseStream(e.Inode); closeErr != nil {
			log.LogErrorf("migrateToEbs: CloseStream fail, inode(%v) err: %v", e.Inode, closeErr)
		}
	}()

	r, w := io.Pipe()
	go func() {
		err = rtm.readFromExtentClient(e, w, false, 0, 0)
		if err != nil {
			log.LogErrorf("migrateToEbs: read from extent err: %v, inode(%v)", err, e.Inode)
		}
		w.CloseWithError(err)
	}()

	ctx := context.Background()
	oek, _md5, err := rtm.ebsClient.Put(ctx, rtm.volume, r, e.Size)
	if err != nil {
		log.LogErrorf("migrateToEbs: ebs put err: %v, inode(%v)", err, e.Inode)
		r.Close()
		return
	}
	r.Close()

	var md5Value []string
	for _, m := range _md5 {
		md5Value = append(md5Value, hex.EncodeToString(m))
	}
	log.LogDebugf("migrateToEbs finished, inode(%v), oek: %v, md5Value: %v", e.Inode, oek, md5Value)

	//check read from extent
	var srcMd5 []string
	var from int
	var part uint64 = util.ExtentSize
	var rest = e.Size
	for rest > 0 {
		var getSize uint64
		if rest > part {
			getSize = part
		} else {
			getSize = rest
		}
		rest -= getSize
		srcMd5Hash := md5.New()
		err = rtm.readFromExtentClient(e, srcMd5Hash, false, from, int(getSize))
		if err != nil {
			log.LogErrorf("migrateToEbs: check err: %v, inode(%v)", err, e.Inode)
			return
		}
		srcMd5 = append(srcMd5, hex.EncodeToString(srcMd5Hash.Sum(nil)))
		from += int(getSize)
	}
	log.LogDebugf("migrateToEbs check finished, inode(%v), srcmd5: %v", e.Inode, srcMd5)

	if strings.Join(srcMd5, ",") != strings.Join(md5Value, ",") {
		err = errors.NewErrorf("migrateToEbs check md5 inconsistent, srcMd5: %v, md5Value: %v", srcMd5, md5Value)
		return
	}
	log.LogInfof("migrateToEbs and check finished, inode(%v)", e.Inode)
	return
}

type LocalTransitionMgr struct {
	Volume                 string
	Masters                []string
	AppendExtentKeys       stream.AppendExtentKeysFunc
	GetExtents             stream.GetExtentsFunc
	VolStorageClass        uint32
	VolAllowedStorageClass []uint32
	DpWrapper              *wrapper.DataPartitionWrapper
}

func (ltm *LocalTransitionMgr) migrate(e *proto.ScanDentry) (err error) {
	// 1. get inode extents info
	gen, size, extents, err := ltm.GetExtents(e.Inode, false, false, false)
	if err != nil {
		return err
	}
	log.LogDebugf("local transition mgr: get extents of inode %v,  %v %v %v", e.Inode, gen, size, extents)

	// 2. group extents by data partition
	dp2Extents := map[uint64][]proto.ExtentKey{}
	for _, extent := range extents {
		dpId := extent.PartitionId
		dp2Extents[dpId] = append(dp2Extents[dpId], extent)
	}

	// 3. for each srcDp , select a dstDp (a dp which is co-located to srcDp is preferred )  and build extentsLocalTransitionReq for each dp
	var extentsLocalTransitionReqList []proto.ExtentsLocalTransitionRequest
	for dpId, dpExtents := range dp2Extents {

		srcDp, ok := ltm.DpWrapper.TryGetPartition(dpId)
		if !ok {
			errMsg := fmt.Sprintf("can't get dp [%v] information when using local transition mgr to migrate file %v", dpId, e.Path)
			log.LogErrorf(errMsg)
			return errors.New(errMsg)
		}

		hostsStr := srcDp.GetHostsString()
		preferredLocalDstDp, err := ltm.DpWrapper.GetDataPartitionForWrite(map[string]struct{}{}, hostsStr, proto.OpTypeToStorageType(e.Op), 0)
		if err != nil {
			errMsg := fmt.Sprintf("can't get another local dp for dp[%v] when using local transition mgr to migrate file %v", dpId, e.Path)
			log.LogErrorf(errMsg)
			return errors.New(errMsg)
		}
		if preferredLocalDstDp.GetHostsString() != hostsStr {
			errMsg := fmt.Sprintf("dp [%v][%v] and dp[%v][%v] hove different hosts ", dpId, hostsStr, preferredLocalDstDp.PartitionID, preferredLocalDstDp.GetHostsString())
			log.LogErrorf(errMsg)
			return errors.New(errMsg)
		}

		extentsLocalTransitionReqList = append(extentsLocalTransitionReqList,
			proto.ExtentsLocalTransitionRequest{SrcDp: srcDp.PartitionID, DstDp: preferredLocalDstDp.PartitionID, SrcExtents: dpExtents, Hosts: preferredLocalDstDp.Hosts})
	}

	log.LogDebugf("extentsLocalTransitionReqList len(%v) to send %v", len(extentsLocalTransitionReqList), extentsLocalTransitionReqList)

	// 4. send request to target datanode on which the srcDp and dstDp located
	var dpLocalTransitionRespList []proto.ExtentsLocalTransitionResponse
	if dpLocalTransitionRespList, err = ltm.sendDpLocalTransitionInfoToDataNode(extentsLocalTransitionReqList); err != nil {
		return err
	}

	// 5. update inode metadata with new extent list
	if err = ltm.updateInodeMetaInfo(e, dpLocalTransitionRespList); err != nil {
		return err
	}

	return nil
}

func (ltm *LocalTransitionMgr) sendDpLocalTransitionInfoToDataNode(dpLocalTransitionReqList []proto.ExtentsLocalTransitionRequest) ([]proto.ExtentsLocalTransitionResponse, error) {
	wg := sync.WaitGroup{}
	wg.Add(len(dpLocalTransitionReqList))
	responseC := make(chan proto.ExtentsLocalTransitionResponse, len(dpLocalTransitionReqList))

	for _, req := range dpLocalTransitionReqList {
		go func(req proto.ExtentsLocalTransitionRequest) {

			defer wg.Done()
			if resp, e := ltm.doSend(req.Hosts[0], req); e != nil { // send to first replica of dst dp
				log.LogErrorf("error when send ExtentsLocalTransitionRequest to %v, err %v,req :%v ", req.Hosts[0], e, req)
			} else {
				responseC <- *resp
			}
		}(req)
	}
	wg.Wait()

	close(responseC)
	if len(dpLocalTransitionReqList) != len(responseC) {
		return nil, errors.NewErrorf("error occurred when sendDpLocalTransitionInfoToDataNode")
	}
	var extentsLocalTransitionResponses []proto.ExtentsLocalTransitionResponse
	for resp := range responseC {
		extentsLocalTransitionResponses = append(extentsLocalTransitionResponses, resp)
	}

	return extentsLocalTransitionResponses, nil
}

func (ltm *LocalTransitionMgr) doSend(host string, request proto.ExtentsLocalTransitionRequest) (*proto.ExtentsLocalTransitionResponse, error) {
	conn, err := stream.StreamConnPool.GetConnect(host)
	if err != nil {
		errMsg := fmt.Sprintf("can't get conn of host %v when using local transition mgr", host)
		log.LogErrorf(errMsg)
		return nil, errors.New(errMsg)
	}
	defer func() {
		if err != nil {
			stream.StreamConnPool.PutConnect(conn, true)
		} else {
			stream.StreamConnPool.PutConnect(conn, false)
		}
	}()

	p, err := ltm.newExtentsLocalTransitionPacket(request)
	if err != nil {
		return nil, err
	}
	log.LogDebugf("local transition mgr : send request[%v] to data node %v, req: %v ", p.ReqID, host, request)
	if err = p.WriteToConn(conn); err != nil {
		errMsg := fmt.Sprintf("local transition mgr : failed to WriteToConn, packet(%v) host(%v)", p, host)
		log.LogErrorf(errMsg)
		return nil, errors.New(errMsg)
	}

	if err = p.ReadFromConnWithVer(conn, proto.ExtentsLocalTransitionDeadLineTime); err != nil {
		return nil, errors.Trace(err, "local transition mgr : failed to ReadFromConn, packet(%v) host(%v)", p, host)
	}
	if p.ResultCode != proto.OpOk {
		return nil, errors.New(fmt.Sprintf("local transition mgr : ResultCode NOK, packet(%v) host(%v) ResultCode(%v)", p, host, p.GetResultMsg()))
	}

	resp := &proto.ExtentsLocalTransitionResponse{}
	if err = json.Unmarshal(p.Data, resp); err != nil {
		errMsg := fmt.Sprintf("local transition mgr : failed to unmarshal resp, packet(%v) err %v", p, err)
		log.LogErrorf(errMsg)
		return nil, errors.New(errMsg)
	}
	log.LogDebugf("local transition mgr : receive resp[%v] from data node %v, resp: %v ", p.ReqID, host, resp)

	return resp, nil
}

func (ltm *LocalTransitionMgr) newExtentsLocalTransitionPacket(info proto.ExtentsLocalTransitionRequest) (*proto.Packet, error) {
	p := new(proto.Packet)
	p.ReqID = proto.GenerateRequestID()
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpExtentsLocalTransition
	p.PartitionID = info.SrcDp
	p.ExtentType = proto.NormalExtentType

	body, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	p.Size = uint32(len(body))
	p.Data = body

	p.Arg = ([]byte)(strings.Join(info.Hosts[1:], proto.AddrSplit) + proto.AddrSplit)
	p.ArgLen = uint32(len(p.Arg))
	p.RemainingFollowers = uint8(len(info.Hosts) - 1)
	if len(info.Hosts) == 1 {
		p.RemainingFollowers = 127
	}

	return p, nil
}

func (ltm *LocalTransitionMgr) updateInodeMetaInfo(e *proto.ScanDentry, extentsTransited []proto.ExtentsLocalTransitionResponse) error {
	var allExtentsTransited []proto.ExtentKey
	for _, response := range extentsTransited {
		allExtentsTransited = append(allExtentsTransited, response.DstExtents...)
	}

	if err := ltm.AppendExtentKeys(e.Inode, allExtentsTransited, proto.OpTypeToStorageType(e.Op), true); err != nil {
		errMsg := fmt.Sprintf("local transition mgr : update migrated extents of file [%v,%v] to meta node , err %v", e.Inode, e.Path, err)
		log.LogErrorf(errMsg)
		return errors.New(errMsg)
	}

	return nil
}
