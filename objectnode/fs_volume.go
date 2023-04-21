// Copyright 2019 The CubeFS Authors.
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

package objectnode

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

const (
	rootIno               = proto.RootIno
	OSSMetaUpdateDuration = time.Duration(time.Second * 30)
)

// AsyncTaskErrorFunc is a callback method definition for asynchronous tasks when an error occurs.
// It is mainly used to notify other objects when an error occurs during asynchronous task execution.
// These asynchronous tasks include periodic volume topology and metadata update tasks.
type AsyncTaskErrorFunc func(err error)

// OnError protects the call of AsyncTaskErrorFunc with null pointer access. Allocated to simplify caller code.
func (f AsyncTaskErrorFunc) OnError(err error) {
	if f != nil {
		f(err)
	}
}

// VolumeConfig is the configuration used to initialize the Volume instance.
type VolumeConfig struct {
	// Name of volume.
	// This is a required configuration item.
	Volume string

	// Master addresses or host names.
	// This is a required configuration item.
	Masters []string

	// Storage fro ACP management
	Store Store

	// Callback method for notifying when an error occurs in an asynchronous task.
	// Such as Volume topology and metadata update tasks.
	// This is a optional configuration item.
	OnAsyncTaskError AsyncTaskErrorFunc

	// Get OSSMeta from the MetaNode every time if it is set true.
	MetaStrict bool
}

type PutFileOption struct {
	MIMEType     string
	Disposition  string
	Tagging      *Tagging
	ACL          *AccessControlPolicy
	Metadata     map[string]string
	CacheControl string
	Expires      string
}

type ListFilesV1Option struct {
	Prefix     string
	Delimiter  string
	Marker     string
	MaxKeys    uint64
	OnlyObject bool
}

type ListFilesV1Result struct {
	Files          []*FSFileInfo
	NextMarker     string
	Truncated      bool
	CommonPrefixes []string
}

type ListFilesV2Option struct {
	Delimiter  string
	MaxKeys    uint64
	Prefix     string
	ContToken  string
	FetchOwner bool
	StartAfter string
}

type ListFilesV2Result struct {
	Files          []*FSFileInfo
	KeyCount       uint64
	NextToken      string
	Truncated      bool
	CommonPrefixes []string
}

// Volume is a high-level encapsulation of meta sdk and data sdk methods.
// A high-level approach that exposes the semantics of object storage to the outside world.
// Volume escapes high-level object storage semantics to low-level POSIX semantics.
type Volume struct {
	mw         *meta.MetaWrapper
	ec         *stream.ExtentClient
	store      Store // Storage for ACP management
	name       string
	owner      string
	metaLoader ossMetaLoader
	ticker     *time.Ticker
	createTime int64

	volType        int
	ebsBlockSize   int
	cacheAction    int
	cacheThreshold int

	closeOnce sync.Once
	closeCh   chan struct{}

	onAsyncTaskError AsyncTaskErrorFunc
}

func (v *Volume) GetOwner() string {
	return v.owner
}

func (v *Volume) syncOSSMeta() {
	v.ticker = time.NewTicker(OSSMetaUpdateDuration)
	defer v.ticker.Stop()
	for {
		select {
		case <-v.ticker.C:
			v.loadOSSMeta()
		case <-v.closeCh:
			return
		}
	}
}

// update Volume meta info
func (v *Volume) loadOSSMeta() {
	var err error
	defer func() {
		if err != nil {
			v.onAsyncTaskError.OnError(err)
		}
	}()
	var policy *Policy
	if policy, err = v.loadBucketPolicy(); err != nil {
		return
	}
	v.metaLoader.storePolicy(policy)

	var acl *AccessControlPolicy
	if acl, err = v.loadBucketACL(); err != nil {
		return
	}
	v.metaLoader.storeACL(acl)

	var cors *CORSConfiguration
	if cors, err = v.loadBucketCors(); err != nil { // if cors isn't exist, it may return nil. So it needs to be cleared manually when deleting cors.
		return
	}
	v.metaLoader.storeCors(cors)
}

func (v *Volume) Name() string {
	return v.name
}

func (v *Volume) Owner() string {
	return v.mw.Owner()
}

func (v *Volume) CreateTime() time.Time {
	return time.Unix(v.createTime, 0)
}

// load bucket policy from vm
func (v *Volume) loadBucketPolicy() (policy *Policy, err error) {
	var data []byte
	data, err = v.store.Get(v.name, bucketRootPath, XAttrKeyOSSPolicy)
	if err != nil {
		log.LogErrorf("loadBucketPolicy: load bucket policy fail: Volume(%v) err(%v)", v.name, err)
		return
	}
	if len(data) == 0 {
		return
	}
	policy = &Policy{}
	if err = json.Unmarshal(data, policy); err != nil {
		return
	}
	return
}

func (v *Volume) loadBucketACL() (acp *AccessControlPolicy, err error) {
	var raw []byte
	if raw, err = v.store.Get(v.name, bucketRootPath, XAttrKeyOSSACL); err != nil {
		return
	}
	if len(raw) == 0 {
		return
	}
	acp = &AccessControlPolicy{}
	if err = xml.Unmarshal(raw, acp); err != nil {
		return
	}
	return
}

func (v *Volume) loadBucketCors() (configuration *CORSConfiguration, err error) {
	var raw []byte
	if raw, err = v.store.Get(v.name, bucketRootPath, XAttrKeyOSSCORS); err != nil {
		return
	}
	if len(raw) == 0 {
		return
	}
	configuration = &CORSConfiguration{}
	if err = xml.Unmarshal(raw, configuration); err != nil {
		return
	}
	return configuration, nil
}

func (v *Volume) getInodeFromPath(path string) (inode uint64, err error) {
	if path == "/" {
		return volumeRootInode, nil
	}

	dirs, filename := splitPath(path)

	if len(dirs) == 0 && filename == "" {
		return volumeRootInode, nil
	} else {
		// process path
		var parentId uint64
		if parentId, err = v.lookupDirectories(dirs, false); err != nil {
			return 0, err
		}
		log.LogDebugf("GetXAttr: lookup directories: path(%v) parentId(%v)", path, parentId)
		// check file
		var lookupMode uint32
		inode, lookupMode, err = v.mw.Lookup_ll(parentId, filename)
		if err != nil {
			return 0, err
		}
		if os.FileMode(lookupMode).IsDir() {
			err = syscall.ENOENT
			return 0, err
		}
	}

	return
}

func (v *Volume) SetXAttr(path string, key string, data []byte, autoCreate bool) error {
	var err error
	var inode uint64
	if inode, err = v.getInodeFromPath(path); err != nil && err != syscall.ENOENT {
		return err
	}
	if err == syscall.ENOENT && !autoCreate {
		return err
	}
	if err == syscall.ENOENT {
		var dirs, filename = splitPath(path)
		var parentID uint64
		if parentID, err = v.lookupDirectories(dirs, true); err != nil {
			return err
		}
		var inodeInfo *proto.InodeInfo
		if inodeInfo, err = v.mw.Create_ll(parentID, filename, DefaultFileMode, 0, 0, nil); err != nil {
			return err
		}
		inode = inodeInfo.Inode
	}

	err = v.mw.XAttrSet_ll(inode, []byte(key), data)
	if err == nil {
		if objMetaCache != nil {
			attrItem := &AttrItem{
				XAttrInfo: proto.XAttrInfo{
					Inode:  inode,
					XAttrs: make(map[string]string, 0),
				},
			}
			attrItem.XAttrs[key] = string(data)
			objMetaCache.MergeAttr(v.name, attrItem)
		}
	}

	return err
}

func (v *Volume) getXAttr(path string, key string) (info *proto.XAttrInfo, err error) {
	var inode uint64
	inode, err = v.getInodeFromPath(path)
	if err != nil {
		return
	}
	if info, err = v.mw.XAttrGet_ll(inode, key); err != nil {
		log.LogErrorf("getXAttr: meta get xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
		return
	}
	return
}

func (v *Volume) IsEmpty() bool {
	children, err := v.mw.ReadDir_ll(proto.RootIno)
	if err != nil {
		log.LogErrorf("IsEmpty: parent ino(%v) err(%v)", proto.RootIno, err)
		return false
	}

	if len(children) > 0 {
		log.LogDebugf("IsEmpty: parent ino(%v), children: %v", proto.RootIno, children)
		return false
	}
	return true
}

func (v *Volume) GetXAttr(path string, key string) (info *proto.XAttrInfo, err error) {
	var inode uint64

	if objMetaCache != nil {
		var retry = 0
		for {
			if _, inode, _, _, err = v.recursiveLookupTarget(path); err != nil {
				return v.getXAttr(path, key)
			}

			_, err = v.mw.InodeGet_ll(inode)
			if err == syscall.ENOENT && retry < MaxRetry {
				retry++
				continue
			}
			if err != nil {
				log.LogErrorf("GetXAttr: get inode fail: volume(%v) path(%v) inode(%v) retry(%v) err(%v)", v.name, path, inode, retry, err)
				return v.getXAttr(path, key)
			}
			break
		}

		info = &proto.XAttrInfo{
			Inode:  inode,
			XAttrs: make(map[string]string, 0),
		}

		var attr *proto.XAttrInfo
		attrItem, needRefresh := objMetaCache.GetAttr(v.name, inode)
		if attrItem == nil || needRefresh {
			log.LogDebugf("GetXAttr: get attr in cache failed: volume(%v) inode(%v) attrItem(%v), needRefresh(%v)",
				v.name, inode, attrItem, needRefresh)
			if attr, err = v.mw.XAttrGetAll_ll(inode); err != nil {
				log.LogErrorf("XAttrGetAll_ll: meta get xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
				return v.getXAttr(path, key)
			} else {
				attrItem = &AttrItem{
					XAttrInfo: *attr,
				}
				objMetaCache.PutAttr(v.name, attrItem)
				val, ok := attr.XAttrs[key]
				if !ok {
					log.LogErrorf("XAttrGetAll_ll: meta get xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
					return v.getXAttr(path, key)
				} else {
					info.XAttrs[key] = val
					return
				}
			}
		} else {
			val, ok := attrItem.XAttrs[key]
			if !ok {
				if attr, err = v.mw.XAttrGetAll_ll(inode); err != nil {
					log.LogErrorf("XAttrGetAll_ll: meta get xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
					return v.getXAttr(path, key)
				} else {
					val, ok := attr.XAttrs[key]
					if !ok {
						log.LogErrorf("XAttrGetAll_ll: meta get xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
						return v.getXAttr(path, key)
					} else {
						info.XAttrs[key] = val
						return
					}
				}
			} else {
				info.XAttrs[key] = val
				return
			}
		}

	} else {
		return v.getXAttr(path, key)
	}
}

func (v *Volume) DeleteXAttr(path string, key string) (err error) {
	inode, err1 := v.getInodeFromPath(path)
	if err1 != nil {
		err = err1
		return
	}
	if err = v.mw.XAttrDel_ll(inode, key); err != nil {
		log.LogErrorf("SetXAttr: meta set xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
		return
	}
	if objMetaCache != nil {
		objMetaCache.DeleteAttrWithKey(v.name, inode, key)
	}
	return
}

func (v *Volume) listXAttrs(path string) (keys []string, err error) {
	var inode uint64
	inode, err = v.getInodeFromPath(path)
	if err != nil {
		return
	}
	if keys, err = v.mw.XAttrsList_ll(inode); err != nil {
		log.LogErrorf("GetXAttr: meta get xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
		return
	}
	return
}

func (v *Volume) ListXAttrs(path string) (keys []string, err error) {
	var inode uint64

	if objMetaCache != nil {
		var retry = 0
		for {

			if _, inode, _, _, err = v.recursiveLookupTarget(path); err != nil {
				return v.listXAttrs(path)
			}

			_, err = v.mw.InodeGet_ll(inode)
			if err == syscall.ENOENT && retry < MaxRetry {
				retry++
				continue
			}
			if err != nil {
				log.LogErrorf("ListXAttrs: get inode fail: volume(%v) path(%v) inode(%v) retry(%v) err(%v)", v.name, path, inode, retry, err)
				return v.listXAttrs(path)
			}
			break
		}

		attrItem, needRefresh := objMetaCache.GetAttr(v.name, inode)
		if attrItem == nil || needRefresh {
			log.LogDebugf("ListXAttrs: get attr in cache failed: volume(%v) inode(%v) attrItem(%v), needRefresh(%v)",
				v.name, inode, attrItem, needRefresh)
			if attr, err := v.mw.XAttrGetAll_ll(inode); err != nil {
				log.LogErrorf("XAttrGetAll_ll: meta get xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
				return v.listXAttrs(path)
			} else {
				attrItem = &AttrItem{
					XAttrInfo: *attr,
				}
				objMetaCache.PutAttr(v.name, attrItem)
				for key := range attr.XAttrs {
					keys = append(keys, key)
				}
			}
		} else {
			for key := range attrItem.XAttrs {
				keys = append(keys, key)
			}
		}

	} else {
		return v.listXAttrs(path)
	}

	return
}

func (v *Volume) OSSSecure() (accessKey, secretKey string) {
	return v.mw.OSSSecure()
}

// ListFilesV1 returns file and directory entry list information that meets the parameters.
// It supports parameters such as prefix, delimiter, and paging.
// It is a data plane logical encapsulation of the object storage interface ListObjectsV1.
func (v *Volume) ListFilesV1(opt *ListFilesV1Option) (result *ListFilesV1Result, err error) {
	marker := opt.Marker
	prefix := opt.Prefix
	maxKeys := opt.MaxKeys
	delimiter := opt.Delimiter
	onlyObject := opt.OnlyObject

	var infos []*FSFileInfo
	var prefixes Prefixes
	var nextMarker string

	infos, prefixes, nextMarker, err = v.listFilesV1(prefix, marker, delimiter, maxKeys, onlyObject)
	if err != nil {
		log.LogErrorf("ListFilesV1: list fail: volume(%v) prefix(%v) marker(%v) delimiter(%v) maxKeys(%v) nextMarker(%v) err(%v)",
			v.name, prefix, marker, delimiter, maxKeys, nextMarker, err)
		return
	}

	result = &ListFilesV1Result{
		CommonPrefixes: prefixes,
	}

	result.NextMarker = nextMarker
	result.Files = infos
	if len(nextMarker) > 0 {
		result.Truncated = true
	}

	return
}

// ListFilesV2 returns file and directory entry list information that meets the parameters.
// It supports parameters such as prefix, delimiter, and paging.
// It is a data plane logical encapsulation of the object storage interface ListObjectsV2.
func (v *Volume) ListFilesV2(opt *ListFilesV2Option) (result *ListFilesV2Result, err error) {
	delimiter := opt.Delimiter
	maxKeys := opt.MaxKeys
	prefix := opt.Prefix
	contToken := opt.ContToken
	startAfter := opt.StartAfter

	var infos []*FSFileInfo
	var prefixes Prefixes
	var nextMarker string

	infos, prefixes, nextMarker, err = v.listFilesV2(prefix, startAfter, contToken, delimiter, maxKeys)
	if err != nil {
		log.LogErrorf("ListFilesV2: list fail: volume(%v) prefix(%v) startAfter(%v) contToken(%v) delimiter(%v) maxKeys(%v) err(%v)",
			v.name, prefix, startAfter, contToken, delimiter, maxKeys, err)
		return
	}

	result = &ListFilesV2Result{
		CommonPrefixes: prefixes,
	}

	result.Files = infos
	result.KeyCount = uint64(len(infos))
	if nextMarker != "" {
		result.Truncated = true
		result.NextToken = nextMarker
	}
	return
}

// PutObject creates or updates target path objects and data.
// Differentiate whether a target is a file or a directory by identifying its MIME type.
// When the MIME type is "application/directory", the target object is a directory.
// During processing, conflicts may occur because the actual type of the target object is
// different from the expected type.
//
// For example, create a directory called "backup", but a file called "backup" already exists.
// When a conflict occurs, the method returns an syscall.EINVAL error.
//
// An syscall.EINVAL error is returned indicating that a part of the target path expected to be a file
// but actual is a directory.
// An syscall.EINVAL error is returned indicating that a part of the target path expected to be a directory
// but actual is a file.
func (v *Volume) PutObject(path string, reader io.Reader, opt *PutFileOption) (fsInfo *FSFileInfo, err error) {
	defer func() {
		// Audit behavior
		log.LogInfof("Audit: PutObject: volume(%v) path(%v) err(%v)", v.name, path, err)
	}()
	// The path is processed according to the content-type. If it is a directory type,
	// a path separator is appended at the end of the path, so the recursiveMakeDirectory
	// method can be processed directly in recursion.
	var fixedPath = path
	if opt != nil && opt.MIMEType == HeaderValueContentTypeDirectory && !strings.HasSuffix(path, pathSep) {
		fixedPath = path + pathSep
	}

	var pathItems = NewPathIterator(fixedPath).ToSlice()
	if len(pathItems) == 0 {
		// A blank directory entry indicates that the path after the validation is the volume
		// root directory itself.
		fsInfo = &FSFileInfo{
			Path:       path,
			Size:       0,
			Mode:       DefaultDirMode,
			CreateTime: time.Now(),
			ModifyTime: time.Now(),
			ETag:       EmptyContentMD5String,
			Inode:      rootIno,
			MIMEType:   HeaderValueContentTypeDirectory,
		}
		return fsInfo, nil
	}
	var parentId uint64
	if parentId, err = v.recursiveMakeDirectory(fixedPath); err != nil {
		log.LogErrorf("PutObject: recursive make directory fail: volume(%v) path(%v) err(%v)",
			v.name, path, err)
		return
	}
	var lastPathItem = pathItems[len(pathItems)-1]
	if lastPathItem.IsDirectory {
		// If the last path node is a directory, then it has been processed by the previous logic.
		// Just get the information of this node and return.
		var info *proto.InodeInfo
		if info, err = v.mw.InodeGet_ll(parentId); err != nil {
			log.LogErrorf("PutObject: inode get fail: volume(%v) path(%v) inode(%v) err(%v)",
				v.name, path, parentId, err)
			return
		}
		fsInfo = &FSFileInfo{
			Path:       path,
			Size:       0,
			Mode:       DefaultDirMode,
			CreateTime: info.CreateTime,
			ModifyTime: info.ModifyTime,
			ETag:       EmptyContentMD5String,
			Inode:      info.Inode,
			MIMEType:   HeaderValueContentTypeDirectory,
		}
		return
	}

	// check file
	var lookupMode uint32
	_, lookupMode, err = v.mw.Lookup_ll(parentId, lastPathItem.Name)
	if err != nil && err != syscall.ENOENT {
		log.LogErrorf("PutObject: lookup name fail: volume(%v) path(%v) parentInode(%v) name(%v) err(%v)",
			v.name, path, parentId, lastPathItem.Name, err)
		return
	}
	if err == nil && os.FileMode(lookupMode).IsDir() {
		log.LogErrorf("PutObject: the last name is a dir: volume(%v) path(%v) name(%v)",
			v.name, path, lastPathItem.Name)
		err = syscall.EINVAL
		return
	}

	// Intermediate data during the writing of new versions is managed through invisible files.
	// This file has only inode but no dentry. In this way, this temporary file can be made invisible
	// in the true sense. In order to avoid the adverse impact of other user operations on temporary data.
	var invisibleTempDataInode *proto.InodeInfo
	if invisibleTempDataInode, err = v.mw.InodeCreate_ll(DefaultFileMode, 0, 0, nil, make([]uint64, 0)); err != nil {
		log.LogErrorf("PutObject: inode create fail: volume(%v) path(%v) err(%v)", v.name, path, err)
		return
	}
	defer func() {
		// An error has caused the entire process to fail. Delete the inode and release the written data.
		if err != nil {
			log.LogWarnf("PutObject: unlink temp inode: volume(%v) path(%v) inode(%v)",
				v.name, path, invisibleTempDataInode.Inode)
			_, _ = v.mw.InodeUnlink_ll(invisibleTempDataInode.Inode)
			log.LogWarnf("PutObject: evict temp inode: volume(%v) path(%v) inode(%v)",
				v.name, path, invisibleTempDataInode.Inode)
			_ = v.mw.Evict(invisibleTempDataInode.Inode)
		}
	}()

	var (
		md5Hash  = md5.New()
		md5Value string
	)

	if err = v.ec.OpenStream(invisibleTempDataInode.Inode); err != nil {
		log.LogErrorf("PutObject: open stream fail: volume(%v) path(%v) inode(%v) err(%v)",
			v.name, path, invisibleTempDataInode.Inode, err)
		return
	}
	defer func() {
		if closeErr := v.ec.CloseStream(invisibleTempDataInode.Inode); closeErr != nil {
			log.LogErrorf("PutObject: close stream fail: volume(%v) inode(%v) err(%v)",
				v.name, invisibleTempDataInode.Inode, closeErr)
		}
	}()
	if proto.IsCold(v.volType) {
		if _, err = v.ebsWrite(invisibleTempDataInode.Inode, reader, md5Hash); err != nil {
			log.LogErrorf("PutObject: ebs write fail: volume(%v) path(%v) inode(%v) err(%v)",
				v.name, path, invisibleTempDataInode.Inode, err)
			return
		}

	} else {

		if _, err = v.streamWrite(invisibleTempDataInode.Inode, reader, md5Hash); err != nil {
			log.LogErrorf("PutObject: stream write fail: volume(%v) path(%v) inode(%v) err(%v)",
				v.name, path, invisibleTempDataInode.Inode, err)
			return
		}
		// flush
		if err = v.ec.Flush(invisibleTempDataInode.Inode); err != nil {
			log.LogErrorf("PutObject: data flush inode fail: volume(%v) path(%v) inode(%v) err(%v)",
				v.name, path, invisibleTempDataInode.Inode, err)
			return nil, err
		}
	}

	// compute file md5
	md5Value = hex.EncodeToString(md5Hash.Sum(nil))

	var finalInode *proto.InodeInfo
	if finalInode, err = v.mw.InodeGet_ll(invisibleTempDataInode.Inode); err != nil {
		log.LogErrorf("PutObject: get final inode fail: volume(%v) path(%v) inode(%v) err(%v)",
			v.name, path, invisibleTempDataInode.Inode, err)
		return
	}

	var etagValue = ETagValue{
		Value:   md5Value,
		PartNum: 0,
		TS:      finalInode.ModifyTime,
	}

	attr := &AttrItem{
		XAttrInfo: proto.XAttrInfo{
			Inode:  invisibleTempDataInode.Inode,
			XAttrs: make(map[string]string),
		},
	}

	attr.XAttrs[XAttrKeyOSSETag] = etagValue.Encode()
	if opt != nil && opt.MIMEType != "" {
		attr.XAttrs[XAttrKeyOSSMIME] = opt.MIMEType
	}
	if opt != nil && len(opt.Disposition) > 0 {
		attr.XAttrs[XAttrKeyOSSDISPOSITION] = opt.Disposition
	}
	if opt != nil && opt.Tagging != nil {
		attr.XAttrs[XAttrKeyOSSTagging] = opt.Tagging.Encode()
	}
	if opt != nil && len(opt.CacheControl) > 0 {
		attr.XAttrs[XAttrKeyOSSCacheControl] = opt.CacheControl
	}
	if opt != nil && len(opt.Expires) > 0 {
		attr.XAttrs[XAttrKeyOSSExpires] = opt.Expires
	}
	if opt != nil && opt.ACL != nil {
		attr.XAttrs[XAttrKeyOSSACL] = opt.ACL.XmlEncode()
	}

	// If user-defined metadata have been specified, use extend attributes for storage.
	if opt != nil && len(opt.Metadata) > 0 {
		for name, value := range opt.Metadata {
			attr.XAttrs[name] = value
			log.LogDebugf("PutObject: store user-defined metadata: "+
				"volume(%v) path(%v) inode(%v) key(%v) value(%v)",
				v.name, path, invisibleTempDataInode.Inode, name, value)
		}
	}

	if err = v.mw.BatchSetXAttr_ll(invisibleTempDataInode.Inode, attr.XAttrs); err != nil {
		log.LogErrorf("PutObject: BatchSetXAttr_ll fail: volume(%v) path(%v) inode(%v) attrs(%v) err(%v)",
			v.name, path, invisibleTempDataInode.Inode, attr.XAttrs, err)
		return nil, err
	}
	log.LogDebugf("PutObject: BatchSetXAttr_ll success: volume(%v) path(%v) inode(%v) attrs(%v)",
		v.name, path, invisibleTempDataInode.Inode, attr.XAttrs)
	// create file info
	fsInfo = &FSFileInfo{
		Path:       path,
		Size:       int64(finalInode.Size),
		Mode:       os.FileMode(finalInode.Mode),
		CreateTime: finalInode.CreateTime,
		ModifyTime: finalInode.ModifyTime,
		ETag:       etagValue.ETag(),
		Inode:      finalInode.Inode,
	}

	// apply new inode to dentry
	err = v.applyInodeToDEntry(parentId, lastPathItem.Name, invisibleTempDataInode.Inode)
	if err != nil {
		log.LogErrorf("PutObject: apply new inode to dentry fail: parentID(%v) name(%v) inode(%v) err(%v)",
			parentId, lastPathItem.Name, invisibleTempDataInode.Inode, err)
		return
	}

	//force updating dentry and attrs in cache
	if objMetaCache != nil {
		dentry := &DentryItem{
			Dentry: metanode.Dentry{
				ParentId: parentId,
				Name:     lastPathItem.Name,
				Inode:    invisibleTempDataInode.Inode,
				Type:     DefaultFileMode,
			},
		}
		objMetaCache.PutDentry(v.name, dentry)
		objMetaCache.PutAttr(v.name, attr)
	}

	return fsInfo, nil
}

func (v *Volume) applyInodeToDEntry(parentId uint64, name string, inode uint64) (err error) {
	var existMode uint32
	_, existMode, err = v.mw.Lookup_ll(parentId, name)
	if err != nil && err != syscall.ENOENT {
		log.LogErrorf("applyInodeToDEntry: meta lookup fail: parentID(%v) name(%v) err(%v)", parentId, name, err)
		return
	}

	if err == syscall.ENOENT {
		if err = v.applyInodeToNewDentry(parentId, name, inode); err != nil {
			log.LogErrorf("applyInodeToDEntry: apply inode to new dentry fail: parentID(%v) name(%v) inode(%v) err(%v)",
				parentId, name, inode, err)
			return
		}
		log.LogDebugf("applyInodeToDEntry: apply inode to new dentry: parentID(%v) name(%v) inode(%v)",
			parentId, name, inode)
	} else {
		if os.FileMode(existMode).IsDir() {
			log.LogErrorf("applyInodeToDEntry: target mode conflict: parentID(%v) name(%v) mode(%v)",
				parentId, name, os.FileMode(existMode).String())
			err = syscall.EINVAL
			return
		}
		//current implementation dosen't support object versioning, so uploading a object with a key already existed in bucket
		// is implemented with replacing the old one instead.
		// refer: https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html
		if err = v.applyInodeToExistDentry(parentId, name, inode); err != nil {
			log.LogErrorf("applyInodeToDEntry: apply inode to exist dentry fail: parentID(%v) name(%v) inode(%v) err(%v)",
				parentId, name, inode, err)
			return
		}
	}
	return
}

// DeletePath deletes the specified path.
// If the target is a non-empty directory, it will return success without any operation.
// If the target does not exist, it returns success.
//
// Notes:
// This method will only returns internal system errors.
// This method will not return syscall.ENOENT error
func (v *Volume) DeletePath(path string) (err error) {
	defer func() {
		// Audit behavior
		log.LogInfof("Audit: DeletePath: volume(%v) path(%v), err(%v)", v.name, path, err)
	}()
	defer func() {
		// In the operation of deleting a path, if no path matching the given path is found,
		// that is, the given path does not exist, the return is successful.
		if err == syscall.ENOENT {
			err = nil
		}
	}()
	var parent uint64
	var ino uint64
	var name string
	var mode os.FileMode
	parent, ino, name, mode, err = v.recursiveLookupTarget(path)
	if err != nil {
		// An unexpected error occurred
		return
	}
	log.LogDebugf("DeletePath: lookup target: path(%v) parentID(%v) inode(%v) name(%v) mode(%v)",
		path, parent, ino, name, mode)
	if mode.IsDir() {
		// Check if the directory is empty and cannot delete non-empty directories.
		var dentries []proto.Dentry
		dentries, err = v.mw.ReadDirLimit_ll(ino, "", 1)
		if err != nil || len(dentries) > 0 {
			return
		}
	}
	log.LogWarnf("DeletePath: delete: volume(%v) path(%v) inode(%v)", v.name, path, ino)
	if _, err = v.mw.Delete_ll(parent, name, mode.IsDir()); err != nil {
		return
	}

	// Evict inode
	if err = v.ec.EvictStream(ino); err != nil {
		log.LogWarnf("DeletePath EvictStream: path(%v) inode(%v)", path, ino)
	}

	var dentry = &DentryItem{
		Dentry: metanode.Dentry{
			ParentId: parent,
			Name:     name,
		},
	}
	if objMetaCache != nil {
		objMetaCache.DeleteDentry(v.name, dentry.Key())
		objMetaCache.DeleteAttr(v.name, ino)
	}

	log.LogWarnf("DeletePath: evict: volume(%v) path(%v) inode(%v)", v.name, path, ino)
	if err = v.mw.Evict(ino); err != nil {
		log.LogWarnf("DeletePath Evict: path(%v) inode(%v)", path, ino)
	}
	err = nil
	return
}

func (v *Volume) InitMultipart(path string, opt *PutFileOption) (multipartID string, err error) {
	defer func() {
		log.LogInfof("Audit: InitMultipart: volume(%v) path(%v) multipartID(%v) err(%v)", v.name, path, multipartID, err)
	}()

	extend := make(map[string]string)
	// handle object system metadata, self-defined metadata, tagging
	if opt != nil && opt.MIMEType != "" {
		extend[XAttrKeyOSSMIME] = opt.MIMEType
	}
	// If request contain content-disposition header, store it to xattr
	if opt != nil && len(opt.Disposition) > 0 {
		extend[XAttrKeyOSSDISPOSITION] = opt.Disposition
	}
	// If request contain cache-control header, store it to xattr
	if opt != nil && len(opt.CacheControl) > 0 {
		extend[XAttrKeyOSSCacheControl] = opt.CacheControl
	}
	// If request contain expires header, store it to xattr
	if opt != nil && len(opt.Expires) > 0 {
		extend[XAttrKeyOSSExpires] = opt.Expires
	}
	// If user-defined metadata have been specified, use extend attributes for storage.
	if opt != nil && len(opt.Metadata) > 0 {
		for name, value := range opt.Metadata {
			extend[name] = value
		}
	}
	// If tagging have been specified, use extend attributes for storage.
	if opt != nil && opt.Tagging != nil {
		var encoded = opt.Tagging.Encode()
		extend[XAttrKeyOSSTagging] = encoded
	}
	// If ACL have been specified, use extend attributes for storage.
	if opt != nil && opt.ACL != nil {
		extend[XAttrKeyOSSACL] = opt.ACL.XmlEncode()
	}

	// Iterate all the meta partition to create multipart id
	multipartID, err = v.mw.InitMultipart_ll(path, extend)
	if err != nil {
		log.LogErrorf("InitMultipart: meta init multipart fail: path(%v) err(%v)", path, err)
		return "", err
	}
	return multipartID, nil
}

func (v *Volume) WritePart(path string, multipartId string, partId uint16, reader io.Reader) (*FSFileInfo, error) {
	var exist bool
	var err error
	defer func() {
		// Audit behavior
		log.LogInfof("Audit: WritePart: volume(%v) path(%v) multipartID(%v) partID(%v) exist(%v) err(%v)",
			v.name, path, multipartId, partId, exist, err)
	}()

	var fInfo *FSFileInfo
	_, fileName := splitPath(path)

	// create temp file (inode only, invisible for user)
	var tempInodeInfo *proto.InodeInfo
	if tempInodeInfo, err = v.mw.InodeCreate_ll(DefaultFileMode, 0, 0, nil, make([]uint64, 0)); err != nil {
		log.LogErrorf("WritePart: meta create inode fail: multipartID(%v) partID(%v) err(%v)",
			multipartId, partId, err)
		return nil, err
	}
	log.LogDebugf("WritePart: meta create temp file inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
		v.name, path, multipartId, partId, tempInodeInfo.Inode)

	var oldInode uint64
	defer func() {
		// An error has caused the entire process to fail. Delete the inode and release the written data.
		if err != nil {
			log.LogWarnf("WritePart: unlink part inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
				v.name, path, multipartId, partId, tempInodeInfo.Inode)
			_, _ = v.mw.InodeUnlink_ll(tempInodeInfo.Inode)
			log.LogWarnf("WritePart: evict part inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
				v.name, path, multipartId, partId, tempInodeInfo.Inode)
			_ = v.mw.Evict(tempInodeInfo.Inode)
		}
		// Delete the old inode and release the written data.
		if exist {
			log.LogWarnf("WritePart: unlink old part inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
				v.name, path, multipartId, partId, oldInode)
			_, _ = v.mw.InodeUnlink_ll(oldInode)
			log.LogWarnf("WritePart: evict old part inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
				v.name, path, multipartId, partId, oldInode)
			_ = v.mw.Evict(oldInode)
		}
	}()

	var (
		size    uint64
		etag    string
		md5Hash = md5.New()
	)
	if err = v.ec.OpenStream(tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WritePart: data open stream fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
			v.name, path, multipartId, partId, tempInodeInfo.Inode, err)
		return nil, err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(tempInodeInfo.Inode); closeErr != nil {
			log.LogErrorf("WritePart: data close stream fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
				v.name, path, multipartId, partId, tempInodeInfo.Inode, closeErr)
		}
	}()
	if proto.IsCold(v.volType) {
		if size, err = v.ebsWrite(tempInodeInfo.Inode, reader, md5Hash); err != nil {
			log.LogErrorf("WritePart: ebs write fail: volume(%v) inode(%v) multipartID(%v) partID(%v) err(%v)",
				v.name, tempInodeInfo.Inode, multipartId, partId, err)
			return nil, err
		}
	} else {
		// Write data to data node
		if size, err = v.streamWrite(tempInodeInfo.Inode, reader, md5Hash); err != nil {
			log.LogErrorf("WritePart: stream write fail: volume(%v) inode(%v) multipartID(%v) partID(%v) err(%v)",
				v.name, tempInodeInfo.Inode, multipartId, partId, err)
			return nil, err
		}
		// flush
		if err = v.ec.Flush(tempInodeInfo.Inode); err != nil {
			log.LogErrorf("WritePart: data flush inode fail: volume(%v) inode(%v) err(%v)", v.name, tempInodeInfo.Inode, err)
			return nil, err
		}
	}

	// compute file md5
	etag = hex.EncodeToString(md5Hash.Sum(nil))

	// update temp file inode to meta with session, overwrite existing part can result in exist == true
	oldInode, exist, err = v.mw.AddMultipartPart_ll(path, multipartId, partId, size, etag, tempInodeInfo)
	if err != nil {
		log.LogErrorf("WritePart: meta add multipart part fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) size(%v) MD5(%v) err(%v)",
			v.name, path, multipartId, partId, tempInodeInfo.Inode, size, etag, err)
		return nil, err
	}
	log.LogDebugf("WritePart: meta add multipart part: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) size(%v) MD5(%v)",
		v.name, path, multipartId, partId, tempInodeInfo.Inode, size, etag)
	// create file info
	fInfo = &FSFileInfo{
		Path:       fileName,
		Size:       int64(size),
		Mode:       os.FileMode(DefaultFileMode),
		ModifyTime: time.Now(),
		CreateTime: tempInodeInfo.CreateTime,
		ETag:       etag,
		Inode:      tempInodeInfo.Inode,
	}
	return fInfo, nil
}

func (v *Volume) AbortMultipart(path string, multipartID string) (err error) {
	defer func() {
		log.LogInfof("Audit: AbortMultipart: volume(%v) path(%v) multipartID(%v) err(%v)",
			v.name, path, multipartID, err)
	}()

	// get multipart info
	var multipartInfo *proto.MultipartInfo
	if multipartInfo, err = v.mw.GetMultipart_ll(path, multipartID); err != nil {
		log.LogErrorf("AbortMultipart: meta get multipart fail: volume(%v) multipartID(%v) path(%v) err(%v)",
			v.name, multipartID, path, err)
		return
	}
	// release part data
	for _, part := range multipartInfo.Parts {
		log.LogWarnf("AbortMultipart: unlink part inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
			v.name, path, multipartID, part.ID, part.Inode)
		if _, err = v.mw.InodeUnlink_ll(part.Inode); err != nil {
			log.LogErrorf("AbortMultipart: meta inode unlink fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
				v.name, path, multipartID, part.ID, part.Inode, err)
		}
		log.LogWarnf("AbortMultipart: evict part inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
			v.name, path, multipartID, part.ID, part.Inode)
		if err = v.mw.Evict(part.Inode); err != nil {
			log.LogErrorf("AbortMultipart: meta inode evict fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
				v.name, path, multipartID, part.ID, part.Inode, err)
		}
		log.LogDebugf("AbortMultipart: multipart part data released: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
			v.name, path, multipartID, part.ID, part.Inode)
	}

	if err = v.mw.RemoveMultipart_ll(path, multipartID); err != nil {
		log.LogErrorf("AbortMultipart: meta abort multipart fail: volume(%v) path(%v) multipartID(%v) err(%v)",
			v.name, path, multipartID, err)
		return err
	}
	log.LogDebugf("AbortMultipart: meta abort multipart: volume(%v) path(%v) multipartID(%v) path(%v)",
		v.name, path, multipartID, path)
	return nil
}

func (v *Volume) CompleteMultipart(path, multipartID string, multipartInfo *proto.MultipartInfo, discardedPartInodes map[uint64]uint16) (fsFileInfo *FSFileInfo, err error) {
	defer func() {
		log.LogInfof("Audit: CompleteMultipart: volume(%v) path(%v) multipartID(%v) err(%v)",
			v.name, path, multipartID, err)
	}()

	parts := multipartInfo.Parts
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })

	// create inode for complete data
	var completeInodeInfo *proto.InodeInfo
	if completeInodeInfo, err = v.mw.InodeCreate_ll(DefaultFileMode, 0, 0, nil, make([]uint64, 0)); err != nil {
		log.LogErrorf("CompleteMultipart: meta inode create fail: volume(%v) path(%v) multipartID(%v) err(%v)",
			v.name, path, multipartID, err)
		return
	}
	log.LogDebugf("CompleteMultipart: meta inode create: volume(%v) path(%v) multipartID(%v) inode(%v)",
		v.name, path, multipartID, completeInodeInfo.Inode)
	defer func() {
		if err != nil {
			log.LogWarnf("CompleteMultipart: destroy inode: volume(%v) path(%v) multipartID(%v) inode(%v)",
				v.name, path, multipartID, completeInodeInfo.Inode)
			if deleteErr := v.mw.InodeDelete_ll(completeInodeInfo.Inode); deleteErr != nil {
				log.LogErrorf("CompleteMultipart: meta delete complete inode fail: volume(%v) path(%v) multipartID(%v) inode(%v) err(%v)",
					v.name, path, multipartID, completeInodeInfo.Inode, err)
			}
		}
	}()

	// merge complete extent keys
	var size uint64
	var fileOffset uint64
	if proto.IsCold(v.volType) {
		var completeObjExtentKeys = make([]proto.ObjExtentKey, 0)
		for _, part := range parts {
			var objExtents []proto.ObjExtentKey
			if _, _, _, objExtents, err = v.mw.GetObjExtents(part.Inode); err != nil {
				log.LogErrorf("CompleteMultipart: meta get objextents fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
					v.name, path, multipartID, part.ID, part.Inode, err)
				return
			}
			for _, ek := range objExtents {
				ek.FileOffset = fileOffset
				fileOffset += uint64(ek.Size)
				completeObjExtentKeys = append(completeObjExtentKeys, ek)
			}
			size += part.Size
		}
		if err = v.mw.AppendObjExtentKeys(completeInodeInfo.Inode, completeObjExtentKeys); err != nil {
			log.LogErrorf("CompleteMultipart: meta append extent keys fail: volume(%v) path(%v) multipartID(%v) inode(%v) err(%v)",
				v.name, path, multipartID, completeInodeInfo.Inode, err)
			return
		}
	} else {
		var completeExtentKeys = make([]proto.ExtentKey, 0)
		for _, part := range parts {
			var eks []proto.ExtentKey
			if _, _, eks, err = v.mw.GetExtents(part.Inode); err != nil {
				log.LogErrorf("CompleteMultipart: meta get extents fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
					v.name, path, multipartID, part.ID, part.Inode, err)
				return
			}
			// recompute offsets of extent keys
			for _, ek := range eks {
				ek.FileOffset = fileOffset
				fileOffset += uint64(ek.Size)
				completeExtentKeys = append(completeExtentKeys, ek)
			}
			size += part.Size
		}
		if err = v.mw.AppendExtentKeys(completeInodeInfo.Inode, completeExtentKeys); err != nil {
			log.LogErrorf("CompleteMultipart: meta append extent keys fail: volume(%v) path(%v) multipartID(%v) inode(%v) err(%v)",
				v.name, path, multipartID, completeInodeInfo.Inode, err)
			return
		}
	}

	// compute md5 hash
	var md5Val string
	if len(parts) == 1 {
		md5Val = parts[0].MD5
	} else {
		var md5Hash = md5.New()
		for _, part := range parts {
			md5Hash.Write([]byte(part.MD5))
		}
		md5Val = hex.EncodeToString(md5Hash.Sum(nil))
	}
	log.LogDebugf("CompleteMultipart: merge parts: volume(%v) path(%v) multipartID(%v) numParts(%v) MD5(%v)",
		v.name, path, multipartID, len(parts), md5Val)

	var (
		pathItems = NewPathIterator(path).ToSlice()
		filename  = pathItems[len(pathItems)-1].Name
		parentId  uint64
	)
	if parentId, err = v.recursiveMakeDirectory(path); err != nil {
		log.LogErrorf("PutObject: recursive make directory fail: volume(%v) path(%v) err(%v)",
			v.name, path, err)
		return
	}

	var finalInode *proto.InodeInfo
	if finalInode, err = v.mw.InodeGet_ll(completeInodeInfo.Inode); err != nil {
		log.LogErrorf("CompleteMultipart: get inode fail: volume(%v) inode(%v) err(%v)",
			v.name, completeInodeInfo.Inode, err)
		return
	}

	var etagValue = ETagValue{
		Value:   md5Val,
		PartNum: len(parts),
		TS:      finalInode.ModifyTime,
	}

	attrs := make(map[string]string)
	attrs[XAttrKeyOSSETag] = etagValue.Encode()
	// set user modified system metadata, self defined metadata and tag
	extend := multipartInfo.Extend
	if len(extend) > 0 {
		for key, value := range extend {
			attrs[key] = value
		}
	}
	if err = v.mw.BatchSetXAttr_ll(finalInode.Inode, attrs); err != nil {
		log.LogErrorf("CompleteMultipart: store multipart extend fail: volume(%v) path(%v) inode(%v) attrs(%v) err(%v)",
			v.name, path, finalInode.Inode, attrs, err)
		return nil, err
	}

	// remove multipart
	err = v.mw.RemoveMultipart_ll(path, multipartID)
	if err == syscall.ENOENT {
		log.LogWarnf("CompleteMultipart: removing not exist multipart: volume(%v) multipartID(%v) path(%v)",
			v.name, multipartID, path)
	}
	if err != nil {
		log.LogErrorf("CompleteMultipart: meta complete multipart fail: volume(%v) multipartID(%v) path(%v) err(%v)",
			v.name, multipartID, path, err)
		return nil, err
	}
	// delete part inodes
	for _, part := range parts {
		log.LogWarnf("CompleteMultipart: destroy part inode: volume(%v) multipartID(%v) partID(%v) inode(%v)",
			v.name, multipartID, part.ID, part.Inode)
		if err = v.mw.InodeDelete_ll(part.Inode); err != nil {
			log.LogErrorf("CompleteMultipart: destroy part inode fail: volume(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
				v.name, multipartID, part.ID, part.Inode, err)
		}
	}

	//discard part inodes
	for discardedInode, partNum := range discardedPartInodes {
		log.LogDebugf("CompleteMultipart: discard part number(%v)", partNum)
		if _, err = v.mw.InodeUnlink_ll(discardedInode); err != nil {
			log.LogWarnf("CompleteMultipart: unlink inode fail: volume(%v) inode(%v) err(%v)",
				v.name, discardedInode, err)
		}
	}

	log.LogDebugf("CompleteMultipart: meta complete multipart: volume(%v) multipartID(%v) path(%v) parentID(%v) inode(%v) etagValue(%v)",
		v.name, multipartID, path, parentId, finalInode.Inode, etagValue)

	// create file info
	fInfo := &FSFileInfo{
		Path:       path,
		Size:       int64(size),
		Mode:       os.FileMode(DefaultFileMode),
		CreateTime: time.Now(),
		ModifyTime: time.Now(),
		ETag:       etagValue.ETag(),
		Inode:      finalInode.Inode,
	}

	// apply new inode to dentry
	err = v.applyInodeToDEntry(parentId, filename, completeInodeInfo.Inode)
	if err != nil {
		log.LogErrorf("CompleteMultipart: apply new inode to dentry fail, parent id (%v), file name(%v), inode(%v)",
			parentId, filename, completeInodeInfo.Inode)
	}
	return fInfo, nil
}

func (v *Volume) ebsWrite(inode uint64, reader io.Reader, h hash.Hash) (size uint64, err error) {
	ctx := context.Background()
	size, err = v.getEbsWriter(inode).WriteFromReader(ctx, reader, h)
	return
}

func (v *Volume) streamWrite(inode uint64, reader io.Reader, h hash.Hash) (size uint64, err error) {
	var (
		buf                   = make([]byte, 2*util.BlockSize)
		readN, writeN, offset int
		hashBuf               = make([]byte, 2*util.BlockSize)
	)
	for {
		readN, err = reader.Read(buf)
		if err != nil && err != io.EOF {
			return
		}
		if readN > 0 {
			if writeN, err = v.ec.Write(inode, offset, buf[:readN], 0); err != nil {
				log.LogErrorf("streamWrite: data write tmp file fail, inode(%v) offset(%v) err(%v)", inode, offset, err)
				exporter.Warning(fmt.Sprintf("write data fail: volume(%v) inode(%v) offset(%v) size(%v) err(%v)",
					v.name, inode, offset, readN, err))
				return
			}
			offset += writeN
			// copy to md5 buffer, and then write to md5
			size += uint64(writeN)
			copy(hashBuf, buf[:readN])
			if h != nil {
				h.Write(hashBuf[:readN])
			}
		}
		if err == io.EOF {
			err = nil
			break
		}
	}
	return
}

func (v *Volume) appendInodeHash(h hash.Hash, inode uint64, total uint64, preAllocatedBuf []byte) (err error) {
	if err = v.ec.OpenStream(inode); err != nil {
		log.LogErrorf("appendInodeHash: data open stream fail: inode(%v) err(%v)",
			inode, err)
		return
	}
	defer func() {
		if closeErr := v.ec.CloseStream(inode); closeErr != nil {
			log.LogWarnf("appendInodeHash: data close stream fail: inode(%v) err(%v)",
				inode, err)
		}
		if evictErr := v.ec.EvictStream(inode); evictErr != nil {
			log.LogWarnf("appendInodeHash: data evict stream: inode(%v) err(%v)",
				inode, err)
		}
	}()

	var buf = preAllocatedBuf
	if len(buf) == 0 {
		buf = make([]byte, 1024*64)
	}

	var n, offset, size int
	for {
		size = len(buf)
		rest := total - uint64(offset)
		if uint64(size) > rest {
			size = int(rest)
		}
		n, err = v.ec.Read(inode, buf, offset, size)
		if err != nil && err != io.EOF {
			log.LogErrorf("appendInodeHash: data read fail, inode(%v) offset(%v) size(%v) err(%v)", inode, offset, size, err)
			return
		}
		log.LogDebugf("appendInodeHash: data read, inode(%v) offset(%v) n(%v)", inode, offset, n)
		if n > 0 {
			if _, err = h.Write(buf[:n]); err != nil {
				return
			}
			offset += n
		}
		if n == 0 || err == io.EOF {
			break
		}
	}
	log.LogDebugf("appendInodeHash: append to hash function: inode(%v)", inode)
	return
}

func (v *Volume) applyInodeToNewDentry(parentID uint64, name string, inode uint64) (err error) {
	if err = v.mw.DentryCreate_ll(parentID, name, inode, DefaultFileMode); err != nil {
		log.LogErrorf("applyInodeToNewDentry: meta dentry create fail: parentID(%v) name(%v) inode(%v) mode(%v) err(%v)",
			parentID, name, inode, DefaultFileMode, err)
		return err
	}
	return
}

func (v *Volume) applyInodeToExistDentry(parentID uint64, name string, inode uint64) (err error) {
	var oldInode uint64
	oldInode, err = v.mw.DentryUpdate_ll(parentID, name, inode)
	if err != nil {
		log.LogErrorf("applyInodeToExistDentry: meta update dentry fail: parentID(%v) name(%v) inode(%v) err(%v)",
			parentID, name, inode, err)
		return
	}

	// unlink and evict old inode
	log.LogWarnf("applyInodeToExistDentry: unlink inode: volume(%v) inode(%v)", v.name, oldInode)
	if _, err = v.mw.InodeUnlink_ll(oldInode); err != nil {
		log.LogWarnf("applyInodeToExistDentry: unlink inode fail: volume(%v) inode(%v) err(%v)",
			v.name, oldInode, err)
	}

	log.LogWarnf("applyInodeToExistDentry: evict inode: volume(%v) inode(%v)", v.name, oldInode)
	if err = v.mw.Evict(oldInode); err != nil {
		log.LogWarnf("applyInodeToExistDentry: evict inode fail: volume(%v) inode(%v) err(%v)",
			v.name, oldInode, err)
	}
	err = nil
	return
}

func (v *Volume) loadUserDefinedMetadata(inode uint64) (metadata map[string]string, err error) {
	var storedXAttrKeys []string
	if storedXAttrKeys, err = v.mw.XAttrsList_ll(inode); err != nil {
		log.LogErrorf("loadUserDefinedMetadata: meta list xattr fail: volume(%v) inode(%v) err(%v)",
			v.name, inode, err)
		return
	}
	var xattrKeys = make([]string, 0)
	for _, storedXAttrKey := range storedXAttrKeys {
		if !strings.HasPrefix(storedXAttrKey, "oss:") {
			xattrKeys = append(xattrKeys, storedXAttrKey)
		}
	}
	var xattrs []*proto.XAttrInfo
	if xattrs, err = v.mw.BatchGetXAttr([]uint64{inode}, xattrKeys); err != nil {
		log.LogErrorf("loadUserDefinedMetadata: meta get xattr fail, volume(%v) inode(%v) keys(%v) err(%v)",
			v.name, inode, strings.Join(xattrKeys, ","), err)
		return
	}
	metadata = make(map[string]string)
	if len(xattrs) > 0 && xattrs[0].Inode == inode {
		xattrs[0].VisitAll(func(key string, value []byte) bool {
			metadata[key] = string(value)
			return true
		})
	}
	return
}

func (v *Volume) readFile(inode, inodeSize uint64, path string, writer io.Writer, offset, size uint64) (err error) {
	if err = v.ec.OpenStream(inode); err != nil {
		log.LogErrorf("readFile: data open stream fail, Inode(%v) err(%v)", inode, err)
		return err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(inode); closeErr != nil {
			log.LogErrorf("readFile: data close stream fail: inode(%v) err(%v)", inode, closeErr)
		}
	}()

	if proto.IsHot(v.volType) {
		return v.read(inode, inodeSize, path, writer, offset, size)
	} else {
		return v.readEbs(inode, inodeSize, path, writer, offset, size)
	}
}

func (v *Volume) readEbs(inode, inodeSize uint64, path string, writer io.Writer, offset, size uint64) error {
	var err error
	var upper = size + offset
	if upper > inodeSize {
		upper = inodeSize - offset
	}

	ctx := context.Background()
	_ = context.WithValue(ctx, "objectnode", 1)
	reader := v.getEbsReader(inode)
	var n int
	var rest uint64
	var tmp = make([]byte, 2*v.ebsBlockSize)

	for {
		if rest = upper - offset; rest <= 0 {
			break
		}
		readSize := len(tmp)
		if uint64(readSize) > rest {
			readSize = int(rest)
		}
		tmp = tmp[:readSize]
		n, err = reader.Read(ctx, tmp, int(offset), readSize)
		if err != nil && err != io.EOF {
			log.LogErrorf("ReadFile: data read fail: volume(%v) path(%v) inode(%v) offset(%v) size(%v) err(%v)",
				v.name, path, inode, offset, size, err)
			exporter.Warning(fmt.Sprintf("read data fail: volume(%v) path(%v) inode(%v) offset(%v) size(%v) err(%v)",
				v.name, path, inode, offset, readSize, err))
			return err
		}
		if n > 0 {
			if _, err = writer.Write(tmp[:n]); err != nil {
				return err
			}
			offset += uint64(n)
		}
		if n == 0 || err == io.EOF {
			break
		}
	}

	return nil
}

func (v *Volume) read(inode, inodeSize uint64, path string, writer io.Writer, offset, size uint64) error {
	var err error
	var upper = size + offset
	if upper > inodeSize {
		upper = inodeSize - offset
	}

	var n int
	var tmp = make([]byte, 2*util.BlockSize)
	for {
		var rest = upper - offset
		if rest == 0 {
			break
		}
		var readSize = len(tmp)
		if uint64(readSize) > rest {
			readSize = int(rest)
		}
		n, err = v.ec.Read(inode, tmp, int(offset), readSize)
		if err != nil && err != io.EOF {
			log.LogErrorf("ReadFile: data read fail: volume(%v) path(%v) inode(%v) offset(%v) size(%v) err(%v)",
				v.name, path, inode, offset, size, err)
			exporter.Warning(fmt.Sprintf("read data fail: volume(%v) path(%v) inode(%v) offset(%v) size(%v) err(%v)",
				v.name, path, inode, offset, readSize, err))
			return err
		}
		if n > 0 {
			if _, err = writer.Write(tmp[:n]); err != nil {
				return err
			}
			offset += uint64(n)
		}
		if n == 0 || err == io.EOF {
			break
		}
	}
	return nil
}

func (v *Volume) ReadFile(path string, writer io.Writer, offset, size uint64) error {
	var err error

	var ino uint64
	var mode os.FileMode
	if _, ino, _, mode, err = v.recursiveLookupTarget(path); err != nil {
		return err
	}

	if mode.IsDir() {
		return nil
	}
	// read file data
	var inoInfo *proto.InodeInfo
	if inoInfo, err = v.mw.InodeGet_ll(ino); err != nil {
		return err
	}

	return v.readFile(ino, inoInfo.Size, path, writer, offset, size)
}

func (v *Volume) ObjectMeta(path string) (info *FSFileInfo, xattr *proto.XAttrInfo, err error) {

	// process path
	var inode uint64
	var mode os.FileMode
	var inoInfo *proto.InodeInfo

	var retry = 0
	for {

		if _, inode, _, mode, err = v.recursiveLookupTarget(path); err != nil {
			return
		}

		inoInfo, err = v.mw.InodeGet_ll(inode)
		if err == syscall.ENOENT && retry < MaxRetry {
			retry++
			continue
		}
		if err != nil {
			log.LogErrorf("ObjectMeta: get inode fail: volume(%v) path(%v) inode(%v) retry(%v) err(%v)", v.name, path, inode, retry, err)
			return
		}
		break
	}

	var (
		etagValue    ETagValue
		mimeType     string
		disposition  string
		cacheControl string
		expires      string
	)

	if objMetaCache != nil {
		attrItem, needRefresh := objMetaCache.GetAttr(v.name, inode)
		if attrItem == nil || needRefresh {
			log.LogDebugf("ObjectMeta: get attr in cache failed: volume(%v) inode(%v) attrItem(%v), needRefresh(%v)",
				v.name, inode, attrItem, needRefresh)
			xattr, err = v.mw.XAttrGetAll_ll(inode)
			if err != nil {
				log.LogErrorf("ObjectMeta:  XAttrGetAll_ll fail, volume(%v) inode(%v) path(%v) err(%v)",
					v.name, inode, path, err)
				return
			} else {
				attrItem = &AttrItem{
					XAttrInfo: *xattr,
				}
				objMetaCache.PutAttr(v.name, attrItem)
			}
		} else {
			xattr = &proto.XAttrInfo{
				XAttrs: attrItem.XAttrs,
				Inode:  attrItem.Inode,
			}
		}
	} else {
		xattr, err = v.mw.XAttrGetAll_ll(inode)
		if err != nil {
			log.LogErrorf("ObjectMeta:  XAttrGetAll_ll fail, volume(%v) inode(%v) path(%v) err(%v)",
				v.name, inode, path, err)
			return
		}
	}

	if mode.IsDir() {
		// Folder has specific ETag and MIME type.
		etagValue = DirectoryETagValue()
		mimeType = HeaderValueContentTypeDirectory
	} else {
		// Try to get the advanced attributes stored in the extended attributes.
		// The following advanced attributes apply to the object storage:
		// 1. Etag (MD5)
		// 2. MIME type
		mimeType = string(xattr.Get(XAttrKeyOSSMIME))
		disposition = string(xattr.Get(XAttrKeyOSSDISPOSITION))
		cacheControl = string(xattr.Get(XAttrKeyOSSCacheControl))
		expires = string(xattr.Get(XAttrKeyOSSExpires))
		var rawETag = string(xattr.Get(XAttrKeyOSSETag))
		if len(rawETag) == 0 {
			rawETag = string(xattr.Get(XAttrKeyOSSETagDeprecated))
		}
		if len(rawETag) > 0 {
			etagValue = ParseETagValue(rawETag)
		}
	}

	// Load user-defined metadata
	metadata := make(map[string]string, 0)
	for key, val := range xattr.XAttrs {
		if !strings.HasPrefix(key, "oss:") {
			metadata[key] = val
		}
	}

	// Validating ETag value.
	if !mode.IsDir() && (!etagValue.Valid() || etagValue.TS.Before(inoInfo.ModifyTime)) {
		// The ETag is invalid or outdated then generate a new ETag and make update.
		oldEtagVal := etagValue
		if etagValue, err = v.updateETag(inoInfo.Inode, int64(inoInfo.Size), inoInfo.ModifyTime); err != nil {
			log.LogErrorf("ObjectMeta: update ETag fail: volume(%v) path(%v) inoInfo(%v) etagVal(%v) err(%v)",
				v.name, path, inoInfo, err)
		}
		xattr.XAttrs[XAttrKeyOSSETag] = etagValue.Encode()
		log.LogWarnf("ObjectMeta: update ETag: volume(%v) path(%v) inoInfo(%v) oldEtagVal(%v) newEtagVal(%v)",
			v.name, path, inoInfo, oldEtagVal, etagValue)
	}

	info = &FSFileInfo{
		Path:         path,
		Size:         int64(inoInfo.Size),
		Mode:         os.FileMode(inoInfo.Mode),
		CreateTime:   inoInfo.CreateTime,
		ModifyTime:   inoInfo.ModifyTime,
		ETag:         etagValue.ETag(),
		Inode:        inoInfo.Inode,
		MIMEType:     mimeType,
		Disposition:  disposition,
		CacheControl: cacheControl,
		Expires:      expires,
		Metadata:     metadata,
	}
	return
}

func (v *Volume) Close() error {
	v.closeOnce.Do(func() {
		close(v.closeCh)
		_ = v.mw.Close()
		_ = v.ec.Close()
	})
	return nil
}

// Find the path recursively and return the exact inode information.
// When a path conflict is found, for example, a given path is a directory,
// and the actual search result is a non-directory, an ENOENT error is returned.
//
// ENOENT:
// 		0x2 ENOENT No such file or directory. A component of a specified
// 		pathname did not exist, or the pathname was an empty string.
func (v *Volume) recursiveLookupTarget(path string) (parent uint64, ino uint64, name string, mode os.FileMode, err error) {
	parent = rootIno
	var pathIterator = NewPathIterator(path)
	if !pathIterator.HasNext() {
		err = syscall.ENOENT
		return
	}

	cacheUsed := false

	if objMetaCache != nil {
		for pathIterator.HasNext() {
			var pathItem = pathIterator.Next()
			var curIno uint64
			var curMode uint32

			dentry := &DentryItem{
				Dentry: metanode.Dentry{
					ParentId: parent,
					Name:     pathItem.Name,
				},
			}
			var needRefresh bool
			dentry, needRefresh = objMetaCache.GetDentry(v.name, dentry.Key())
			if dentry == nil || needRefresh || os.FileMode(dentry.Type).IsDir() != pathItem.IsDirectory {
				log.LogDebugf("recursiveLookupTarget: get dentry in cache failed: volume(%v) dentry(%v) needRefresh(%v)",
					v.name, dentry, needRefresh)
				curIno, curMode, err = v.mw.Lookup_ll(parent, pathItem.Name)
				if err != nil && err != syscall.ENOENT {
					log.LogErrorf("recursiveLookupPath: lookup fail, parentID(%v) name(%v) fail err(%v)",
						parent, pathItem.Name, err)
					if cacheUsed {
						break
					} else {
						return
					}
				}
				if err == syscall.ENOENT {
					if cacheUsed {
						break
					} else {
						return
					}
				}

				//force updating dentry in cache
				dentryNew := &DentryItem{
					Dentry: metanode.Dentry{
						ParentId: parent,
						Name:     pathItem.Name,
						Inode:    curIno,
						Type:     curMode,
					},
				}
				objMetaCache.PutDentry(v.name, dentryNew)

				log.LogDebugf("recursiveLookupPath: lookup item: parentID(%v) inode(%v) name(%v) mode(%v)",
					parent, curIno, pathItem.Name, os.FileMode(curMode))
				// Check file mode
				if os.FileMode(curMode).IsDir() != pathItem.IsDirectory {
					err = syscall.ENOENT
					return
				}
				if pathIterator.HasNext() {
					parent = curIno
					continue
				}
				ino = curIno
				name = pathItem.Name
				mode = os.FileMode(curMode)
				break

			} else {
				cacheUsed = true
				if pathIterator.HasNext() {
					parent = dentry.Inode
					continue
				}
				ino = dentry.Inode
				name = pathItem.Name
				mode = os.FileMode(dentry.Type)
				break
			}
		}
		if err == nil {
			return
		}
	}

	parent = rootIno
	pathIterator = NewPathIterator(path)
	if !pathIterator.HasNext() {
		err = syscall.ENOENT
		return
	}
	for pathIterator.HasNext() {
		var pathItem = pathIterator.Next()
		var curIno uint64
		var curMode uint32
		curIno, curMode, err = v.mw.Lookup_ll(parent, pathItem.Name)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("recursiveLookupPath: lookup fail, parentID(%v) name(%v) fail err(%v)",
				parent, pathItem.Name, err)
			return
		}
		if err == syscall.ENOENT {
			if objMetaCache != nil {
				dentry := &DentryItem{
					Dentry: metanode.Dentry{
						ParentId: parent,
						Name:     pathItem.Name,
					},
				}
				objMetaCache.DeleteDentry(v.name, dentry.Key())
			}
			return
		}

		//force updating dentry in cache
		if objMetaCache != nil {
			dentry := &DentryItem{
				Dentry: metanode.Dentry{
					ParentId: parent,
					Name:     pathItem.Name,
					Inode:    curIno,
					Type:     curMode,
				},
			}
			objMetaCache.PutDentry(v.name, dentry)
		}

		log.LogDebugf("recursiveLookupPath: lookup item: parentID(%v) inode(%v) name(%v) mode(%v)",
			parent, curIno, pathItem.Name, os.FileMode(curMode))
		// Check file mode
		if os.FileMode(curMode).IsDir() != pathItem.IsDirectory {
			err = syscall.ENOENT
			return
		}
		if pathIterator.HasNext() {
			parent = curIno
			continue
		}
		ino = curIno
		name = pathItem.Name
		mode = os.FileMode(curMode)
		break
	}
	return
}

func (v *Volume) recursiveMakeDirectory(path string) (ino uint64, err error) {
	//in case of any mv or rename operation within refresh interval of dentry item in cache,
	//recursiveMakeDirectory don't look up cache, and will force update dentry item
	ino = rootIno
	var pathIterator = NewPathIterator(path)
	if !pathIterator.HasNext() {
		err = syscall.ENOENT
		return
	}
	for pathIterator.HasNext() {
		var pathItem = pathIterator.Next()
		if !pathItem.IsDirectory {
			break
		}
		var curIno uint64
		var curMode uint32
		curIno, curMode, err = v.mw.Lookup_ll(ino, pathItem.Name)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("recursiveMakeDirectory: lookup fail, parentID(%v) name(%v) fail err(%v)",
				ino, pathItem.Name, err)
			return
		}
		if err == syscall.ENOENT {
			var info *proto.InodeInfo
			info, err = v.mw.Create_ll(ino, pathItem.Name, uint32(DefaultDirMode), 0, 0, nil)
			if err != nil && err == syscall.EEXIST {
				existInode, mode, e := v.mw.Lookup_ll(ino, pathItem.Name)
				if e != nil {
					return
				}
				if os.FileMode(mode).IsDir() {
					ino, err = existInode, nil
					continue
				}
			}
			if err != nil {
				return
			}
			curIno, curMode = info.Inode, info.Mode
		}

		//force updating dentry in cache
		if objMetaCache != nil {
			dentry := &DentryItem{
				Dentry: metanode.Dentry{
					ParentId: ino,
					Name:     pathItem.Name,
					Inode:    curIno,
					Type:     curMode,
				},
			}
			objMetaCache.PutDentry(v.name, dentry)
		}
		log.LogDebugf("recursiveMakeDirectory: lookup item: parentID(%v) inode(%v) name(%v) mode(%v)",
			ino, curIno, pathItem.Name, os.FileMode(curMode))
		// Check file mode
		if os.FileMode(curMode).IsDir() != pathItem.IsDirectory {
			err = syscall.EINVAL
			return
		}
		ino = curIno
	}
	return
}

// Deprecated
func (v *Volume) lookupDirectories(dirs []string, autoCreate bool) (inode uint64, err error) {
	var parentId = rootIno
	// check and create dirs
	for _, dir := range dirs {
		curIno, curMode, lookupErr := v.mw.Lookup_ll(parentId, dir)
		if lookupErr != nil && lookupErr != syscall.ENOENT {
			log.LogErrorf("lookupDirectories: meta lokkup fail, parentID(%v) name(%v) fail err(%v)", parentId, dir, lookupErr)
			return 0, lookupErr
		}
		if lookupErr == syscall.ENOENT && !autoCreate {
			return 0, syscall.ENOENT
		}
		// this item is not exist
		if lookupErr == syscall.ENOENT {
			var inodeInfo *proto.InodeInfo
			var createErr error
			inodeInfo, createErr = v.mw.Create_ll(parentId, dir, uint32(DefaultDirMode), 0, 0, nil)
			if createErr != nil && createErr != syscall.EEXIST {
				log.LogErrorf("lookupDirectories: meta create fail, parentID(%v) name(%v) mode(%v) err(%v)", parentId, dir, os.ModeDir, createErr)
				return 0, createErr
			}
			// retry lookup if it exists.
			if createErr == syscall.EEXIST {
				curIno, curMode, lookupErr = v.mw.Lookup_ll(parentId, dir)
				if lookupErr != nil {
					return 0, lookupErr
				}
				if !os.FileMode(curMode).IsDir() {
					return 0, syscall.EEXIST
				}
				parentId = curIno
				continue
			}
			if inodeInfo == nil {
				panic("illegal internal pointer found")
			}
			parentId = inodeInfo.Inode
			continue
		}

		// check mode
		if !os.FileMode(curMode).IsDir() {
			return 0, syscall.EEXIST
		}
		parentId = curIno
	}
	inode = parentId
	return
}

func (v *Volume) listFilesV1(prefix, marker, delimiter string, maxKeys uint64, onlyObject bool) (infos []*FSFileInfo,
	prefixes Prefixes, nextMarker string, err error) {
	var prefixMap = PrefixMap(make(map[string]struct{}))

	parentId, dirs, err := v.findParentId(prefix)

	// The method returns an ENOENT error, indicating that there
	// are no files or directories matching the prefix.
	if err == syscall.ENOENT {
		return nil, nil, "", nil
	}

	// Errors other than ENOENT are unexpected errors, method stops and returns it to the caller.
	if err != nil {
		log.LogErrorf("listFilesV1: find parent ID fail, prefix(%v) marker(%v) err(%v)", prefix, marker, err)
		return nil, nil, "", err
	}

	log.LogDebugf("listFilesV1: find parent ID, prefix(%v) marker(%v) delimiter(%v) parentId(%v) dirs(%v)",
		prefix, marker, delimiter, parentId, len(dirs))

	// Init the value that queried result count.
	// Check this value when adding key to contents or common prefix,
	// return if it reach to max keys
	var rc uint64
	// recursion scan
	infos, prefixMap, nextMarker, _, err = v.recursiveScan(infos, prefixMap, parentId, maxKeys, maxKeys, rc, dirs,
		prefix, marker, delimiter, onlyObject, true)
	if err != nil {
		log.LogErrorf("listFilesV1: volume list dir fail: Volume(%v) err(%v)", v.name, err)
		return
	}

	// Supplementary file information, such as file modification time, MIME type, Etag information, etc.
	if err = v.supplyListFileInfo(infos); err != nil {
		log.LogDebugf("listFilesV1: supply list file info fail, err(%v)", err)
		return
	}

	prefixes = prefixMap.Prefixes()

	log.LogDebugf("listFilesV1: Volume list dir: Volume(%v) prefix(%v) marker(%v) delimiter(%v) maxKeys(%v) infos(%v) prefixes(%v) nextMarker(%v)",
		v.name, prefix, marker, delimiter, maxKeys, len(infos), len(prefixes), nextMarker)

	return
}

func (v *Volume) listFilesV2(prefix, startAfter, contToken, delimiter string, maxKeys uint64) (infos []*FSFileInfo,
	prefixes Prefixes, nextMarker string, err error) {
	var prefixMap = PrefixMap(make(map[string]struct{}))

	var marker string
	if startAfter != "" {
		marker = startAfter
	}
	if contToken != "" {
		marker = contToken
	}
	parentId, dirs, err := v.findParentId(prefix)

	// The method returns an ENOENT error, indicating that there
	// are no files or directories matching the prefix.
	if err == syscall.ENOENT {
		return nil, nil, "", nil
	}

	// Errors other than ENOENT are unexpected errors, method stops and returns it to the caller.
	if err != nil {
		log.LogErrorf("listFilesV2: find parent ID fail, prefix(%v) marker(%v) err(%v)", prefix, marker, err)
		return nil, nil, "", err
	}

	log.LogDebugf("listFilesV2: find parent ID, prefix(%v) marker(%v) delimiter(%v) parentId(%v) dirs(%v)",
		prefix, marker, delimiter, parentId, len(dirs))

	// Init the value that queried result count.
	// Check this value when adding key to contents or common prefix,
	// return if it reach to max keys
	var rc uint64
	// recursion scan
	infos, prefixMap, nextMarker, _, err = v.recursiveScan(infos, prefixMap, parentId, maxKeys, maxKeys, rc, dirs,
		prefix, marker, delimiter, true, true)
	if err != nil {
		log.LogErrorf("listFilesV2: Volume list dir fail, Volume(%v) err(%v)", v.name, err)
		return
	}

	// Supplementary file information, such as file modification time, MIME type, Etag information, etc.
	err = v.supplyListFileInfo(infos)
	if err != nil {
		log.LogDebugf("listFilesV2: supply list file info fail, err(%v)", err)
		return
	}

	prefixes = prefixMap.Prefixes()

	log.LogDebugf("listFilesV2: Volume list dir: Volume(%v) prefix(%v) marker(%v) delimiter(%v) maxKeys(%v) infos(%v) prefixes(%v), nextMarker(%v)",
		v.name, prefix, marker, delimiter, maxKeys, len(infos), len(prefixes), nextMarker)

	return
}

func (v *Volume) findParentId(prefix string) (inode uint64, prefixDirs []string, err error) {
	prefixDirs = make([]string, 0)

	// if prefix and marker are both not empty, use marker
	var dirs []string
	if prefix != "" {
		dirs = strings.Split(prefix, "/")
	}
	if len(dirs) <= 1 {
		return proto.RootIno, prefixDirs, nil
	}

	var parentId = proto.RootIno
	for index, dir := range dirs {

		// Because lookup can only retrieve dentry whose name exactly matches,
		// so do not lookup the last part.
		if index+1 == len(dirs) {
			break
		}

		curIno, curMode, err := v.mw.Lookup_ll(parentId, dir)

		// If the part except the last part does not match exactly the same dentry, there is
		// no path matching the path prefix. An ENOENT error is returned to the caller.
		if err == syscall.ENOENT {
			return 0, nil, syscall.ENOENT
		}

		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("findParentId: find directories fail: prefix(%v) err(%v)", prefix, err)
			return 0, nil, err
		}

		// Because the file cannot have the next level members,
		// if there is a directory in the middle of the prefix,
		// it means that there is no file matching the prefix.
		if !os.FileMode(curMode).IsDir() {
			return 0, nil, syscall.ENOENT
		}

		prefixDirs = append(prefixDirs, dir)
		parentId = curIno
	}
	inode = parentId
	return
}

// Recursive scan of the directory starting from the given parentID. Match files and directories
// that match the prefix and delimiter criteria. Stop when the number of matches reaches a threshold
// or all files and directories are scanned.
func (v *Volume) recursiveScan(fileInfos []*FSFileInfo, prefixMap PrefixMap, parentId, maxKeys, readLimit, rc uint64, dirs []string,
	prefix, marker, delimiter string, onlyObject, firstEnter bool) ([]*FSFileInfo, PrefixMap, string, uint64, error) {
	var err error
	var nextMarker string
	var lastKey string

	var currentPath = strings.Join(dirs, pathSep) + pathSep
	if strings.HasPrefix(currentPath, pathSep) {
		currentPath = strings.TrimPrefix(currentPath, pathSep)
	}
	log.LogDebugf("recursiveScan enter: currentPath(/%v) fileInfos(%v) parentId(%v) prefix(%v) marker(%v) rc(%v)",
		currentPath, fileInfos, parentId, prefix, marker, rc)
	defer func() {
		log.LogDebugf("recursiveScan exit: currentPath(/%v) fileInfos(%v) parentId(%v)  prefix(%v) nextMarker(%v) rc(%v)",
			currentPath, fileInfos, parentId, prefix, nextMarker, rc)
	}()

	// The "prefix" needs to be extracted as marker when it is larger than "marker".
	// So extract prefixMarker in this layer.
	prefixMarker := ""
	if prefix != "" {
		if len(dirs) == 0 {
			prefixMarker = prefix
		} else if strings.HasPrefix(prefix, currentPath) {
			prefixMarker = strings.TrimPrefix(prefix, currentPath)
		}
	}

	// To be sent in the readdirlimit request as a search start point.
	fromName := ""
	// Marker in this layer, shall be compared with prefixMarker to
	// determine which one should be used as the search start point.
	currentMarker := ""
	if marker != "" {
		markerNames := strings.Split(marker, pathSep)
		if len(markerNames) > len(dirs) {
			currentMarker = markerNames[len(dirs)]
		}
		if prefixMarker > currentMarker {
			fromName = prefixMarker
		} else {
			fromName = currentMarker
		}
	} else if prefixMarker != "" {
		fromName = prefixMarker
	}

	// During the process of scanning the child nodes of the current directory, there may be other
	// parallel operations that may delete the current directory.
	// If got the syscall.ENOENT error when invoke readdir, it means that the above situation has occurred.
	// At this time, stops process and returns success.
	var children []proto.Dentry

readDir:
	children, err = v.mw.ReadDirLimit_ll(parentId, fromName, readLimit+1) // one more for nextMarker
	if err != nil && err != syscall.ENOENT {
		return fileInfos, prefixMap, "", 0, err
	}
	if err == syscall.ENOENT {
		return fileInfos, prefixMap, "", 0, nil
	}

	log.LogDebugf("recursiveScan read: currentPath(%v) parentId(%v) fromName(%v) maxKey(%v) readLimit(%v) children(%v)",
		currentPath, parentId, fromName, maxKeys, readLimit, children)

	for _, child := range children {
		if child.Name == lastKey {
			continue
		}
		var path = strings.Join(append(dirs, child.Name), pathSep)
		if os.FileMode(child.Type).IsDir() {
			path += pathSep
		}
		if prefix != "" && !strings.HasPrefix(path, prefix) {
			continue
		}

		if marker != "" {
			if !os.FileMode(child.Type).IsDir() && path < marker {
				continue
			}
			if os.FileMode(child.Type).IsDir() && strings.HasPrefix(marker, path) {
				fileInfos, prefixMap, nextMarker, rc, err = v.recursiveScan(fileInfos, prefixMap, child.Inode, maxKeys,
					readLimit, rc, append(dirs, child.Name), prefix, marker, delimiter, onlyObject, false)
				if err != nil {
					return fileInfos, prefixMap, nextMarker, rc, err
				}
				if rc >= maxKeys && nextMarker != "" {
					return fileInfos, prefixMap, nextMarker, rc, err
				}
				continue
			}
		}

		if delimiter != "" {
			var nonPrefixPart = strings.Replace(path, prefix, "", 1)
			if idx := strings.Index(nonPrefixPart, delimiter); idx >= 0 {
				var commonPrefix = prefix + util.SubString(nonPrefixPart, 0, idx) + delimiter
				if prefixMap.contain(commonPrefix) {
					continue
				}
				if rc >= maxKeys {
					return fileInfos, prefixMap, commonPrefix, rc, nil
				}
				prefixMap.AddPrefix(commonPrefix)
				rc++
				continue
			}
		}

		if onlyObject && os.FileMode(child.Type).IsRegular() || !onlyObject {
			if rc >= maxKeys {
				nextMarker = path
				return fileInfos, prefixMap, nextMarker, rc, nil
			}

			fileInfo := &FSFileInfo{
				Inode: child.Inode,
				Path:  path,
			}
			if marker == "" || marker != "" && fileInfo.Path != marker {
				fileInfos = append(fileInfos, fileInfo)
				rc++
			}
		}

		if os.FileMode(child.Type).IsDir() {
			nextMarker = fmt.Sprintf("%v%v%v", currentPath, child.Name, pathSep)
			fileInfos, prefixMap, nextMarker, rc, err = v.recursiveScan(fileInfos, prefixMap, child.Inode, maxKeys,
				readLimit, rc, append(dirs, child.Name), prefix, nextMarker, delimiter, onlyObject, false)
			if err != nil {
				return fileInfos, prefixMap, nextMarker, rc, err
			}
			if rc >= maxKeys && nextMarker != "" {
				return fileInfos, prefixMap, nextMarker, rc, err
			}
		}
	}

	if firstEnter && len(children) > 1 && rc <= maxKeys {
		lastKey = children[len(children)-1].Name
		if strings.HasPrefix(strings.Join(append(dirs, lastKey), pathSep), prefix) {
			fromName = lastKey
			readLimit = maxKeys - rc + 1
			log.LogDebugf("recursiveScan continue: currentPath(%v) parentId(%v) prefix(%v) marker(%v) lastKey(%v) rc(%v)",
				currentPath, parentId, prefix, marker, lastKey, rc)
			goto readDir
		}
	}

	return fileInfos, prefixMap, nextMarker, rc, nil
}

// This method is used to supplement file metadata. Supplement the specified file
// information with Size, ModifyTIme, Mode, Etag, and MIME type information.
func (v *Volume) supplyListFileInfo(fileInfos []*FSFileInfo) (err error) {
	var inodes []uint64
	for _, fileInfo := range fileInfos {
		inodes = append(inodes, fileInfo.Inode)
	}

	// Get size information in batches, then update to fileInfos
	inodeInfos := v.mw.BatchInodeGet(inodes)
	sort.SliceStable(inodeInfos, func(i, j int) bool {
		return inodeInfos[i].Inode < inodeInfos[j].Inode
	})
	for _, fileInfo := range fileInfos {
		i := sort.Search(len(inodeInfos), func(i int) bool {
			return inodeInfos[i].Inode >= fileInfo.Inode
		})
		if i >= 0 && i < len(inodeInfos) && inodeInfos[i].Inode == fileInfo.Inode {
			fileInfo.Size = int64(inodeInfos[i].Size)
			fileInfo.ModifyTime = inodeInfos[i].ModifyTime
			fileInfo.CreateTime = inodeInfos[i].CreateTime
			fileInfo.Mode = os.FileMode(inodeInfos[i].Mode)
		}
	}

	// Get MD5 information in batches, then update to fileInfos
	keys := []string{XAttrKeyOSSETag, XAttrKeyOSSETagDeprecated}
	xattrs, err := v.mw.BatchGetXAttr(inodes, keys)
	if err != nil {
		log.LogErrorf("supplyListFileInfo: batch get xattr fail, inodes(%v), err(%v)", inodes, err)
		return
	}
	sort.SliceStable(xattrs, func(i, j int) bool {
		return xattrs[i].Inode < xattrs[j].Inode
	})
	for _, fileInfo := range fileInfos {
		if fileInfo.Mode.IsDir() {
			fileInfo.ETag = DirectoryETagValue().ETag()
			continue
		}
		i := sort.Search(len(xattrs), func(i int) bool {
			return xattrs[i].Inode >= fileInfo.Inode
		})
		var etagValue ETagValue
		if i >= 0 && i < len(xattrs) && xattrs[i].Inode == fileInfo.Inode {
			var xattr = xattrs[i]
			var rawETag = string(xattr.Get(XAttrKeyOSSETag))
			if len(rawETag) == 0 {
				rawETag = string(xattr.Get(XAttrKeyOSSETagDeprecated))
			}
			if len(rawETag) > 0 {
				etagValue = ParseETagValue(rawETag)
			}
		}
		if !etagValue.Valid() || etagValue.TS.Before(fileInfo.ModifyTime) {
			// The ETag is invalid or outdated then generate a new ETag and make update.
			if etagValue, err = v.updateETag(fileInfo.Inode, fileInfo.Size, fileInfo.ModifyTime); err != nil {
				log.LogErrorf("supplyListFileInfo: update ETag fail: volume(%v) path(%v) inode(%v) err(%v)",
					v.name, fileInfo.Path, fileInfo.Inode, err)
			}
			log.LogDebugf("supplyListFileInfo: update ETag: volume(%v) path(%v) inode(%v) etagValue(%v)",
				v.name, fileInfo.Path, fileInfo.Inode, etagValue)
		}
		fileInfo.ETag = etagValue.ETag()
	}
	return
}

func (v *Volume) updateETag(inode uint64, size int64, mt time.Time) (etagValue ETagValue, err error) {
	// The ETag is invalid or outdated then generate a new ETag and make update.
	if size == 0 {
		etagValue = EmptyContentETagValue(mt)
	} else {
		var splittedRanges = SplitFileRange(size, SplitFileRangeBlockSize)
		etagValue = NewRandomUUIDETagValue(len(splittedRanges), mt)
	}
	if err = v.mw.XAttrSet_ll(inode, []byte(XAttrKeyOSSETag), []byte(etagValue.Encode())); err != nil {
		return
	}
	return
}

func (v *Volume) ListMultipartUploads(prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) (
	uploads []*FSUpload, nextMarker, nextMultipartIdMarker string, isTruncated bool, prefixes []string, err error) {
	sessions, err := v.mw.ListMultipart_ll(prefix, delimiter, keyMarker, multipartIdMarker, maxUploads)
	if err != nil || len(sessions) == 0 {
		return
	}

	uploads = make([]*FSUpload, 0)
	prefixes = make([]string, 0)
	prefixMap := make(map[string]interface{})

	var count uint64
	var lastUpload *proto.MultipartInfo
	for _, session := range sessions {
		var tempKey = session.Path
		if len(prefix) > 0 {
			if !strings.HasPrefix(tempKey, prefix) {
				continue
			}
			pIndex := strings.Index(tempKey, prefix)
			tempKeyRunes := []rune(tempKey)
			tempKey = string(tempKeyRunes[pIndex+len(prefix):])
		}
		if len(keyMarker) > 0 {
			if session.Path < keyMarker {
				continue
			}
			if session.Path == keyMarker && multipartIdMarker == "" {
				continue
			}
			if session.Path == keyMarker && multipartIdMarker != "" && session.ID <= multipartIdMarker {
				continue
			}
		}
		if count >= maxUploads {
			isTruncated = true
			break
		}
		count++
		lastUpload = session
		if len(delimiter) > 0 && strings.Contains(tempKey, delimiter) {
			dIndex := strings.Index(tempKey, delimiter)
			tempKeyRunes := []rune(tempKey)
			commonPrefix := string(tempKeyRunes[:dIndex+len(delimiter)])
			if len(prefix) > 0 {
				commonPrefix = prefix + commonPrefix
			}
			if _, ok := prefixMap[commonPrefix]; !ok {
				prefixMap[commonPrefix] = nil
			}
		} else {
			fsUpload := &FSUpload{
				Key:          session.Path,
				UploadId:     session.ID,
				Initiated:    formatTimeISO(session.InitTime),
				StorageClass: StorageClassStandard,
			}
			uploads = append(uploads, fsUpload)
		}
	}
	if isTruncated && lastUpload != nil {
		nextMarker = lastUpload.Path
		nextMultipartIdMarker = lastUpload.ID
	}
	for pref := range prefixMap {
		prefixes = append(prefixes, pref)
	}
	sort.SliceStable(prefixes, func(i, j int) bool {
		return prefixes[i] < prefixes[j]
	})

	return
}

func (v *Volume) ListParts(path, uploadId string, maxParts, partNumberMarker uint64) (parts []*FSPart, nextMarker uint64, isTruncated bool, err error) {
	multipartInfo, err := v.mw.GetMultipart_ll(path, uploadId)
	if err != nil {
		log.LogErrorf("ListPart: get multipart upload fail: path(%v) volume(%v) uploadID(%v) err(%v)", path, v.name, uploadId, err)
		return
	}

	sessionParts := multipartInfo.Parts
	resLength := maxParts
	isTruncated = true
	if (uint64(len(sessionParts)) - partNumberMarker) < maxParts {
		resLength = uint64(len(sessionParts)) - partNumberMarker
		isTruncated = false
		nextMarker = 0
	} else {
		nextMarker = partNumberMarker + resLength
	}

	sessionPartsTemp := sessionParts[partNumberMarker:resLength]
	for _, sessionPart := range sessionPartsTemp {
		fsPart := &FSPart{
			PartNumber:   int(sessionPart.ID),
			LastModified: formatTimeISO(sessionPart.UploadTime),
			ETag:         sessionPart.MD5,
			Size:         int(sessionPart.Size),
		}
		parts = append(parts, fsPart)
	}

	return parts, nextMarker, isTruncated, nil
}

func (v *Volume) CopyFile(sv *Volume, sourcePath, targetPath, metaDirective string, opt *PutFileOption) (info *FSFileInfo, err error) {
	defer func() {
		log.LogInfof("Audit: copy file: source path(%v) target path(%v) err(%v)",
			sourcePath, targetPath, err)
	}()

	// operation at source object
	var (
		sInode     uint64
		sMode      os.FileMode
		sInodeInfo *proto.InodeInfo
	)

	if _, sInode, _, sMode, err = sv.recursiveLookupTarget(sourcePath); err != nil {
		log.LogErrorf("CopyFile: look up source path fail, source path(%v) err(%v)", sourcePath, err)
		return
	}
	if sInodeInfo, err = sv.mw.InodeGet_ll(sInode); err != nil {
		log.LogErrorf("CopyFile: get source path inode info fail, source path(%v) err(%v)", sourcePath, err)
		return
	}
	if sInodeInfo.Size > MaxCopyObjectSize {
		log.LogErrorf("CopyFile: copy source path file size greater than 5GB, source path(%v), target path(%v)", sourcePath, targetPath)
		return nil, syscall.EFBIG
	}
	if err = sv.ec.OpenStream(sInode); err != nil {
		log.LogErrorf("CopyFile: open source path stream fail, source path(%v) source path inode(%v) err(%v)",
			sourcePath, sInode, err)
		return
	}
	defer func() {
		if closeErr := sv.ec.CloseStream(sInode); closeErr != nil {
			log.LogErrorf("CopyFile: close source path stream fail: source path(%v) source path inode(%v) err(%v)",
				sourcePath, sInode, closeErr)
		}
	}()

	var xattr *proto.XAttrInfo
	// if source path is same with target path, just reset file metadata
	// source path is same with target path, and metadata directive is not 'REPLACE', object node do nothing
	if targetPath == sourcePath && v.name == sv.name {
		if metaDirective != MetadataDirectiveReplace {
			log.LogInfof("CopyFile: target path is equal with source path, object node do nothing, source path(%v) target path(%v) err(%v)",
				sourcePath, targetPath, err)
		} else {
			// replace system metadata : 'Content-Type' and 'Content-Disposition', if user specified,
			// replace user defined metadata
			// If MIME information is valid, use extended attributes for storage.

			attr := &AttrItem{
				XAttrInfo: proto.XAttrInfo{
					Inode:  sInode,
					XAttrs: make(map[string]string),
				},
			}

			if opt != nil && opt.MIMEType != "" {
				attr.XAttrs[XAttrKeyOSSMIME] = opt.MIMEType
			}
			if opt != nil && opt.Disposition != "" {
				attr.XAttrs[XAttrKeyOSSDISPOSITION] = opt.Disposition
			}
			if opt != nil && opt.CacheControl != "" {
				attr.XAttrs[XAttrKeyOSSCacheControl] = opt.CacheControl
			}
			if opt != nil && opt.Expires != "" {
				attr.XAttrs[XAttrKeyOSSExpires] = opt.Expires
			}
			if opt != nil && opt.ACL != nil {
				attr.XAttrs[XAttrKeyOSSACL] = opt.ACL.XmlEncode()
			}

			// If user-defined metadata have been specified, use extend attributes for storage.
			if opt != nil && len(opt.Metadata) > 0 {
				for name, value := range opt.Metadata {
					attr.XAttrs[name] = value
					log.LogDebugf("PutObject: store user-defined metadata: "+
						"volume(%v) path(%v) inode(%v) key(%v) value(%v)",
						sv.name, sourcePath, sInode, name, value)
				}
			}

			if err = v.mw.BatchSetXAttr_ll(sInode, attr.XAttrs); err != nil {
				log.LogErrorf("CopyFile: BatchSetXAttr_ll fail: volume(%v) source path(%v) inode(%v) attrs(%v) err(%v)",
					sv.name, sourcePath, sInode, attr.XAttrs, err)
				return nil, err
			}
			// merge attrs in cache
			if objMetaCache != nil {
				objMetaCache.MergeAttr(v.name, attr)
			}
			log.LogInfof("CopyFile: target path is equal with source path, replace metadata, source path(%v) target path(%v) opt(%v)",
				sourcePath, targetPath, opt)
		}
		info, xattr, err = sv.ObjectMeta(sourcePath)
		return info, err
	}

	// operation at target object
	var (
		tMode      os.FileMode
		tInode     uint64
		tInodeInfo *proto.InodeInfo
		tParentId  uint64
		pathItems  []PathItem
		tLastName  string
	)
	if _, _, _, tMode, err = v.recursiveLookupTarget(targetPath); err != nil && err != syscall.ENOENT {
		log.LogErrorf("CopyFile: look up target path failed, target path(%v), err(%v)", targetPath, err)
		return
	}
	// if target file existed, check target file node is whether same with source file
	if err != syscall.ENOENT && tMode.IsDir() != sMode.IsDir() {
		log.LogErrorf("CopyFile: target path existed and target path mode not same with source path, "+
			"target path(%v), target inode(%v), source path(%v), source inode(%v)", targetPath, tInode, sourcePath, sInode)
		return nil, syscall.EINVAL
	}
	// if source file mode is directory, return OK, and need't create target directory
	if sMode == DefaultDirMode {
		// create target directory
		if !strings.HasSuffix(targetPath, pathSep) {
			targetPath += pathSep
		}
		if tParentId, err = v.recursiveMakeDirectory(targetPath); err != nil {
			log.LogErrorf("CopyFile: recursive make directory of target path fail: volume(%v) target path(%v) err(%v)",
				v.name, targetPath, err)
			return
		}
		if tInodeInfo, err = v.mw.InodeGet_ll(tParentId); err != nil {
			log.LogErrorf("CopyFile: get create directory of target path inode info fail: volume(%v) target path(%v) err(%v)",
				v.name, targetPath, err)
			return
		}

		info = &FSFileInfo{
			Path:       targetPath,
			Size:       0,
			Mode:       DefaultDirMode,
			ModifyTime: tInodeInfo.ModifyTime,
			CreateTime: tInodeInfo.CreateTime,
			ETag:       EmptyContentMD5String,
			Inode:      tInodeInfo.Inode,
			MIMEType:   HeaderValueContentTypeDirectory,
		}
		return info, nil
	}
	// recursive create target directory, and get parent id and last name
	if tParentId, err = v.recursiveMakeDirectory(targetPath); err != nil {
		log.LogErrorf("CopyFile: recursive make target path directory fail: volume(%v) path(%v) err(%v)",
			v.name, targetPath, err)
		return
	}
	pathItems = NewPathIterator(targetPath).ToSlice()
	if len(pathItems) <= 0 {
		log.LogErrorf("CopyFile: get target path pathItems is empty: volume(%v) path(%v) err(%v)",
			v.name, targetPath, err)
		return nil, syscall.EINVAL
	}
	tLastName = pathItems[len(pathItems)-1].Name

	// create target file inode and set target inode to be source file inode
	if tInodeInfo, err = v.mw.InodeCreate_ll(uint32(sMode), 0, 0, nil, make([]uint64, 0)); err != nil {
		return
	}
	defer func() {
		// An error has caused the entire process to fail. Delete the inode and release the written data.
		if err != nil {
			log.LogWarnf("CopyFile: unlink target temp inode: volume(%v) path(%v) inode(%v) ",
				v.name, targetPath, tInodeInfo.Inode)
			_, _ = v.mw.InodeUnlink_ll(tInodeInfo.Inode)
			log.LogWarnf("CopyFile: evict target temp inode: volume(%v) path(%v) inode(%v)",
				v.name, targetPath, tInodeInfo.Inode)
			_ = v.mw.Evict(tInodeInfo.Inode)
		}
	}()
	if err = v.ec.OpenStream(tInodeInfo.Inode); err != nil {
		return
	}
	defer func() {
		if closeErr := v.ec.CloseStream(tInodeInfo.Inode); closeErr != nil {
			log.LogErrorf("CopyFile: close target path stream fail: volume(%v) path(%v) inode(%v) err(%v)",
				v.name, targetPath, tInodeInfo.Inode, closeErr)
		}
	}()

	// write data to invisibleTempDataInode from source object
	var (
		fileSize    = sInodeInfo.Size
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

	var sctx context.Context
	var ebsReader *blobstore.Reader
	var tctx context.Context
	var ebsWriter *blobstore.Writer
	if proto.IsCold(sv.volType) {
		sctx = context.Background()
		ebsReader = v.getEbsReader(sInode)
	}
	if proto.IsCold(v.volType) {
		tctx = context.Background()
		ebsWriter = v.getEbsWriter(tInodeInfo.Inode)
	}

	for {
		if rest = int(fileSize) - readOffset; rest <= 0 {
			break
		}
		readSize = len(buf)
		if rest < len(buf) {
			readSize = rest
		}
		buf = buf[:readSize]
		if proto.IsCold(sv.volType) {
			readN, err = ebsReader.Read(sctx, buf, readOffset, readSize)
		} else {
			readN, err = sv.ec.Read(sInode, buf, readOffset, readSize)
		}
		if err != nil && err != io.EOF {
			return
		}

		if readN > 0 {
			if proto.IsCold(v.volType) {
				writeN, err = ebsWriter.WriteWithoutPool(tctx, writeOffset, buf[:readN])
			} else {
				writeN, err = v.ec.Write(tInodeInfo.Inode, writeOffset, buf[:readN], 0)
			}
			if err != nil {
				log.LogErrorf("CopyFile: write target path from source fail, volume(%v) path(%v) inode(%v) target offset(%v) err(%v)",
					v.name, targetPath, tInodeInfo.Inode, writeOffset, err)
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
	// flush
	if proto.IsCold(v.volType) {
		err = ebsWriter.FlushWithoutPool(tInodeInfo.Inode, tctx)
	} else {
		v.ec.Flush(tInodeInfo.Inode)
	}
	if err != nil {
		log.LogErrorf("CopyFile: data flush inode fail, volume(%v) inode(%v), path (%v) err(%v)", v.name, tInodeInfo.Inode, targetPath, err)
		return
	}

	md5Value = hex.EncodeToString(md5Hash.Sum(nil))
	log.LogDebugf("Audit: copy file: write file finished, volume(%v), path(%v), etag(%v)", v.name, targetPath, md5Value)

	var finalInode *proto.InodeInfo
	if finalInode, err = v.mw.InodeGet_ll(tInodeInfo.Inode); err != nil {
		log.LogErrorf("CopyFile: get finished target path final inode fail: volume(%v) path(%v) inode(%v) err(%v)",
			v.name, targetPath, tInodeInfo.Inode, err)
		return
	}
	var etagValue = ETagValue{
		Value:   md5Value,
		PartNum: 0,
		TS:      finalInode.ModifyTime,
	}

	targetAttr := &AttrItem{
		XAttrInfo: proto.XAttrInfo{
			Inode:  tInodeInfo.Inode,
			XAttrs: make(map[string]string),
		},
	}
	targetAttr.XAttrs[XAttrKeyOSSETag] = etagValue.Encode()

	// copy source file metadata to write target file metadata
	if metaDirective != MetadataDirectiveReplace {

		xattr, err = sv.mw.XAttrGetAll_ll(sInode)
		if xattr == nil || err != nil {
			return
		}

		for key, val := range xattr.XAttrs {
			if key == XAttrKeyOSSETag {
				continue
			}
			targetAttr.XAttrs[key] = val
		}
		if opt != nil && opt.ACL != nil {
			targetAttr.XAttrs[XAttrKeyOSSACL] = opt.ACL.XmlEncode()
		}
		if err = v.mw.BatchSetXAttr_ll(tInodeInfo.Inode, targetAttr.XAttrs); err != nil {
			log.LogErrorf("CopyFile: set target xattr fail: volume(%v) target path(%v) inode(%v) xattr (%v)err(%v)",
				v.name, targetPath, tInodeInfo.Inode, xattr, err)
			return
		}
		// merge attrs in cache
		if objMetaCache != nil {
			objMetaCache.PutAttr(v.name, targetAttr)
		}
	} else {
		log.LogDebugf("debug_CopyFile replace dst meta")
		if opt != nil && opt.MIMEType != "" {
			targetAttr.XAttrs[XAttrKeyOSSMIME] = opt.MIMEType
		}
		if opt != nil && opt.Disposition != "" {
			targetAttr.XAttrs[XAttrKeyOSSDISPOSITION] = opt.Disposition
		}
		if opt != nil && opt.CacheControl != "" {
			targetAttr.XAttrs[XAttrKeyOSSCacheControl] = opt.CacheControl
		}
		if opt != nil && opt.Expires != "" {
			targetAttr.XAttrs[XAttrKeyOSSExpires] = opt.Expires
		}
		if opt != nil && opt.ACL != nil {
			targetAttr.XAttrs[XAttrKeyOSSACL] = opt.ACL.XmlEncode()
		}

		// If user-defined metadata have been specified, use extend attributes for storage.
		if opt != nil && len(opt.Metadata) > 0 {
			for name, value := range opt.Metadata {
				targetAttr.XAttrs[name] = value
				log.LogDebugf("CopyFile: store user-defined metadata: "+
					"volume(%v) path(%v) inode(%v) key(%v) value(%v)",
					v.name, targetPath, tInodeInfo.Inode, name, value)
			}
		}

		if err = v.mw.BatchSetXAttr_ll(tInodeInfo.Inode, targetAttr.XAttrs); err != nil {
			log.LogErrorf("CopyFile: BatchSetXAttr_ll fail: volume(%v) target path(%v) inode(%v) attrs(%v) err(%v)",
				v.name, targetPath, tInodeInfo.Inode, targetAttr.XAttrs, err)
			return nil, err
		}
		// merge attrs in cache
		if objMetaCache != nil {
			objMetaCache.PutAttr(v.name, targetAttr)
		}
	}

	// create file info
	info = &FSFileInfo{
		Path:       targetPath,
		Size:       int64(fileSize),
		Mode:       sMode,
		ModifyTime: tInodeInfo.ModifyTime,
		CreateTime: tInodeInfo.CreateTime,
		ETag:       md5Value,
		Inode:      tInodeInfo.Inode,
	}

	// apply new inode to dentry
	err = v.applyInodeToDEntry(tParentId, tLastName, tInodeInfo.Inode)
	if err != nil {
		log.LogErrorf("CopyFile: apply inode to new dentry fail: path(%v) parentID(%v) name(%v) inode(%v) err(%v)",
			targetPath, tParentId, tLastName, tInodeInfo.Inode, err)
	}

	// force updating dentry and attrs in cache
	if objMetaCache != nil {
		dentry := &DentryItem{
			Dentry: metanode.Dentry{
				ParentId: tParentId,
				Name:     tLastName,
				Inode:    tInodeInfo.Inode,
				Type:     DefaultFileMode,
			},
		}
		objMetaCache.PutDentry(v.name, dentry)
	}

	return
}

func (v *Volume) copyFile(parentID uint64, newFileName string, sourceFileInode uint64, mode uint32) (info *proto.InodeInfo, err error) {

	if err = v.mw.DentryCreate_ll(parentID, newFileName, sourceFileInode, mode); err != nil {
		return
	}
	if info, err = v.mw.InodeLink_ll(sourceFileInode); err != nil {
		return
	}
	return
}

func NewVolume(config *VolumeConfig) (*Volume, error) {
	var err error
	var metaConfig = &meta.MetaConfig{
		Volume:        config.Volume,
		Masters:       config.Masters,
		Authenticate:  false,
		ValidateOwner: false,
		OnAsyncTaskError: func(err error) {
			config.OnAsyncTaskError.OnError(err)
		},
	}

	var metaWrapper *meta.MetaWrapper
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		log.LogErrorf("NewVolume: new meta wrapper failed: volume(%v) err(%v)", metaConfig.Volume, err)
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = metaWrapper.Close()
		}
	}()

	mc := master.NewMasterClient(config.Masters, false)
	var volumeInfo *proto.SimpleVolView
	volumeInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(config.Volume)
	if err != nil {
		log.LogErrorf("NewVolume: get volume info from master failed: volume(%v) err(%v)", config.Volume, err)
		return nil, err
	}
	if volumeInfo.Status == 1 {
		log.LogWarnf("NewVolume: volume has been marked for deletion: volume(%v) status(%v - 0:normal/1:markDelete)",
			config.Volume, volumeInfo.Status)
		return nil, proto.ErrVolNotExists
	}

	var extentConfig = &stream.ExtentConfig{
		Volume:            config.Volume,
		Masters:           config.Masters,
		FollowerRead:      true,
		OnAppendExtentKey: metaWrapper.AppendExtentKey,
		OnGetExtents:      metaWrapper.GetExtents,
		OnTruncate:        metaWrapper.Truncate,
	}
	if proto.IsCold(volumeInfo.VolType) {
		if blockCache != nil {
			extentConfig.BcacheEnable = true
			extentConfig.OnLoadBcache = blockCache.Get
			extentConfig.OnCacheBcache = blockCache.Put
			extentConfig.OnEvictBcache = blockCache.Evict
		}
		log.LogDebugf("%v is cold volume", config.Volume)
	}
	var extentClient *stream.ExtentClient
	if extentClient, err = stream.NewExtentClient(extentConfig); err != nil {
		log.LogErrorf("NewVolume: new extent client failed: volume(%v) err(%v)", metaConfig.Volume, err)
		return nil, err
	}

	v := &Volume{
		mw:             metaWrapper,
		ec:             extentClient,
		name:           config.Volume,
		owner:          volumeInfo.Owner,
		store:          config.Store,
		createTime:     metaWrapper.VolCreateTime(),
		volType:        volumeInfo.VolType,
		ebsBlockSize:   volumeInfo.ObjBlockSize,
		cacheAction:    volumeInfo.CacheAction,
		cacheThreshold: volumeInfo.CacheThreshold,
		closeCh:        make(chan struct{}),
		onAsyncTaskError: func(err error) {
			if err == syscall.ENOENT {
				config.OnAsyncTaskError.OnError(proto.ErrVolNotExists)
			}
		},
	}
	if config.MetaStrict {
		v.metaLoader = &strictMetaLoader{v: v}
	} else {
		v.metaLoader = &cacheMetaLoader{om: new(OSSMeta)}
		go v.syncOSSMeta()
	}

	return v, nil
}

func (v *Volume) getEbsWriter(ino uint64) (writer *blobstore.Writer) {
	clientConf := blobstore.ClientConfig{
		VolName:         v.name,
		VolType:         v.volType,
		Ino:             ino,
		BlockSize:       v.ebsBlockSize,
		Bc:              blockCache,
		Mw:              v.mw,
		Ec:              v.ec,
		Ebsc:            ebsClient,
		EnableBcache:    enableBlockcache,
		WConcurrency:    writeThreads,
		ReadConcurrency: readThreads,
		CacheAction:     v.cacheAction,
		FileCache:       false,
		FileSize:        0,
		CacheThreshold:  v.cacheThreshold,
	}

	writer = blobstore.NewWriter(clientConf)
	log.LogDebugf("getEbsWriter: writer(%v) ", writer)
	return
}

func (v *Volume) getEbsReader(ino uint64) (reader *blobstore.Reader) {
	clientConf := blobstore.ClientConfig{
		VolName:         v.name,
		VolType:         v.volType,
		Ino:             ino,
		BlockSize:       v.ebsBlockSize,
		Bc:              blockCache,
		Mw:              v.mw,
		Ec:              v.ec,
		Ebsc:            ebsClient,
		EnableBcache:    enableBlockcache,
		WConcurrency:    writeThreads,
		ReadConcurrency: readThreads,
		CacheAction:     v.cacheAction,
		FileCache:       false,
		FileSize:        0,
		CacheThreshold:  v.cacheThreshold,
	}

	reader = blobstore.NewReader(clientConf)
	log.LogDebugf("getEbsReader: reader(%v) ", reader)
	return
}
