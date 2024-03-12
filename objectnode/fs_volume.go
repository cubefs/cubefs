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
	"reflect"
	"sort"
	"strconv"
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
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/exporter"
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
	ObjectLock   *ObjectLockConfig
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
	v.loadOSSMeta()

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
	ctx := context.Background()
	span := spanWithOperation(ctx, "LoadOSSMeta")

	var err error
	defer func() {
		if err != nil {
			v.onAsyncTaskError.OnError(err)
		}
	}()

	var policy *Policy
	if policy, err = v.loadBucketPolicy(ctx); err != nil {
		span.Errorf("load policy fail: volume(%v) err(%v)", v.name, err)
		return
	}
	v.metaLoader.storePolicy(policy)

	var acl *AccessControlPolicy
	if acl, err = v.loadBucketACL(ctx); err != nil {
		span.Errorf("load acl fail: volume(%v) err(%v)", v.name, err)
		return
	}
	v.metaLoader.storeACL(acl)

	var cors *CORSConfiguration
	if cors, err = v.loadBucketCors(ctx); err != nil {
		span.Errorf("load cors fail: volume(%v) err(%v)", v.name, err)
		return
	}
	v.metaLoader.storeCORS(cors)

	var objectlock *ObjectLockConfig
	if objectlock, err = v.loadObjectLock(ctx); err != nil {
		span.Errorf("load objectLock fail: volume(%v) err(%v)", v.name, err)
		return
	}
	v.metaLoader.storeObjectLock(objectlock)

	v.metaLoader.setSynced()
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
func (v *Volume) loadBucketPolicy(ctx context.Context) (policy *Policy, err error) {
	var data []byte
	data, err = v.store.Get(ctx, v.name, bucketRootPath, XAttrKeyOSSPolicy)
	if err != nil {
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

func (v *Volume) loadBucketACL(ctx context.Context) (acp *AccessControlPolicy, err error) {
	var raw []byte
	if raw, err = v.store.Get(ctx, v.name, bucketRootPath, XAttrKeyOSSACL); err != nil {
		return
	}
	if len(raw) == 0 {
		return
	}
	acp = &AccessControlPolicy{}
	if err = json.Unmarshal(raw, acp); err != nil {
		err = xml.Unmarshal(raw, acp)
		return
	}
	return
}

func (v *Volume) loadBucketCors(ctx context.Context) (configuration *CORSConfiguration, err error) {
	var raw []byte
	if raw, err = v.store.Get(ctx, v.name, bucketRootPath, XAttrKeyOSSCORS); err != nil {
		return
	}
	if len(raw) == 0 {
		return
	}
	configuration = &CORSConfiguration{}
	if err = xml.Unmarshal(raw, configuration); err != nil {
		return
	}
	return
}

func (v *Volume) loadObjectLock(ctx context.Context) (configuration *ObjectLockConfig, err error) {
	var raw []byte
	if raw, err = v.store.Get(ctx, v.name, bucketRootPath, XAttrKeyOSSLock); err != nil {
		return
	}
	if len(raw) == 0 {
		return
	}
	configuration = &ObjectLockConfig{}
	if err = json.Unmarshal(raw, configuration); err != nil {
		return
	}
	return
}

func (v *Volume) getInodeFromPath(ctx context.Context, path string) (inode uint64, err error) {
	if path == "/" {
		inode = volumeRootInode
		return
	}

	dirs, filename := splitPath(path)
	if len(dirs) == 0 && filename == "" {
		inode = volumeRootInode
	} else {
		// process path
		var parentId uint64
		if parentId, err = v.lookupDirectories(ctx, dirs, false); err != nil {
			return
		}
		// check file mode
		var lookupMode uint32
		inode, lookupMode, err = v.mw.Lookup_ll(parentId, filename)
		if err != nil {
			return
		}
		if os.FileMode(lookupMode).IsDir() {
			err = syscall.ENOENT
			return
		}
	}

	return
}

func (v *Volume) SetXAttr(ctx context.Context, path string, key string, data []byte, autoCreate bool) error {
	span := spanWithOperation(ctx, "SetXAttr")

	inode, err := v.getInodeFromPath(ctx, path)
	if err != nil {
		span.Errorf("getInodeFromPath fail: volume(%v) path(%v) autoCreate(%v) err(%v)",
			v.name, path, autoCreate, err)
		if err != syscall.ENOENT || !autoCreate {
			return err
		}
	}
	if err == syscall.ENOENT {
		dirs, filename := splitPath(path)
		var parentID uint64
		if parentID, err = v.lookupDirectories(ctx, dirs, true); err != nil {
			return err
		}
		var inodeInfo *proto.InodeInfo
		inodeInfo, err = v.mw.Create_ll(parentID, filename, DefaultFileMode, 0, 0, nil, path)
		if err != nil {
			span.Errorf("meta Create_ll file fail: volume(%v) parentID(%v) name(%v) err(%v)",
				v.name, parentID, filename, err)
			return err
		}
		inode = inodeInfo.Inode
	}

	if err = v.mw.XAttrSet_ll(inode, []byte(key), data); err != nil {
		span.Errorf("meta XAttrSet_ll fail: volume(%v) path(%v) inode(%v) key(%v) err(%v)",
			v.name, path, inode, key, err)
		return err
	}
	updateAttrCache(inode, key, string(data), v.name)

	return nil
}

func (v *Volume) getXAttr(ctx context.Context, path string, key string) (*proto.XAttrInfo, error) {
	span := spanWithOperation(ctx, "getXAttr")

	inode, err := v.getInodeFromPath(ctx, path)
	if err == nil {
		span.Errorf("getInodeFromPath fail: volume(%v) path(%v) err(%v)", v.name, path, err)
		return nil, err
	}
	info, err := v.mw.XAttrGet_ll(inode, key)
	if err != nil {
		span.Errorf("meta XAttrGet_ll fail: volume(%v) path(%v) inode(%v) key(%v) err(%v)",
			v.name, path, inode, key, err)
		return nil, err
	}

	return info, err
}

func (v *Volume) IsEmpty() bool {
	children, err := v.mw.ReadDir_ll(proto.RootIno)
	if err != nil {
		return false
	}
	if len(children) > 0 {
		return false
	}
	return true
}

func (v *Volume) GetXAttr(ctx context.Context, path string, key string) (*proto.XAttrInfo, error) {
	span := spanWithOperation(ctx, "GetXAttr")

	var inode uint64
	var notUseCache bool
	var err error
	if objMetaCache != nil {
		retry := 0
		for {
			if _, inode, _, _, err = v.recursiveLookupTarget(ctx, path, notUseCache); err != nil {
				span.Errorf("recursiveLookupTarget fail: volume(%v) path(%v) notUseCache(%v) err(%v)",
					v.name, path, notUseCache, err)
				return v.getXAttr(ctx, path, key)
			}

			_, err = v.mw.InodeGet_ll(inode)
			if err == syscall.ENOENT && retry < MaxRetry {
				notUseCache = true
				retry++
				continue
			}
			if err != nil {
				span.Errorf("meta InodeGet_ll fail: volume(%v) inode(%v) retry(%v) err(%v)",
					v.name, inode, retry, err)
				return v.getXAttr(ctx, path, key)
			}
			break
		}

		info := &proto.XAttrInfo{
			Inode:  inode,
			XAttrs: make(map[string]string, 0),
		}

		var attr *proto.XAttrInfo
		attrItem, needRefresh := objMetaCache.GetAttr(v.name, inode)
		if attrItem == nil || needRefresh {
			if attr, err = v.mw.XAttrGetAll_ll(inode); err != nil {
				span.Errorf("meta XAttrGetAll_ll fail: volume(%v) inode(%v) err(%v)",
					v.name, inode, err)
				return v.getXAttr(ctx, path, key)
			}
			attrItem = &AttrItem{
				XAttrInfo: *attr,
			}
			objMetaCache.PutAttr(v.name, attrItem)
			val, ok := attr.XAttrs[key]
			if !ok {
				return v.getXAttr(ctx, path, key)
			}
			info.XAttrs[key] = val
		} else {
			val, ok := attrItem.XAttrs[key]
			if !ok {
				if attr, err = v.mw.XAttrGetAll_ll(inode); err != nil {
					span.Errorf("meta XAttrGetAll_ll fail: volume(%v) inode(%v) err(%v)",
						v.name, inode, err)
					return v.getXAttr(ctx, path, key)
				}
				if val, ok = attr.XAttrs[key]; !ok {
					return v.getXAttr(ctx, path, key)
				}
			}
			info.XAttrs[key] = val
		}

		return info, nil
	}

	return v.getXAttr(ctx, path, key)
}

func (v *Volume) DeleteXAttr(ctx context.Context, path string, key string) (err error) {
	span := spanWithOperation(ctx, "DeleteXAttr")

	inode, err := v.getInodeFromPath(ctx, path)
	if err != nil {
		span.Errorf("getInodeFromPath fail: volume(%v) path(%v) err(%v)",
			v.name, path, err)
		return
	}
	if err = v.mw.XAttrDel_ll(inode, key); err != nil {
		span.Errorf("meta XAttrDel_ll fail: volume(%v) inode(%v) key(%v) err(%v)",
			v.name, inode, key, err)
		return
	}
	if objMetaCache != nil {
		objMetaCache.DeleteAttrWithKey(v.name, inode, key)
	}

	return
}

func (v *Volume) listXAttrs(ctx context.Context, path string) ([]string, error) {
	span := spanWithOperation(ctx, "listXAttrs")

	inode, err := v.getInodeFromPath(ctx, path)
	if err != nil {
		span.Errorf("getInodeFromPath fail: volume(%v) path(%v) err(%v)",
			v.name, path, err)
		return nil, err
	}

	keys, err := v.mw.XAttrsList_ll(inode)
	if err != nil {
		span.Errorf("meta XAttrsList_ll fail: volume(%v) inode(%v) err(%v)",
			v.name, inode, err)
		return nil, err
	}

	return keys, nil
}

func (v *Volume) ListXAttrs(ctx context.Context, path string) ([]string, error) {
	span := spanWithOperation(ctx, "ListXAttrs")

	var inode uint64
	var notUseCache bool
	var err error
	if objMetaCache != nil {
		retry := 0
		for {
			if _, inode, _, _, err = v.recursiveLookupTarget(ctx, path, notUseCache); err != nil {
				span.Errorf("recursiveLookupTarget fail: volume(%v) path(%v) notUseCache(%v) err(%v)",
					v.name, path, notUseCache, err)
				return v.listXAttrs(ctx, path)
			}
			_, err = v.mw.InodeGet_ll(inode)
			if err == syscall.ENOENT && retry < MaxRetry {
				notUseCache = true
				retry++
				continue
			}
			if err != nil {
				span.Errorf("meta InodeGet_ll fail: volume(%v) inode(%v) retry(%v) err(%v)",
					v.name, inode, retry, err)
				return v.listXAttrs(ctx, path)
			}
			break
		}

		var attr *proto.XAttrInfo
		attrItem, needRefresh := objMetaCache.GetAttr(v.name, inode)
		if attrItem == nil || needRefresh {
			if attr, err = v.mw.XAttrGetAll_ll(inode); err != nil {
				span.Errorf("meta XAttrGetAll_ll fail: volume(%v) inode(%v) err(%v)",
					v.name, inode, err)
				return v.listXAttrs(ctx, path)
			}
			attrItem = &AttrItem{
				XAttrInfo: *attr,
			}
			objMetaCache.PutAttr(v.name, attrItem)
		}
		keys := make([]string, 0, len(attrItem.XAttrs))
		for key := range attrItem.XAttrs {
			keys = append(keys, key)
		}

		return keys, nil
	}

	return v.listXAttrs(ctx, path)
}

func (v *Volume) OSSSecure() (accessKey, secretKey string) {
	return v.mw.OSSSecure()
}

// ListFilesV1 returns file and directory entry list information that meets the parameters.
// It supports parameters such as prefix, delimiter, and paging.
// It is a data plane logical encapsulation of the object storage interface ListObjectsV1.
func (v *Volume) ListFilesV1(ctx context.Context, opt *ListFilesV1Option) (result *ListFilesV1Result, err error) {
	span := spanWithOperation(ctx, "ListFilesV1")

	var (
		prefix, marker, delimiter string
		maxKeys                   uint64
		onlyObject                bool
	)

	if opt != nil {
		prefix, marker, delimiter = opt.Prefix, opt.Marker, opt.Delimiter
		maxKeys, onlyObject = opt.MaxKeys, opt.OnlyObject
	}

	infos, prefixes, nextMarker, err := v.listFilesV1(ctx, prefix, marker, delimiter, maxKeys, onlyObject)
	if err != nil {
		span.Errorf("listFilesV1 fail: volume(%v) prefix(%v) marker(%v) delimiter(%v) maxKeys(%v) err(%v)",
			v.name, prefix, marker, delimiter, maxKeys, err)
		return
	}

	result = &ListFilesV1Result{
		CommonPrefixes: prefixes,
		NextMarker:     nextMarker,
		Files:          infos,
	}
	if len(nextMarker) > 0 {
		result.Truncated = true
	}

	return
}

// ListFilesV2 returns file and directory entry list information that meets the parameters.
// It supports parameters such as prefix, delimiter, and paging.
// It is a data plane logical encapsulation of the object storage interface ListObjectsV2.
func (v *Volume) ListFilesV2(ctx context.Context, opt *ListFilesV2Option) (result *ListFilesV2Result, err error) {
	span := spanWithOperation(ctx, "ListFilesV2")

	var (
		prefix, startAfter, contToken, delimiter string
		maxKeys                                  uint64
	)

	if opt != nil {
		prefix, startAfter, contToken = opt.Prefix, opt.StartAfter, opt.ContToken
		delimiter, maxKeys = opt.Delimiter, opt.MaxKeys
	}

	infos, prefixes, nextMarker, err := v.listFilesV2(ctx, prefix, startAfter, contToken, delimiter, maxKeys)
	if err != nil {
		span.Errorf("listFilesV2 fail: volume(%v) prefix(%v) startAfter(%v) contToken(%v) delimiter(%v) "+
			"maxKeys(%v) err(%v)",
			v.name, prefix, startAfter, contToken, delimiter, maxKeys, err)
		return
	}

	result = &ListFilesV2Result{
		CommonPrefixes: prefixes,
		Files:          infos,
		KeyCount:       uint64(len(infos)),
	}
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
func (v *Volume) PutObject(ctx context.Context, path string, reader io.Reader, opt *PutFileOption) (*FSFileInfo, error) {
	span := spanWithOperation(ctx, "PutObject")

	// The path is processed according to the content-type. If it is a directory type,
	// a path separator is appended at the end of the path, so the recursiveMakeDirectory
	// method can be processed directly in recursion.
	fixedPath := path
	if opt != nil && opt.MIMEType == ValueContentTypeDirectory && !strings.HasSuffix(path, pathSep) {
		fixedPath = path + pathSep
	}

	pathItems := NewPathIterator(fixedPath).ToSlice()
	if len(pathItems) == 0 {
		// A blank directory entry indicates that the path after the validation is the volume
		// root directory itself.
		return &FSFileInfo{
			Path:       path,
			Size:       0,
			Mode:       DefaultDirMode,
			CreateTime: time.Now(),
			ModifyTime: time.Now(),
			ETag:       EmptyContentMD5String,
			Inode:      rootIno,
			MIMEType:   ValueContentTypeDirectory,
		}, nil
	}

	parentId, err := v.recursiveMakeDirectory(ctx, fixedPath)
	if err != nil {
		span.Errorf("recursiveMakeDirectory fail: volume(%v) path(%v) err(%v)",
			v.name, path, err)
		return nil, err
	}

	lastPathItem := pathItems[len(pathItems)-1]
	if lastPathItem.IsDirectory {
		// If the last path node is a directory, then it has been processed by the previous logic.
		// Just get the information of this node and return.
		var info *proto.InodeInfo
		if info, err = v.mw.InodeGet_ll(parentId); err != nil {
			span.Errorf("meta InodeGet_ll fail: volume(%v) path(%v) inode(%v) err(%v)",
				v.name, path, parentId, err)
			return nil, err
		}
		return &FSFileInfo{
			Path:       path,
			Size:       0,
			Mode:       DefaultDirMode,
			CreateTime: info.CreateTime,
			ModifyTime: info.ModifyTime,
			ETag:       EmptyContentMD5String,
			Inode:      info.Inode,
			MIMEType:   ValueContentTypeDirectory,
		}, nil
	}

	// check file mode
	oldInode, lookupMode, err := v.mw.Lookup_ll(parentId, lastPathItem.Name)
	if err != nil && err != syscall.ENOENT {
		span.Errorf("meta Lookup_ll fail: volume(%v) parentID(%v) name(%v) err(%v)",
			v.name, parentId, lastPathItem.Name, err)
		return nil, err
	}
	if err == nil && os.FileMode(lookupMode).IsDir() {
		span.Errorf("the last name of path is a dir: volume(%v) path(%v) name(%v)",
			v.name, path, lastPathItem.Name)
		err = syscall.EINVAL
		return nil, err
	}

	// check whether existing object is protected by object lock
	if oldInode != 0 && opt != nil && opt.ObjectLock != nil {
		err = isObjectLocked(ctx, v, oldInode, lastPathItem.Name, path)
		if err != nil {
			span.Errorf("check objectLock protected: volume(%v) path(%v) inode(%v) err(%v)",
				v.name, path, oldInode, err)
			return nil, err
		}
	}

	// Intermediate data during the writing of new versions is managed through invisible files.
	// This file has only inode but no dentry. In this way, this temporary file can be made invisible
	// in the true sense. In order to avoid the adverse impact of other user operations on temporary data.
	quotaIds := make([]uint64, 0)
	tempInode, err := v.mw.InodeCreate_ll(parentId, DefaultFileMode, 0, 0, nil, quotaIds, fixedPath)
	if err != nil {
		span.Errorf("meta InodeCreate_ll file fail: volume(%v) parentID(%v) err(%v)",
			v.name, parentId, err)
		return nil, err
	}
	defer func() {
		// An error has caused the entire process to fail. Delete the inode and release the written data.
		if err != nil {
			span.Warnf("unlink temp inode: volume(%v) path(%v) inode(%v)",
				v.name, path, tempInode.Inode)
			_, _ = v.mw.InodeUnlink_ll(tempInode.Inode, fixedPath)
			span.Warnf("evict temp inode: volume(%v) path(%v) inode(%v)",
				v.name, path, tempInode.Inode)
			_ = v.mw.Evict(tempInode.Inode, fixedPath)
		}
	}()

	if err = v.ec.OpenStream(ctx, tempInode.Inode); err != nil {
		span.Errorf("data OpenStream fail: volume(%v) inode(%v) err(%v)",
			v.name, tempInode.Inode, err)
		return nil, err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(tempInode.Inode); closeErr != nil {
			span.Errorf("data CloseStream fail: volume(%v) inode(%v) err(%v)",
				v.name, tempInode.Inode, closeErr)
		}
	}()

	md5Hash := md5.New()
	if proto.IsCold(v.volType) {
		if _, err = v.ebsWrite(ctx, tempInode.Inode, reader, md5Hash); err != nil {
			span.Errorf("ebsWrite fail: volume(%v) inode(%v) err(%v)",
				v.name, tempInode.Inode, err)
			return nil, err
		}
	} else {
		if _, err = v.streamWrite(ctx, tempInode.Inode, reader, md5Hash); err != nil {
			span.Errorf("streamWrite fail: volume(%v) inode(%v) err(%v)",
				v.name, tempInode.Inode, err)
			return nil, err
		}
		// flush
		if err = v.ec.Flush(ctx, tempInode.Inode); err != nil {
			span.Errorf("data Flush fail: volume(%v) inode(%v) err(%v)",
				v.name, tempInode.Inode, err)
			return nil, err
		}
	}

	var finalInode *proto.InodeInfo
	if finalInode, err = v.mw.InodeGet_ll(tempInode.Inode); err != nil {
		span.Errorf("meta InodeGet_ll fail: volume(%v) inode(%v) err(%v)",
			v.name, tempInode.Inode, err)
		return nil, err
	}

	etagValue := ETagValue{
		Value:   hex.EncodeToString(md5Hash.Sum(nil)),
		PartNum: 0,
		TS:      finalInode.ModifyTime,
	}

	attr := &AttrItem{
		XAttrInfo: proto.XAttrInfo{
			Inode:  tempInode.Inode,
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
		attr.XAttrs[XAttrKeyOSSACL] = opt.ACL.Encode()
	}
	if opt != nil && opt.ObjectLock != nil && opt.ObjectLock.ToRetention() != nil {
		attr.XAttrs[XAttrKeyOSSLock] = formatRetentionDateStr(finalInode.ModifyTime, opt.ObjectLock.ToRetention())
	}

	// If user-defined metadata have been specified, use extend attributes for storage.
	if opt != nil && len(opt.Metadata) > 0 {
		for name, value := range opt.Metadata {
			attr.XAttrs[name] = value
		}
	}

	if err = v.mw.BatchSetXAttr_ll(tempInode.Inode, attr.XAttrs); err != nil {
		span.Errorf("meta BatchSetXAttr_ll fail: volume(%v) inode(%v) attrs(%+v) err(%v)",
			v.name, tempInode.Inode, attr.XAttrs, err)
		return nil, err
	}

	// apply new inode to dentry
	err = v.applyInodeToDEntry(ctx, parentId, lastPathItem.Name, tempInode.Inode, false, fixedPath)
	if err != nil {
		span.Errorf("applyInodeToDEntry fail: volume(%v) parentID(%v) name(%v) inode(%v) err(%v)",
			v.name, parentId, lastPathItem.Name, tempInode.Inode, err)
		return nil, err
	}

	// force updating dentry and attrs in cache
	updateDentryCache(parentId, tempInode.Inode, DefaultFileMode, lastPathItem.Name, v.name)
	putAttrCache(attr, v.name)

	return &FSFileInfo{
		Path:       path,
		Size:       int64(finalInode.Size),
		Mode:       os.FileMode(finalInode.Mode),
		CreateTime: finalInode.CreateTime,
		ModifyTime: finalInode.ModifyTime,
		ETag:       etagValue.ETag(),
		Inode:      finalInode.Inode,
	}, nil
}

func (v *Volume) applyInodeToDEntry(
	ctx context.Context,
	parentId uint64,
	name string,
	inode uint64,
	isCompleteMultipart bool,
	fullPath string,
) error {
	span := spanWithOperation(ctx, "applyInodeToDEntry")
	_, existMode, err := v.mw.Lookup_ll(parentId, name) // exist object inode
	if err != nil {
		if err == syscall.ENOENT {
			if err = v.applyInodeToNewDentry(ctx, parentId, name, inode, fullPath); err != nil {
				span.Errorf("applyInodeToNewDentry fail: volume(%v) parentID(%v) name(%v) inode(%v) err(%v)",
					v.name, parentId, name, inode, err)
			}
		}
		return err
	}

	if os.FileMode(existMode).IsDir() {
		span.Errorf("target mode conflict: parentID(%v) name(%v) mode(%v)",
			parentId, name, os.FileMode(existMode).String())
		return syscall.EINVAL
	}
	// current implementation doesn't support object versioning, so uploading a object with a key already existed in bucket
	// is implemented with replacing the old one instead.
	// refer: https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html
	if err = v.applyInodeToExistDentry(ctx, parentId, name, inode, isCompleteMultipart, fullPath); err != nil {
		span.Errorf("applyInodeToExistDentry fail: volume(%v) parentID(%v) name(%v) inode(%v) "+
			"isCompleteMultipart(%v) err(%v)",
			v.name, parentId, name, inode, isCompleteMultipart, err)
	}

	return err
}

// DeletePath deletes the specified path.
// If the target is a non-empty directory, it will return success without any operation.
// If the target does not exist, it returns success.
//
// Notes:
// This method will only returns internal system errors.
// This method will not return syscall.ENOENT error
func (v *Volume) DeletePath(ctx context.Context, path string) (err error) {
	span := spanWithOperation(ctx, "DeletePath")

	defer func() {
		// In the operation of deleting a path, if no path matching the given path is found,
		// that is, the given path does not exist, the return is successful.
		if err == syscall.ENOENT {
			err = nil
		}
	}()

	parent, ino, name, mode, err := v.recursiveLookupTarget(ctx, path, false)
	if err != nil {
		span.Errorf("recursiveLookupTarget fail: volume(%v) path(%v) err(%v)",
			v.name, path, err)
		return err
	}
	span.Infof("lookup target: volume(%v) path(%v) parentID(%v) inode(%v) name(%v) mode(%v)",
		v.name, path, parent, ino, name, mode)
	if mode.IsDir() {
		// Check if the directory is empty and cannot delete non-empty directories.
		var dentries []proto.Dentry
		dentries, err = v.mw.ReadDirLimit_ll(ino, "", 1)
		if err != nil || len(dentries) > 0 {
			span.Errorf("meta ReadDirLimit_ll fail or not empty: volume(%v) inode(%v) err(%v)",
				v.name, ino, err)
			return err
		}
	}
	// check whether object is protected by object lock
	objetLock, err := v.metaLoader.loadObjectLock(ctx)
	if err != nil {
		span.Errorf("load objetLock fail: volume(%v) err(%v)", v.name, err)
		return
	}

	// delete dentry with condition when objectlock is open
	if objetLock != nil {
		_, err = v.mw.DeleteWithCond_ll(parent, ino, name, mode.IsDir(), path)
	} else {
		_, err = v.mw.Delete_ll(parent, name, mode.IsDir(), path)
	}
	if err != nil {
		span.Errorf("meta Delete_ll fail: volume(%v) parentID(%v) inode(%v) name(%v) err(%v)",
			v.name, parent, ino, name, err)
		return
	}

	if err = v.ec.EvictStream(ino); err != nil {
		span.Warnf("data EvictStream fail: volume(%v) inode(%v) err(%v)", v.name, ino, err)
	}

	// delete objectnode meta cache
	deleteDentryCache(parent, name, v.name)
	deleteAttrCache(parent, v.name)

	// Evict inode
	if err = v.mw.Evict(ino, path); err != nil {
		span.Warnf("meta Evict fail: volume(%v) path(%v) inode(%v) err(%v)",
			v.name, path, ino, err)
		err = nil
	}

	return
}

func (v *Volume) InitMultipart(ctx context.Context, path string, opt *PutFileOption) (multipartID string, err error) {
	span := spanWithOperation(ctx, "InitMultipart")

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
		encoded := opt.Tagging.Encode()
		extend[XAttrKeyOSSTagging] = encoded
	}
	// If ACL have been specified, use extend attributes for storage.
	if opt != nil && opt.ACL != nil {
		extend[XAttrKeyOSSACL] = opt.ACL.Encode()
	}

	if v.mw.EnableQuota {
		var parentId uint64
		if parentId, err = v.recursiveMakeDirectory(ctx, path); err != nil {
			span.Errorf("recursiveMakeDirectory fail: volume(%v) path(%v) err(%v)",
				v.name, path, err)
			return
		}

		if v.mw.IsQuotaLimitedById(parentId, true, true) {
			span.Errorf("volume(%v) inode(%v) quota limited", v.name, parentId)
			err = syscall.ENOSPC
			return
		}
	}

	// Iterate all the meta partition to create multipart id
	multipartID, err = v.mw.InitMultipart_ll(path, extend)
	if err != nil {
		span.Errorf("meta InitMultipart_ll fail: volume(%v) path(%v) err(%v)", v.name, path, err)
	}

	return
}

func (v *Volume) WritePart(ctx context.Context, path string, multipartId string, partId uint16, reader io.Reader) (*FSFileInfo, error) {
	span := spanWithOperation(ctx, "WritePart")

	// create temp file (inode only, invisible for user)
	tempInodeInfo, err := v.mw.InodeCreate_ll(0, DefaultFileMode, 0, 0, nil, make([]uint64, 0), path)
	if err != nil {
		span.Errorf("meta InodeCreate_ll file fail: volume(%v) multipartID(%v) partNum(%v) err(%v)",
			v.name, multipartId, partId, err)
		return nil, err
	}

	var oldInode uint64
	var exist bool
	defer func() {
		// An error has caused the entire process to fail. Delete the inode and release the written data.
		if err != nil {
			span.Warnf("unlink part inode: volume(%v) multipartID(%v) partNum(%v) inode(%v)",
				v.name, multipartId, partId, tempInodeInfo.Inode)
			_, _ = v.mw.InodeUnlink_ll(tempInodeInfo.Inode, path)
			span.Warnf("evict part inode: volume(%v) multipartID(%v) partNum(%v) inode(%v)",
				v.name, multipartId, partId, tempInodeInfo.Inode)
			_ = v.mw.Evict(tempInodeInfo.Inode, path)
		}
		// Delete the old inode and release the written data.
		if exist {
			span.Warnf("unlink old part inode: volume(%v) multipartID(%v) partNum(%v) inode(%v)",
				v.name, multipartId, partId, oldInode)
			_, _ = v.mw.InodeUnlink_ll(oldInode, path)
			span.Warnf("evict old part inode: volume(%v) multipartID(%v) partNum(%v) inode(%v)",
				v.name, multipartId, partId, oldInode)
			_ = v.mw.Evict(oldInode, path)
		}
	}()

	var (
		size    uint64
		etag    string
		md5Hash = md5.New()
	)

	if err = v.ec.OpenStream(ctx, tempInodeInfo.Inode); err != nil {
		span.Errorf("data OpenStream fail: volume(%v) multipartID(%v) partNum(%v) inode(%v) err(%v)",
			v.name, multipartId, partId, tempInodeInfo.Inode, err)
		return nil, err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(tempInodeInfo.Inode); closeErr != nil {
			span.Errorf("data CloseStream fail: volume(%v) multipartID(%v) partNum(%v) inode(%v) err(%v)",
				v.name, multipartId, partId, tempInodeInfo.Inode, closeErr)
		}
	}()

	if proto.IsCold(v.volType) {
		if size, err = v.ebsWrite(ctx, tempInodeInfo.Inode, reader, md5Hash); err != nil {
			span.Errorf("data ebsWrite fail: volume(%v) inode(%v) multipartID(%v) partNum(%v) err(%v)",
				v.name, tempInodeInfo.Inode, multipartId, partId, err)
			return nil, err
		}
	} else {
		// Write data to data node
		if size, err = v.streamWrite(ctx, tempInodeInfo.Inode, reader, md5Hash); err != nil {
			span.Errorf("data streamWrite fail: volume(%v) inode(%v) multipartID(%v) partNum(%v) err(%v)",
				v.name, tempInodeInfo.Inode, multipartId, partId, err)
			return nil, err
		}
		// flush
		if err = v.ec.Flush(ctx, tempInodeInfo.Inode); err != nil {
			span.Errorf("data Flush fail: volume(%v) inode(%v) multipartID(%v) partNum(%v) err(%v)",
				v.name, tempInodeInfo.Inode, multipartId, partId, err)
			return nil, err
		}
	}

	// compute file md5
	etag = hex.EncodeToString(md5Hash.Sum(nil))

	// update temp file inode to meta with session, overwrite existing part can result in exist == true
	oldInode, exist, err = v.mw.AddMultipartPart_ll(path, multipartId, partId, size, etag, tempInodeInfo)
	if err != nil {
		span.Errorf("meta AddMultipartPart_ll fail: volume(%v) path(%v) multipartID(%v) partNum(%v) "+
			"inode(%v) size(%v) MD5(%v) err(%v)",
			v.name, path, multipartId, partId, tempInodeInfo.Inode, size, etag, err)
		return nil, err
	}

	_, fileName := splitPath(path)
	return &FSFileInfo{
		Path:       fileName,
		Size:       int64(size),
		Mode:       os.FileMode(DefaultFileMode),
		ModifyTime: time.Now(),
		CreateTime: tempInodeInfo.CreateTime,
		ETag:       etag,
		Inode:      tempInodeInfo.Inode,
	}, nil
}

func (v *Volume) AbortMultipart(ctx context.Context, path string, multipartID string) (err error) {
	span := spanWithOperation(ctx, "AbortMultipart")

	// get multipart info
	var multipartInfo *proto.MultipartInfo
	if multipartInfo, err = v.mw.GetMultipart_ll(path, multipartID); err != nil {
		span.Errorf("meta GetMultipart_ll fail: volume(%v) path(%v) multipartID(%v) err(%v)",
			v.name, path, multipartID, err)
		return
	}
	// release part data asyncly
	go func() {
		for _, part := range multipartInfo.Parts {
			span.Warnf("unlink part inode: volume(%v) multipartID(%v) partNum(%v) inode(%v)",
				v.name, multipartID, part.ID, part.Inode)
			if _, err = v.mw.InodeUnlink_ll(part.Inode, path); err != nil {
				span.Warnf("meta InodeUnlink_ll fail: volume(%v) multipartID(%v) partNum(%v) inode(%v) err(%v)",
					v.name, multipartID, part.ID, part.Inode, err)
			}
		}
	}()
	if err = v.mw.RemoveMultipart_ll(path, multipartID); err != nil {
		span.Errorf("meta RemoveMultipart_ll fail: volume(%v) path(%v) multipartID(%v) err(%v)",
			v.name, path, multipartID, err)
	}

	return
}

func (v *Volume) CompleteMultipart(
	ctx context.Context,
	path, multipartID string,
	multipartInfo *proto.MultipartInfo,
	discardedPartInodes map[uint64]uint16,
) (fsFileInfo *FSFileInfo, err error) {
	span := spanWithOperation(ctx, "CompleteMultipart")

	var (
		pathItems = NewPathIterator(path).ToSlice()
		filename  = pathItems[len(pathItems)-1].Name
		parentId  uint64
	)

	if parentId, err = v.recursiveMakeDirectory(ctx, path); err != nil {
		span.Errorf("recursiveMakeDirectory fail: volume(%v) path(%v) err(%v)",
			v.name, path, err)
		return
	}
	// check file mode
	oldInode, lookupMode, err := v.mw.Lookup_ll(parentId, filename)
	if err != nil && err != syscall.ENOENT {
		span.Errorf("meta Lookup_ll fail: volume(%v) parentInode(%v) name(%v) err(%v)",
			v.name, parentId, filename, err)
		return
	}
	if err == nil && os.FileMode(lookupMode).IsDir() {
		span.Errorf("the last name of path is a dir: volume(%v) path(%v) name(%v)",
			v.name, path, filename)
		err = syscall.EINVAL
		return
	}
	// check whether object is protected by object lock
	objectLock, err := v.metaLoader.loadObjectLock(ctx)
	if err != nil {
		span.Errorf("load volume objectLock: volume(%v) err(%v)", v.name, err)
		return
	}
	if oldInode != 0 && objectLock != nil {
		err = isObjectLocked(ctx, v, oldInode, filename, path)
		if err != nil {
			span.Errorf("check objectLock protected: volume(%v) inode(%v) err(%v)",
				v.name, oldInode, filename)
			return
		}
	}
	parts := multipartInfo.Parts
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })

	// create inode for complete data
	completeInodeInfo, err := v.mw.InodeCreate_ll(parentId, DefaultFileMode, 0, 0, nil, make([]uint64, 0), path)
	if err != nil {
		span.Errorf("meta InodeCreate_ll file fail: volume(%v) parentID(%v) err(%v)",
			v.name, parentId, err)
		return
	}
	defer func() {
		if err != nil {
			span.Warnf("complete failed and delete inode: volume(%v) path(%v) multipartID(%v) inode(%v)",
				v.name, path, multipartID, completeInodeInfo.Inode)
			if deleteErr := v.mw.InodeDelete_ll(completeInodeInfo.Inode, path); deleteErr != nil {
				span.Errorf("meta InodeDelete_ll fail: volume(%v) path(%v) multipartID(%v) inode(%v) err(%v)",
					v.name, path, multipartID, completeInodeInfo.Inode, err)
			}
		}
	}()

	// merge complete extent keys
	var size uint64
	var fileOffset uint64
	if proto.IsCold(v.volType) {
		completeObjExtentKeys := make([]proto.ObjExtentKey, 0)
		for _, part := range parts {
			var objExtents []proto.ObjExtentKey
			if _, _, _, objExtents, err = v.mw.GetObjExtents(part.Inode); err != nil {
				span.Errorf("meta GetObjExtents fail: volume(%v) partNum(%v) inode(%v) err(%v)",
					v.name, part.ID, part.Inode, err)
				return
			}
			for _, ek := range objExtents {
				ek.FileOffset = fileOffset
				fileOffset += ek.Size
				completeObjExtentKeys = append(completeObjExtentKeys, ek)
			}
			size += part.Size
		}
		if err = v.mw.AppendObjExtentKeys(completeInodeInfo.Inode, completeObjExtentKeys); err != nil {
			span.Errorf("meta AppendObjExtentKeys fail: volume(%v) inode(%v) err(%v)",
				v.name, completeInodeInfo.Inode, err)
			return
		}
	} else {
		completeExtentKeys := make([]proto.ExtentKey, 0)
		for _, part := range parts {
			var eks []proto.ExtentKey
			if _, _, eks, err = v.mw.GetExtents(part.Inode); err != nil {
				span.Errorf("meta GetExtents fail: volume(%v) partNum(%v) inode(%v) err(%v)",
					v.name, part.ID, part.Inode, err)
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
			span.Errorf("meta AppendExtentKeys fail: volume(%v) inode(%v) err(%v)",
				v.name, completeInodeInfo.Inode, err)
			return
		}
	}

	// compute md5 hash
	var md5Val string
	if len(parts) == 1 {
		md5Val = parts[0].MD5
	} else {
		md5Hash := md5.New()
		for _, part := range parts {
			md5Hash.Write([]byte(part.MD5))
		}
		md5Val = hex.EncodeToString(md5Hash.Sum(nil))
	}

	var finalInode *proto.InodeInfo
	if finalInode, err = v.mw.InodeGet_ll(completeInodeInfo.Inode); err != nil {
		span.Errorf("meta InodeGet_ll fail: volume(%v) inode(%v) err(%v)",
			v.name, completeInodeInfo.Inode, err)
		return
	}

	etagValue := ETagValue{
		Value:   md5Val,
		PartNum: len(parts),
		TS:      finalInode.ModifyTime,
	}

	attrs := make(map[string]string)
	attrItem := &AttrItem{
		XAttrInfo: proto.XAttrInfo{
			Inode:  completeInodeInfo.Inode,
			XAttrs: attrs,
		},
	}
	attrs[XAttrKeyOSSETag] = etagValue.Encode()
	// set user modified system metadata, self defined metadata and tag
	extend := multipartInfo.Extend
	if len(extend) > 0 {
		for key, value := range extend {
			attrs[key] = value
		}
	}
	if objectLock != nil && objectLock.ToRetention() != nil {
		attrs[XAttrKeyOSSLock] = formatRetentionDateStr(finalInode.ModifyTime, objectLock.ToRetention())
	}

	if err = v.mw.BatchSetXAttr_ll(finalInode.Inode, attrs); err != nil {
		span.Errorf("meta BatchSetXAttr_ll fail: volume(%v) inode(%v) attrs(%v) err(%v)",
			v.name, finalInode.Inode, attrs, err)
		return
	}

	// apply new inode to dentry
	err = v.applyInodeToDEntry(ctx, parentId, filename, completeInodeInfo.Inode, true, path)
	if err != nil {
		span.Errorf("applyInodeToDEntry fail: volume(%v) parentId(%v) name(%v) inode(%v) err(%v)",
			v.name, parentId, filename, completeInodeInfo.Inode, err)
		return
	}

	// remove multipart
	var err2 error
	if err2 = v.mw.RemoveMultipart_ll(path, multipartID); err2 != nil {
		span.Warnf("meta RemoveMultipart_ll fail: volume(%v) multipartID(%v) path(%v) err(%v)",
			v.name, multipartID, path, err2)
	}

	// handle temp metadata asyncly
	go func() {
		// delete part inodes
		for _, part := range parts {
			span.Warnf("complete success and delete part inode: %+v", part)
			if err2 = v.mw.InodeDelete_ll(part.Inode, path); err2 != nil {
				span.Warnf("meta InodeDelete_ll fail: volume(%v) part(%+v) err(%v)", v.name, part, err2)
			}
		}
		// discard part inodes and data
		for discardedInode, partNum := range discardedPartInodes {
			span.Warnf("discard part(%v) inode(%v)", partNum, discardedInode)
			if _, err2 = v.mw.InodeUnlink_ll(discardedInode, path); err2 != nil {
				span.Warnf("meta InodeUnlink_ll fail: volume(%v) inode(%v) err(%v)",
					v.name, discardedInode, err2)
			}
		}
	}()

	// force updating dentry and attrs in cache
	updateDentryCache(parentId, completeInodeInfo.Inode, DefaultFileMode, filename, v.name)
	putAttrCache(attrItem, v.name)

	fsFileInfo = &FSFileInfo{
		Path:       path,
		Size:       int64(size),
		Mode:       os.FileMode(DefaultFileMode),
		CreateTime: time.Now(),
		ModifyTime: time.Now(),
		ETag:       etagValue.ETag(),
		Inode:      finalInode.Inode,
	}

	return
}

func (v *Volume) ebsWrite(ctx context.Context, inode uint64, reader io.Reader, h hash.Hash) (uint64, error) {
	return v.getEbsWriter(inode).WriteFromReader(ctx, reader, h)
}

func (v *Volume) streamWrite(ctx context.Context, inode uint64, reader io.Reader, h hash.Hash) (size uint64, err error) {
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
			checkFunc := func() error {
				if !v.mw.EnableQuota {
					return nil
				}

				if ok := v.ec.UidIsLimited(ctx, 0); ok {
					return syscall.ENOSPC
				}

				if v.mw.IsQuotaLimitedById(inode, true, false) {
					return syscall.ENOSPC
				}
				return nil
			}
			if writeN, err = v.ec.Write(ctx, inode, offset, buf[:readN], 0, checkFunc); err != nil {
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

func (v *Volume) appendInodeHash(ctx context.Context, h hash.Hash, inode uint64, total uint64, preAllocatedBuf []byte) (err error) {
	span := spanWithOperation(ctx, "appendInodeHash")

	if err = v.ec.OpenStream(ctx, inode); err != nil {
		span.Errorf("data OpenStream fail: inode(%v) err(%v)", inode, err)
		return
	}
	defer func() {
		if closeErr := v.ec.CloseStream(inode); closeErr != nil {
			span.Warnf("data CloseStream fail: inode(%v) err(%v)", inode, closeErr)
		}
		if evictErr := v.ec.EvictStream(inode); evictErr != nil {
			span.Warnf("data EvictStream fail: inode(%v) err(%v)", inode, evictErr)
		}
	}()

	buf := preAllocatedBuf
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
		n, err = v.ec.Read(ctx, inode, buf, offset, size)
		if err != nil && err != io.EOF {
			span.Errorf("data Read fail: inode(%v) offset(%v) size(%v) err(%v)",
				inode, offset, size, err)
			return
		}
		if n > 0 {
			if _, err = h.Write(buf[:n]); err != nil {
				span.Errorf("hash write fail: %v", err)
				return
			}
			offset += n
		}
		if n == 0 || err == io.EOF {
			break
		}
	}

	return
}

func (v *Volume) applyInodeToNewDentry(ctx context.Context, parentID uint64, name string, inode uint64, fullPath string) error {
	return v.mw.DentryCreate_ll(parentID, name, inode, DefaultFileMode, fullPath)
}

func (v *Volume) applyInodeToExistDentry(
	ctx context.Context,
	parentID uint64,
	name string,
	inode uint64,
	isCompleteMultipart bool,
	fullPath string,
) (err error) {
	span := spanWithOperation(ctx, "applyInodeToExistDentry")

	var oldInode uint64
	oldInode, err = v.mw.DentryUpdate_ll(parentID, name, inode, fullPath)
	if err != nil {
		span.Errorf("meta DentryUpdate_ll fail: volume(%v) parentID(%v) name(%v) inode(%v) err(%v)",
			v.name, parentID, name, inode, err)
		return
	}

	if oldInode == 0 {
		span.Warnf("dentry update the same inode: volume(%v) inode(%v)", v.name, inode)
		return
	}

	// concurrent completeMultipart request: temporary data security check
	if isCompleteMultipart {
		var isSameExtent bool
		isSameExtent, err = v.referenceExtentKey(ctx, oldInode, inode)
		if err != nil {
			span.Errorf("referenceExtentKey fail: volume(%v) oldInode(%v) inode(%v) err(%v)",
				v.name, oldInode, inode, err)
			return
		}
		if isSameExtent {
			span.Warnf("concurrent completeMultipart: volume(%v) parentID(%v) name(%v) inode(%v)",
				v.name, parentID, name, inode)
			return
		}
	}

	// unlink and evict old inode
	span.Warnf("unlink existing inode: volume(%v) inode(%v)", v.name, oldInode)
	if _, err = v.mw.InodeUnlink_ll(oldInode, fullPath); err != nil {
		span.Warnf("meta InodeUnlink_ll fail: volume(%v) inode(%v) err(%v)", v.name, oldInode, err)
	}

	span.Warnf("evict existing inode: volume(%v) inode(%v)", v.name, oldInode)
	if err = v.mw.Evict(oldInode, fullPath); err != nil {
		span.Warnf("meta Evict fail: volume(%v) inode(%v) err(%v)",
			v.name, oldInode, err)
	}
	err = nil

	return
}

func (v *Volume) loadUserDefinedMetadata(ctx context.Context, inode uint64) (metadata map[string]string, err error) {
	span := spanWithOperation(ctx, "loadUserDefinedMetadata")

	var storedXAttrKeys []string
	if storedXAttrKeys, err = v.mw.XAttrsList_ll(inode); err != nil {
		span.Errorf("meta XAttrsList_ll fail: volume(%v) inode(%v) err(%v)", v.name, inode, err)
		return
	}
	xattrKeys := make([]string, 0)
	for _, storedXAttrKey := range storedXAttrKeys {
		if !strings.HasPrefix(storedXAttrKey, "oss:") {
			xattrKeys = append(xattrKeys, storedXAttrKey)
		}
	}
	var xattrs []*proto.XAttrInfo
	if xattrs, err = v.mw.BatchGetXAttr([]uint64{inode}, xattrKeys); err != nil {
		span.Errorf("meta BatchGetXAttr fail: volume(%v) inode(%v) keys(%v) err(%v)",
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

func (v *Volume) readFile(ctx context.Context, inode, inodeSize uint64, path string, writer io.Writer, offset, size uint64) error {
	span := spanWithOperation(ctx, "readFile")

	var err error
	if err = v.ec.OpenStream(ctx, inode); err != nil {
		span.Errorf("data OpenStream fail: volume(%v) inode(%v) err(%v)", v.name, inode, err)
		return err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(inode); closeErr != nil {
			span.Errorf("data CloseStream fail: volume(%v) inode(%v) err(%v)", v.name, inode, closeErr)
		}
	}()

	if proto.IsHot(v.volType) {
		if err = v.read(ctx, inode, inodeSize, path, writer, offset, size); err != nil {
			span.Errorf("read data fail: volume(%v) inode(%v) err(%v)", v.name, inode, err)
		}
	} else {
		if err = v.readEbs(ctx, inode, inodeSize, path, writer, offset, size); err != nil {
			span.Errorf("read ebs data fail: volume(%v) inode(%v) err(%v)", v.name, inode, err)
		}
	}

	return err
}

func (v *Volume) readEbs(ctx context.Context, inode, inodeSize uint64, path string, writer io.Writer, offset, size uint64) error {
	upper := size + offset
	if upper > inodeSize {
		upper = inodeSize - offset
	}

	_ = context.WithValue(ctx, "objectnode", 1)
	reader := v.getEbsReader(inode)
	var n int
	var rest uint64
	tmp := buf.ReadBufPool.Get().([]byte)
	defer buf.ReadBufPool.Put(tmp)

	for {
		if rest = upper - offset; rest <= 0 {
			break
		}
		readSize := len(tmp)
		if uint64(readSize) > rest {
			readSize = int(rest)
		}
		tmp = tmp[:readSize]
		off, err := safeConvertUint64ToInt(offset)
		if err != nil {
			return err
		}
		n, err = reader.Read(ctx, tmp, off, readSize)
		if err != nil && err != io.EOF {
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

func (v *Volume) read(ctx context.Context, inode, inodeSize uint64, path string, writer io.Writer, offset, size uint64) error {
	upper := size + offset
	if upper > inodeSize {
		upper = inodeSize - offset
	}

	var n int
	tmp := make([]byte, 2*util.BlockSize)
	for {
		rest := upper - offset
		if rest == 0 {
			break
		}
		readSize := len(tmp)
		if uint64(readSize) > rest {
			readSize = int(rest)
		}
		off, err := safeConvertUint64ToInt(offset)
		if err != nil {
			return err
		}
		n, err = v.ec.Read(ctx, inode, tmp, off, readSize)
		if err != nil && err != io.EOF {
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

func (v *Volume) ReadFile(ctx context.Context, path string, writer io.Writer, offset, size uint64) error {
	span := spanWithOperation(ctx, "ReadFile")

	_, ino, _, mode, err := v.recursiveLookupTarget(ctx, path, false)
	if err != nil {
		span.Errorf("recursiveLookupTarget fail: volume(%v) path(%v) err(%v)", v.name, path, err)
		return err
	}

	if mode.IsDir() {
		return nil
	}
	// read file data
	var inoInfo *proto.InodeInfo
	if inoInfo, err = v.mw.InodeGet_ll(ino); err != nil {
		span.Errorf("meta InodeGet_ll fail: volume(%v) inode(%v) err(%v)", v.name, ino, err)
		return err
	}
	if err = v.readFile(ctx, ino, inoInfo.Size, path, writer, offset, size); err != nil {
		span.Errorf("readFile fail: volume(%v) path(%v) inode(%v) fsize(%v) offset(%v) size(%v) err(%v)",
			v.name, path, ino, inoInfo.Size, offset, size, err)
	}

	return err
}

func (v *Volume) ObjectMeta(ctx context.Context, path string) (info *FSFileInfo, xattr *proto.XAttrInfo, err error) {
	span := spanWithOperation(ctx, "ObjectMeta")

	// process path
	var inode uint64
	var mode os.FileMode
	var inoInfo *proto.InodeInfo
	retry := 0
	var notUseCache bool
	for {
		if _, inode, _, mode, err = v.recursiveLookupTarget(ctx, path, notUseCache); err != nil {
			span.Errorf("recursiveLookupTarget fail: volume(%v) path(%v) notUseCache(%v) err(%v)",
				v.name, path, notUseCache, err)
			return
		}
		inoInfo, err = v.mw.InodeGet_ll(inode)
		if err == syscall.ENOENT && retry < MaxRetry {
			notUseCache = true
			retry++
			continue
		}
		if err != nil {
			span.Errorf("meta InodeGet_ll fail: volume(%v) inode(%v) retry(%v) err(%v)",
				v.name, inode, retry, err)
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
			xattr, err = v.mw.XAttrGetAll_ll(inode)
			if err != nil {
				span.Errorf("meta XAttrGetAll_ll fail: volume(%v) inode(%v) err(%v)",
					v.name, inode, err)
				return
			}
			attrItem = &AttrItem{
				XAttrInfo: *xattr,
			}
			objMetaCache.PutAttr(v.name, attrItem)
		} else {
			xattr = &proto.XAttrInfo{
				XAttrs: attrItem.XAttrs,
				Inode:  attrItem.Inode,
			}
		}
	} else {
		xattr, err = v.mw.XAttrGetAll_ll(inode)
		if err != nil {
			span.Errorf("meta XAttrGetAll_ll fail: volume(%v) inode(%v) err(%v)",
				v.name, inode, err)
			return
		}
	}

	if mode.IsDir() {
		// Folder has specific ETag and MIME type.
		etagValue = DirectoryETagValue()
		mimeType = ValueContentTypeDirectory
	} else {
		// Try to get the advanced attributes stored in the extended attributes.
		// The following advanced attributes apply to the object storage:
		// 1. Etag (MD5)
		// 2. MIME type
		mimeType = string(xattr.Get(XAttrKeyOSSMIME))
		disposition = string(xattr.Get(XAttrKeyOSSDISPOSITION))
		cacheControl = string(xattr.Get(XAttrKeyOSSCacheControl))
		expires = string(xattr.Get(XAttrKeyOSSExpires))
		rawETag := string(xattr.Get(XAttrKeyOSSETag))
		if len(rawETag) == 0 {
			rawETag = string(xattr.Get(XAttrKeyOSSETagDeprecated))
		}
		if len(rawETag) > 0 {
			etagValue = ParseETagValue(rawETag)
		}
	}
	// Load user-defined metadata
	var retainUntilDate string
	var retainUntilDateInt64 int64
	metadata := make(map[string]string, 0)
	for key, val := range xattr.XAttrs {
		if !strings.HasPrefix(key, XAttrKeyOSSPrefix) {
			metadata[key] = val
		}
		if key == XAttrKeyOSSLock {
			retainUntilDateInt64, err = strconv.ParseInt(val, 10, 64)
			if err != nil {
				span.Errorf("parse save retainUntilDate fail: volume(%v) path(%v) retainUntilDate(%v) err(%v)",
					v.Name(), path, val, err)
				return
			}
			retainUntilDate = time.Unix(0, retainUntilDateInt64).UTC().Format(ISO8601Layout)
		}
	}

	// Validating ETag value.
	if !mode.IsDir() && (!etagValue.Valid() || etagValue.TS.Before(inoInfo.ModifyTime)) {
		span.Warnf("etag invalid or before inode modTime: volume(%v) path(%v) inoInfo(%v) etagVal(%v)",
			v.name, path, inoInfo, etagValue)
	}

	info = &FSFileInfo{
		Path:            path,
		Size:            int64(inoInfo.Size),
		Mode:            os.FileMode(inoInfo.Mode),
		CreateTime:      inoInfo.CreateTime,
		ModifyTime:      inoInfo.ModifyTime,
		ETag:            etagValue.ETag(),
		Inode:           inoInfo.Inode,
		MIMEType:        mimeType,
		Disposition:     disposition,
		CacheControl:    cacheControl,
		Expires:         expires,
		Metadata:        metadata,
		RetainUntilDate: retainUntilDate,
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
//
//	0x2 ENOENT No such file or directory. A component of a specified
//	pathname did not exist, or the pathname was an empty string.
func (v *Volume) recursiveLookupTarget(ctx context.Context, path string, notUseCache bool) (parent, ino uint64,
	name string, mode os.FileMode, err error) {
	parent = rootIno
	pathIterator := NewPathIterator(path)
	if !pathIterator.HasNext() {
		err = syscall.ENOENT
		return
	}

	cacheUsed := false

	if objMetaCache != nil && !notUseCache {
		for pathIterator.HasNext() {
			pathItem := pathIterator.Next()
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
			// cache not found or cache expired or filemode not match
			if dentry == nil || needRefresh || os.FileMode(dentry.Type).IsDir() != pathItem.IsDirectory {
				curIno, curMode, err = v.mw.Lookup_ll(parent, pathItem.Name)
				if err != nil {
					if !cacheUsed {
						return
					}
					break
				}
				// force updating dentry in cache
				updateDentryCache(parent, curIno, curMode, pathItem.Name, v.name)
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
		// no error occurs in recursiveLookup with cache
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
		pathItem := pathIterator.Next()
		var curIno uint64
		var curMode uint32
		curIno, curMode, err = v.mw.Lookup_ll(parent, pathItem.Name)
		if err != nil && err != syscall.ENOENT {
			return
		}
		if err == syscall.ENOENT {
			deleteDentryCache(parent, pathItem.Name, v.name)
			return
		}
		// force updating dentry in cache
		updateDentryCache(parent, curIno, curMode, pathItem.Name, v.name)
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

func updateDentryCache(parentId, ino uint64, curMode uint32, dentryName, volName string) {
	if objMetaCache != nil {
		dentry := &DentryItem{
			Dentry: metanode.Dentry{
				ParentId: parentId,
				Name:     dentryName,
				Inode:    ino,
				Type:     curMode,
			},
		}
		objMetaCache.PutDentry(volName, dentry)
	}
}

func deleteDentryCache(parent uint64, dentryName, volName string) {
	if objMetaCache != nil {
		dentry := &DentryItem{
			Dentry: metanode.Dentry{
				ParentId: parent,
				Name:     dentryName,
			},
		}
		objMetaCache.DeleteDentry(volName, dentry.Key())
	}
}

func putAttrCache(attr *AttrItem, volName string) {
	if objMetaCache != nil {
		objMetaCache.PutAttr(volName, attr)
	}
}

func updateAttrCache(inode uint64, key, value, volName string) {
	if objMetaCache != nil {
		attrItem := &AttrItem{
			XAttrInfo: proto.XAttrInfo{
				Inode:  inode,
				XAttrs: make(map[string]string, 0),
			},
		}
		attrItem.XAttrs[key] = value
		objMetaCache.MergeAttr(volName, attrItem)
	}
}

func deleteAttrCache(inode uint64, volName string) {
	if objMetaCache != nil {
		objMetaCache.DeleteAttr(volName, inode)
	}
}

func (v *Volume) recursiveMakeDirectory(ctx context.Context, path string) (partentIno uint64, err error) {
	// in case of any mv or rename operation within refresh interval of dentry item in cache,
	// recursiveMakeDirectory don't look up cache, and will force update dentry item
	partentIno = rootIno
	pathIterator := NewPathIterator(path)
	if !pathIterator.HasNext() {
		err = syscall.ENOENT
		return
	}
	for pathIterator.HasNext() {
		pathItem := pathIterator.Next()
		if !pathItem.IsDirectory {
			break
		}
		var curIno uint64
		var curMode uint32
		curIno, curMode, err = v.mw.Lookup_ll(partentIno, pathItem.Name)
		if err != nil && err != syscall.ENOENT {
			return
		}
		if err == syscall.ENOENT {
			var info *proto.InodeInfo
			info, err = v.mw.Create_ll(partentIno, pathItem.Name, uint32(DefaultDirMode), 0, 0, nil, path[:pathIterator.cursor])
			if err != nil {
				if err == syscall.EEXIST {
					existInode, mode, e := v.mw.Lookup_ll(partentIno, pathItem.Name)
					if e != nil {
						return
					}
					if os.FileMode(mode).IsDir() {
						partentIno, err = existInode, nil
						continue
					}
				}
				return
			}
			curIno, curMode = info.Inode, info.Mode
		}

		// force updating dentry in cache
		updateDentryCache(partentIno, curIno, curMode, pathItem.Name, v.name)

		// Check file mode
		if os.FileMode(curMode).IsDir() != pathItem.IsDirectory {
			err = syscall.EINVAL
			return
		}
		partentIno = curIno
	}
	return
}

// Deprecated
func (v *Volume) lookupDirectories(ctx context.Context, dirs []string, autoCreate bool) (uint64, error) {
	parentId := rootIno
	// check and create dirs
	for _, dir := range dirs {
		curIno, curMode, err := v.mw.Lookup_ll(parentId, dir)
		if err != nil {
			if err != syscall.ENOENT || !autoCreate {
				return 0, err
			}
		}
		// this item is not exist
		if err == syscall.ENOENT {
			var inodeInfo *proto.InodeInfo
			inodeInfo, err = v.mw.Create_ll(parentId, dir, uint32(DefaultDirMode), 0, 0, nil, "/"+dir)
			if err != nil && err != syscall.EEXIST {
				return 0, err
			}
			// retry lookup if it exists.
			if err == syscall.EEXIST {
				curIno, curMode, err = v.mw.Lookup_ll(parentId, dir)
				if err != nil {
					return 0, err
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

	return parentId, nil
}

func (v *Volume) listFilesV1(
	ctx context.Context,
	prefix, marker, delimiter string,
	maxKeys uint64,
	onlyObject bool,
) (infos []*FSFileInfo, prefixes Prefixes, nextMarker string, err error) {
	parentId, dirs, err := v.findParentId(ctx, prefix)
	// The method returns an ENOENT error, indicating that there
	// are no files or directories matching the prefix.
	if err != nil {
		if err == syscall.ENOENT {
			err = nil
		}
		return
	}

	// Init the value that queried result count.
	// Check this value when adding key to contents or common prefix,
	// return if it reach to max keys
	var rc uint64

	// recursion scan
	prefixMap := PrefixMap(make(map[string]struct{}))
	infos, prefixMap, nextMarker, _, err = v.recursiveScan(ctx, infos, prefixMap, parentId, maxKeys, maxKeys, rc, dirs,
		prefix, marker, delimiter, onlyObject, true)
	if err != nil {
		return
	}

	// Supplementary file information, such as file modification time, MIME type, Etag information, etc.
	if err = v.supplyListFileInfo(ctx, infos); err != nil {
		return
	}

	prefixes = prefixMap.Prefixes()

	return
}

func (v *Volume) listFilesV2(
	ctx context.Context,
	prefix, startAfter, contToken, delimiter string,
	maxKeys uint64,
) (infos []*FSFileInfo, prefixes Prefixes, nextMarker string, err error) {
	prefixMap := PrefixMap(make(map[string]struct{}))

	var marker string
	if startAfter != "" {
		marker = startAfter
	}
	if contToken != "" {
		marker = contToken
	}
	parentId, dirs, err := v.findParentId(ctx, prefix)
	if err != nil {
		// The method returns an ENOENT error, indicating that there
		// are no files or directories matching the prefix.
		if err == syscall.ENOENT {
			err = nil
		}
		return
	}

	// Init the value that queried result count.
	// Check this value when adding key to contents or common prefix,
	// return if it reach to max keys
	var rc uint64
	// recursion scan
	infos, prefixMap, nextMarker, _, err = v.recursiveScan(ctx, infos, prefixMap, parentId, maxKeys, maxKeys, rc, dirs,
		prefix, marker, delimiter, true, true)
	if err != nil {
		return
	}

	// Supplementary file information, such as file modification time, MIME type, Etag information, etc.
	if err = v.supplyListFileInfo(ctx, infos); err != nil {
		return
	}

	prefixes = prefixMap.Prefixes()

	return
}

func (v *Volume) findParentId(ctx context.Context, prefix string) (inode uint64, prefixDirs []string, err error) {
	span := spanWithOperation(ctx, "findParentId")
	prefixDirs = make([]string, 0)

	// if prefix and marker are both not empty, use marker
	var dirs []string
	if prefix != "" {
		dirs = strings.Split(prefix, "/")
	}
	if len(dirs) <= 1 {
		inode = proto.RootIno
		return
	}

	var curIno uint64
	var curMode uint32
	parentId := proto.RootIno
	for index, dir := range dirs {
		// Because lookup can only retrieve dentry whose name exactly matches,
		// so do not lookup the last part.
		if index+1 == len(dirs) {
			break
		}

		// If the part except the last part does not match exactly the same dentry, there is
		// no path matching the path prefix. An ENOENT error is returned to the caller.
		if curIno, curMode, err = v.mw.Lookup_ll(parentId, dir); err != nil {
			span.Errorf("meta Lookup_ll fail: volume(%v) prefix(%v) parentID(%v) name(%v) err(%v)",
				v.name, prefix, parentId, dir, err)
			return
		}

		// Because the file cannot have the next level members,
		// if there is a directory in the middle of the prefix,
		// it means that there is no file matching the prefix.
		if !os.FileMode(curMode).IsDir() {
			err = syscall.ENOENT
			return
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
func (v *Volume) recursiveScan(
	ctx context.Context,
	fileInfos []*FSFileInfo,
	prefixMap PrefixMap,
	parentId, maxKeys,
	readLimit, rc uint64,
	dirs []string,
	prefix, marker, delimiter string,
	onlyObject, firstEnter bool,
) ([]*FSFileInfo, PrefixMap, string, uint64, error) {
	span := spanWithOperation(ctx, "recursiveScan")

	currentPath := strings.Join(dirs, pathSep) + pathSep
	if strings.HasPrefix(currentPath, pathSep) {
		currentPath = strings.TrimPrefix(currentPath, pathSep)
	}

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
	var lastKey, nextMarker string

readDir:
	children, err := v.mw.ReadDirLimit_ll(parentId, fromName, readLimit+1) // one more for nextMarker
	if err != nil {
		span.Errorf("meta ReadDirLimit_ll fail: volume(%v) parentID(%v) fromName(%v) limit(%v) err(%v)",
			v.name, parentId, fromName, readLimit+1, err)
		if err == syscall.ENOENT {
			err = nil
		}
		return fileInfos, prefixMap, "", 0, err
	}

	for _, child := range children {
		if child.Name == lastKey {
			continue
		}
		path := strings.Join(append(dirs, child.Name), pathSep)
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
				fileInfos, prefixMap, nextMarker, rc, err = v.recursiveScan(ctx, fileInfos, prefixMap, child.Inode,
					maxKeys, readLimit, rc, append(dirs, child.Name), prefix, marker, delimiter, onlyObject, false)
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
			nonPrefixPart := strings.Replace(path, prefix, "", 1)
			if idx := strings.Index(nonPrefixPart, delimiter); idx >= 0 {
				commonPrefix := prefix + util.SubString(nonPrefixPart, 0, idx) + delimiter
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
			fileInfos, prefixMap, nextMarker, rc, err = v.recursiveScan(ctx, fileInfos, prefixMap, child.Inode, maxKeys,
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
			goto readDir
		}
	}

	return fileInfos, prefixMap, nextMarker, rc, nil
}

// This method is used to supplement file metadata. Supplement the specified file
// information with Size, ModifyTIme, Mode, Etag, and MIME type information.
func (v *Volume) supplyListFileInfo(ctx context.Context, fileInfos []*FSFileInfo) error {
	span := spanWithOperation(ctx, "supplyListFileInfo")

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
		span.Errorf("meta BatchGetXAttr fail: volume(%v) inodes(%v) keys(%v) err(%v)",
			v.name, inodes, strings.Join(keys, ","), err)
		return err
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
			xattr := xattrs[i]
			rawETag := string(xattr.Get(XAttrKeyOSSETag))
			if len(rawETag) == 0 {
				rawETag = string(xattr.Get(XAttrKeyOSSETagDeprecated))
			}
			if len(rawETag) > 0 {
				etagValue = ParseETagValue(rawETag)
			}
		}
		if !etagValue.Valid() || etagValue.TS.Before(fileInfo.ModifyTime) {
			// The ETag is invalid or outdated then generate a new ETag and make update.
			if etagValue, err = v.updateETag(ctx, fileInfo.Inode, fileInfo.Size, fileInfo.ModifyTime); err != nil {
				span.Errorf("update etag fail: volume(%v) inode(%v) size(%v) modTime(%v) err(%v)",
					v.name, fileInfo.Inode, fileInfo.Size, fileInfo.ModifyTime, err)
				return err
			}
		}
		fileInfo.ETag = etagValue.ETag()
	}

	return nil
}

func (v *Volume) updateETag(ctx context.Context, inode uint64, size int64, mt time.Time) (etagValue ETagValue, err error) {
	// The ETag is invalid or outdated then generate a new ETag and make update.
	if size == 0 {
		etagValue = EmptyContentETagValue(mt)
	} else {
		splittedRanges := SplitFileRange(size, SplitFileRangeBlockSize)
		etagValue = NewRandomUUIDETagValue(len(splittedRanges), mt)
	}
	if err = v.mw.XAttrSet_ll(inode, []byte(XAttrKeyOSSETag), []byte(etagValue.Encode())); err != nil {
		return
	}
	return
}

func (v *Volume) ListMultipartUploads(
	ctx context.Context,
	prefix, delimiter, keyMarker, multipartIdMarker string,
	maxUploads uint64,
) (uploads []*FSUpload, nextMarker, nextMultipartIdMarker string, isTruncated bool, prefixes []string, err error) {
	span := spanWithOperation(ctx, "ListMultipartUploads")

	sessions, err := v.mw.ListMultipart_ll(prefix, delimiter, keyMarker, multipartIdMarker, maxUploads)
	if err != nil || len(sessions) == 0 {
		span.Errorf("meta ListMultipart_ll fail: volume(%v) prefix(%v) delimiter(%v) keyMarker(%v) "+
			"multipartIdMarker(%v) maxUploads(%v) err(%v)",
			v.name, prefix, delimiter, keyMarker, multipartIdMarker, maxUploads, err)
		return
	}

	uploads = make([]*FSUpload, 0)
	prefixes = make([]string, 0)
	prefixMap := make(map[string]interface{})

	var count uint64
	var lastUpload *proto.MultipartInfo
	for _, session := range sessions {
		tempKey := session.Path
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

func (v *Volume) ListParts(
	ctx context.Context,
	path, uploadId string,
	maxParts, partNumberMarker uint64,
) (parts []*FSPart, nextMarker uint64, isTruncated bool, err error) {
	span := spanWithOperation(ctx, "ListParts")

	multipartInfo, err := v.mw.GetMultipart_ll(path, uploadId)
	if err != nil {
		span.Errorf("meta GetMultipart_ll fail: volume(%v) path(%v) multipartId(%v) err(%v)",
			v.name, path, uploadId, err)
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

	return
}

func (v *Volume) CopyFile(
	ctx context.Context,
	sv *Volume,
	sourcePath, targetPath, metaDirective string,
	opt *PutFileOption,
) (info *FSFileInfo, err error) {
	span := spanWithOperation(ctx, "CopyFile")

	// operation at source object
	var (
		sInode     uint64
		sName      string
		sMode      os.FileMode
		sInodeInfo *proto.InodeInfo
	)

	if _, sInode, sName, sMode, err = sv.recursiveLookupTarget(ctx, sourcePath, false); err != nil {
		span.Errorf("recursiveLookupTarget fail: volume(%v) path(%v) err(%v)", v.name, sourcePath, err)
		return
	}
	if sInodeInfo, err = sv.mw.InodeGet_ll(sInode); err != nil {
		span.Errorf("meta InodeGet_ll fail: volume(%v) inode(%v) err(%v)", v.name, sInode, err)
		return
	}
	if sInodeInfo.Size > MaxCopyObjectSize {
		return nil, syscall.EFBIG
	}
	if err = sv.ec.OpenStream(ctx, sInode); err != nil {
		span.Errorf("data OpenStream fail: volume(%v) inode(%v) err(%v)", v.name, sInode, err)
		return
	}
	defer func() {
		if closeErr := sv.ec.CloseStream(sInode); closeErr != nil {
			span.Errorf("data CloseStream fail: volume(%v) inode(%v) err(%v)", v.name, sInode, closeErr)
		}
	}()

	var xattr *proto.XAttrInfo
	// if source path is same with target path, just reset file metadata
	// source path is same with target path, and metadata directive is not 'REPLACE', objectNode does nothing
	if targetPath == sourcePath && v.name == sv.name {
		if metaDirective == MetadataDirectiveReplace {
			// check whether target object is protected by object lock
			if opt != nil && opt.ObjectLock != nil {
				err = isObjectLocked(ctx, v, sInode, sName, sourcePath)
				if err != nil {
					span.Errorf("check objectLock protected: volume(%v) name(%v) inode(%v) err(%v)",
						v.name, sName, sInode, err)
					return
				}
			}
			// replace system metadata : 'Content-Type' and 'Content-Disposition', if user specified, replace user defined metadata
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
				attr.XAttrs[XAttrKeyOSSACL] = opt.ACL.Encode()
			}
			if opt != nil && opt.ObjectLock != nil && opt.ObjectLock.ToRetention() != nil {
				attr.XAttrs[XAttrKeyOSSLock] = formatRetentionDateStr(time.Now(), opt.ObjectLock.ToRetention())
			}
			// If user-defined metadata have been specified, use extend attributes for storage.
			if opt != nil && len(opt.Metadata) > 0 {
				for name, value := range opt.Metadata {
					attr.XAttrs[name] = value
				}
			}
			if err = v.mw.BatchSetXAttr_ll(sInode, attr.XAttrs); err != nil {
				span.Errorf("meta BatchSetXAttr_ll fail: volume(%v) inode(%v) attrs(%v) err(%v)",
					sv.name, sInode, attr.XAttrs, err)
				return nil, err
			}
			// merge attrs in cache
			if objMetaCache != nil {
				objMetaCache.MergeAttr(v.name, attr)
			}
		}
		info, xattr, err = sv.ObjectMeta(ctx, sourcePath)

		return
	}

	// operation at target object
	var (
		tMode      os.FileMode
		oldtInode  uint64
		tInodeInfo *proto.InodeInfo
		tParentId  uint64
		pathItems  []PathItem
		tLastName  string
	)

	_, oldtInode, _, tMode, err = v.recursiveLookupTarget(ctx, targetPath, false)
	if err != nil && err != syscall.ENOENT {
		span.Errorf("recursiveLookupTarget fail: volume(%v) path(%v) err(%v)", v.name, targetPath, err)
		return
	}
	// if target file existed, check target file mode is whether same with source file
	if err != syscall.ENOENT && tMode.IsDir() != sMode.IsDir() {
		span.Errorf("target path existed but mode not same with source: volume(%v) targetPath(%v) targetInode("+
			"%v) sourcePath(%v) sourceInode(%v)",
			v.name, targetPath, oldtInode, sourcePath, sInode)
		err = syscall.EINVAL
		return
	}
	// if source file mode is directory, return OK, and need't create target directory
	if sMode == DefaultDirMode {
		// create target directory
		if !strings.HasSuffix(targetPath, pathSep) {
			targetPath += pathSep
		}
		if tParentId, err = v.recursiveMakeDirectory(ctx, targetPath); err != nil {
			span.Errorf("recursiveMakeDirectory fail: volume(%v) path(%v) err(%v)", v.name, targetPath, err)
			return
		}
		if tInodeInfo, err = v.mw.InodeGet_ll(tParentId); err != nil {
			span.Errorf("meta InodeGet_ll fail: volume(%v) inode(%v) err(%v)", v.name, tParentId, err)
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
			MIMEType:   ValueContentTypeDirectory,
		}
		return
	}
	// recursive create target directory, and get parent id and last name
	if tParentId, err = v.recursiveMakeDirectory(ctx, targetPath); err != nil {
		span.Errorf("recursiveMakeDirectory fail: volume(%v) path(%v) err(%v)", v.name, targetPath, err)
		return
	}
	pathItems = NewPathIterator(targetPath).ToSlice()
	if len(pathItems) <= 0 {
		span.Errorf("get target path pathItems is empty: volume(%v) path(%v)", v.name, targetPath)
		err = syscall.EINVAL
		return
	}
	tLastName = pathItems[len(pathItems)-1].Name
	// check whether existing object is protected by object lock
	if oldtInode != 0 && opt != nil && opt.ObjectLock != nil {
		err = isObjectLocked(ctx, v, oldtInode, tLastName, targetPath)
		if err != nil {
			span.Errorf("check objectLock protected: volume(%v) name(%v) inode(%v) err(%v)",
				v.name, tLastName, oldtInode, err)
			return
		}
	}

	// create target file inode and set target inode to be source file inode
	tInodeInfo, err = v.mw.InodeCreate_ll(tParentId, uint32(sMode), 0, 0, nil, make([]uint64, 0), targetPath)
	if err != nil {
		span.Errorf("meta InodeCreate_ll fail: volume(%v) parentID(%v) mode(%v) err(%v)",
			v.name, tParentId, sMode, err)
		return
	}
	defer func() {
		// An error has caused the entire process to fail. Delete the inode and release the written data.
		if err != nil {
			span.Warnf("unlink target temp inode: volume(%v) path(%v) inode(%v) ",
				v.name, targetPath, tInodeInfo.Inode)
			_, _ = v.mw.InodeUnlink_ll(tInodeInfo.Inode, targetPath)
			span.Warnf("evict target temp inode: volume(%v) path(%v) inode(%v)",
				v.name, targetPath, tInodeInfo.Inode)
			_ = v.mw.Evict(tInodeInfo.Inode, targetPath)
		}
	}()

	if err = v.ec.OpenStream(ctx, tInodeInfo.Inode); err != nil {
		span.Errorf("data OpenStream fail: volume(%v) inode(%v) err(%v)", v.name, tInodeInfo.Inode, err)
		return
	}
	defer func() {
		if closeErr := v.ec.CloseStream(tInodeInfo.Inode); closeErr != nil {
			span.Errorf("data CloseStream fail: volume(%v) inode(%v) err(%v)",
				v.name, tInodeInfo.Inode, closeErr)
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

	var ebsReader *blobstore.Reader
	var ebsWriter *blobstore.Writer
	if proto.IsCold(sv.volType) {
		ebsReader = v.getEbsReader(sInode)
	}
	if proto.IsCold(v.volType) {
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
			readN, err = ebsReader.Read(ctx, buf, readOffset, readSize)
		} else {
			readN, err = sv.ec.Read(ctx, sInode, buf, readOffset, readSize)
		}
		if err != nil && err != io.EOF {
			span.Errorf("read source path fail: volume(%v) inode(%v) offset(%v) size(%v) err(%v)",
				sv.name, sInode, readOffset, readSize, err)
			return
		}
		if readN > 0 {
			if proto.IsCold(v.volType) {
				writeN, err = ebsWriter.WriteWithoutPool(ctx, writeOffset, buf[:readN])
			} else {
				writeN, err = v.ec.Write(ctx, tInodeInfo.Inode, writeOffset, buf[:readN], 0, nil)
			}
			if err != nil {
				span.Errorf("write target path from source fail: volume(%v) inode(%v) offset(%v) err(%v)",
					v.name, tInodeInfo.Inode, writeOffset, err)
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
		err = ebsWriter.FlushWithoutPool(tInodeInfo.Inode, ctx)
	} else {
		v.ec.Flush(ctx, tInodeInfo.Inode)
	}
	if err != nil {
		span.Errorf("data Flush fail: volume(%v) inode(%v) err(%v)", v.name, tInodeInfo.Inode, err)
		return
	}

	var finalInode *proto.InodeInfo
	if finalInode, err = v.mw.InodeGet_ll(tInodeInfo.Inode); err != nil {
		span.Errorf("meta InodeGet_ll fail: volume(%v) inode(%v) err(%v)", v.name, tInodeInfo.Inode, err)
		return
	}

	md5Value = hex.EncodeToString(md5Hash.Sum(nil))
	etagValue := ETagValue{
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
			span.Errorf("meta XAttrGetAll_ll fail: volume(%v) inode(%v) err(%v)", v.name, sInode, err)
			return
		}
		for key, val := range xattr.XAttrs {
			if key == XAttrKeyOSSETag {
				continue
			}
			targetAttr.XAttrs[key] = val
		}
	} else {
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
		// If user-defined metadata have been specified, use extend attributes for storage.
		if opt != nil && len(opt.Metadata) > 0 {
			for name, value := range opt.Metadata {
				targetAttr.XAttrs[name] = value
			}
		}
	}
	if opt != nil && opt.ACL != nil {
		targetAttr.XAttrs[XAttrKeyOSSACL] = opt.ACL.Encode()
	}
	if opt != nil && opt.ObjectLock != nil && opt.ObjectLock.ToRetention() != nil {
		targetAttr.XAttrs[XAttrKeyOSSLock] = formatRetentionDateStr(tInodeInfo.ModifyTime, opt.ObjectLock.ToRetention())
	}
	if err = v.mw.BatchSetXAttr_ll(tInodeInfo.Inode, targetAttr.XAttrs); err != nil {
		span.Errorf("meta BatchSetXAttr_ll fail: volume(%v) inode(%v) attrs(%v) err(%v)",
			v.name, tInodeInfo.Inode, targetAttr.XAttrs, err)
		return
	}
	// merge attrs in cache
	if objMetaCache != nil {
		objMetaCache.PutAttr(v.name, targetAttr)
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
	err = v.applyInodeToDEntry(ctx, tParentId, tLastName, tInodeInfo.Inode, false, targetPath)
	if err != nil {
		span.Errorf("applyInodeToDEntry fail: volume(%v) parentID(%v) name(%v) inode(%v) err(%v)",
			v.name, tParentId, tLastName, tInodeInfo.Inode, err)
		return
	}

	// force updating dentry and attrs in cache
	updateDentryCache(tParentId, tInodeInfo.Inode, DefaultFileMode, tLastName, v.name)
	putAttrCache(targetAttr, v.name)

	return
}

func (v *Volume) copyFile(
	ctx context.Context,
	parentID uint64,
	newFileName string,
	sourceFileInode uint64,
	mode uint32,
	newPath string,
	sourcePath string,
) (*proto.InodeInfo, error) {
	if err := v.mw.DentryCreate_ll(parentID, newFileName, sourceFileInode, mode, newPath); err != nil {
		return nil, err
	}

	return v.mw.InodeLink_ll(sourceFileInode, sourcePath)
}

func NewVolume(ctx context.Context, config *VolumeConfig) (*Volume, error) {
	span := spanWithOperation(ctx, "NewVolume")

	metaConfig := &meta.MetaConfig{
		Volume:        config.Volume,
		Masters:       config.Masters,
		Authenticate:  false,
		ValidateOwner: false,
		OnAsyncTaskError: func(err error) {
			config.OnAsyncTaskError.OnError(err)
		},
	}
	metaWrapper, err := meta.NewMetaWrapper(metaConfig)
	if err != nil {
		span.Errorf("meta NewMetaWrapper fail: config(%+v) err(%v)", metaConfig, err)
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = metaWrapper.Close()
		}
	}()

	mc := master.NewMasterClient(config.Masters, false)
	volumeInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(ctx, config.Volume)
	if err != nil {
		span.Errorf("get volume info from master fail: volume(%v) err(%v)", config.Volume, err)
		return nil, err
	}
	if volumeInfo.Status == 1 {
		span.Warnf("volume has been marked for deletion: volume(%v) status(%v - 0:normal/1:markDelete)",
			config.Volume, volumeInfo.Status)
		return nil, proto.ErrVolNotExists
	}

	extentConfig := &stream.ExtentConfig{
		Volume:            config.Volume,
		Masters:           config.Masters,
		FollowerRead:      true,
		OnAppendExtentKey: metaWrapper.AppendExtentKey,
		OnSplitExtentKey:  metaWrapper.SplitExtentKey,
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
	}
	var extentClient *stream.ExtentClient
	if extentClient, err = stream.NewExtentClient(ctx, extentConfig); err != nil {
		span.Errorf("data NewExtentClient fail: config(%+v) err(%v)", extentConfig, err)
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
		v.metaLoader = &cacheMetaLoader{
			om:     new(OSSMeta),
			sml:    &strictMetaLoader{v: v},
			synced: new(int32),
		}
		go v.syncOSSMeta()
	}

	return v, nil
}

func (v *Volume) getEbsWriter(ino uint64) *blobstore.Writer {
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

	return blobstore.NewWriter(clientConf)
}

func (v *Volume) getEbsReader(ino uint64) *blobstore.Reader {
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

	return blobstore.NewReader(clientConf)
}

func safeConvertUint64ToInt(num uint64) (int, error) {
	str := strconv.FormatUint(num, 10)
	parsed, err := strconv.ParseInt(str, 10, 0)
	if err != nil {
		return 0, err
	}
	return int(parsed), nil
}

func safeConvertInt64ToUint64(num int64) (uint64, error) {
	str := strconv.FormatInt(num, 10)
	parsed, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func safeConvertStrToUint16(str string) (uint16, error) {
	parsed, err := strconv.ParseUint(str, 10, 16)
	if err != nil {
		return 0, err
	}
	return uint16(parsed), nil
}

func (v *Volume) referenceExtentKey(ctx context.Context, oldInode, inode uint64) (bool, error) {
	// cold volume
	if proto.IsCold(v.volType) {
		_, _, _, oldObjExtents, err := v.mw.GetObjExtents(oldInode)
		if err != nil {
			return false, err
		}
		_, _, _, objExtents, err := v.mw.GetObjExtents(inode)
		if err != nil {
			return false, err
		}
		if reflect.DeepEqual(oldObjExtents, objExtents) {
			return true, nil
		}
		return false, nil

	}
	// hot volume
	_, _, oldExtents, err := v.mw.GetExtents(oldInode)
	if err != nil {
		return false, err
	}
	_, _, extents, err := v.mw.GetExtents(inode)
	if err != nil {
		return false, err
	}
	if reflect.DeepEqual(oldExtents, extents) {
		return true, nil
	}
	return false, nil
}
