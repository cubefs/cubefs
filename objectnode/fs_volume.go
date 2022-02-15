// Copyright 2019 The ChubaoFS Authors.
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
	"github.com/chubaofs/chubaofs/util/errors"
	"hash"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	rootIno               = proto.RootIno
	OSSMetaUpdateDuration = time.Duration(time.Second * 30)
)

// AsyncTaskErrorFunc is a callback method definition for asynchronous tasks when an error occurs.
// It is mainly used to notify other objects when an error occurs during asynchronous task execution.
// These asynchronous tasks include periodic volume topology and metadata update tasks.
type AsyncTaskErrorFunc func(err error)

// OnError protects the call of AsyncTaskErrorFunc with null pointer access. Used to simplify caller code.
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
	Metadata     map[string]string
	CacheControl string
	Expires      string
}

type ListFilesV1Option struct {
	Prefix    string
	Delimiter string
	Marker    string
	MaxKeys   uint64
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
	ec         *data.ExtentClient
	store      Store // Storage for ACP management
	name       string
	metaLoader ossMetaLoader
	ticker     *time.Ticker
	createTime int64

	closeOnce sync.Once
	closeCh   chan struct{}

	onAsyncTaskError AsyncTaskErrorFunc
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
	if err = json.Unmarshal(raw, configuration); err != nil {
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
		inode, lookupMode, err = v.mw.Lookup_ll(context.Background(), parentId, filename)
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
		if inodeInfo, err = v.mw.Create_ll(context.Background(), parentID, filename, DefaultFileMode, 0, 0, nil); err != nil {
			return err
		}
		inode = inodeInfo.Inode
	}
	return v.mw.XAttrSet_ll(context.Background(), inode, []byte(key), data)
}

func (v *Volume) GetXAttr(path string, key string) (info *proto.XAttrInfo, err error) {
	var inode uint64
	inode, err = v.getInodeFromPath(path)
	if err != nil {
		return
	}
	if info, err = v.mw.XAttrGet_ll(context.Background(), inode, key); err != nil {
		log.LogErrorf("GetXAttr: meta get xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
		return
	}
	return
}

func (v *Volume) DeleteXAttr(path string, key string) (err error) {
	inode, err1 := v.getInodeFromPath(path)
	if err1 != nil {
		err = err1
		return
	}
	if err = v.mw.XAttrDel_ll(context.Background(), inode, key); err != nil {
		log.LogErrorf("SetXAttr: meta set xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
		return
	}
	return
}

func (v *Volume) ListXAttrs(path string) (keys []string, err error) {
	var inode uint64
	inode, err = v.getInodeFromPath(path)
	if err != nil {
		return
	}
	if keys, err = v.mw.XAttrsList_ll(context.Background(), inode); err != nil {
		log.LogErrorf("GetXAttr: meta get xattr fail: volume(%v) path(%v) inode(%v) err(%v)", v.name, path, inode, err)
		return
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

	var infos []*FSFileInfo
	var prefixes Prefixes
	var nextMarker string

	infos, prefixes, nextMarker, err = v.listFilesV1(prefix, marker, delimiter, maxKeys)
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

// WriteObject creates or updates target path objects and data.
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
		if info, err = v.mw.InodeGet_ll(context.Background(), parentId); err != nil {
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
	_, lookupMode, err = v.mw.Lookup_ll(context.Background(), parentId, lastPathItem.Name)
	if err != nil && err != syscall.ENOENT {
		log.LogErrorf("PutObject: look up target failed: volume(%v) path(%v)", v.name, path)
		return
	}
	if err == nil && os.FileMode(lookupMode).IsDir() {
		log.LogWarnf("PutObject: target exist but mode conflict: volume(%v) path(%v)", v.name, path)
		err = syscall.EINVAL
		return
	}

	// Intermediate data during the writing of new versions is managed through invisible files.
	// This file has only inode but no dentry. In this way, this temporary file can be made invisible
	// in the true sense. In order to avoid the adverse impact of other user operations on temporary data.
	var invisibleTempDataInode *proto.InodeInfo
	if invisibleTempDataInode, err = v.mw.InodeCreate_ll(context.Background(), DefaultFileMode, 0, 0, nil); err != nil {
		return
	}
	defer func() {
		// An error has caused the entire process to fail. Delete the inode and release the written data.
		if err != nil {
			log.LogWarnf("PutObject: unlink temp inode: volume(%v) path(%v) inode(%v)",
				v.name, path, invisibleTempDataInode.Inode)
			_, _ = v.mw.InodeUnlink_ll(context.Background(), invisibleTempDataInode.Inode)
			log.LogWarnf("PutObject: evict temp inode: volume(%v) path(%v) inode(%v)",
				v.name, path, invisibleTempDataInode.Inode)
			_ = v.mw.Evict(context.Background(), invisibleTempDataInode.Inode)
		}
	}()
	if err = v.ec.OpenStream(invisibleTempDataInode.Inode); err != nil {
		return
	}
	defer func() {
		if closeErr := v.ec.CloseStream(context.Background(), invisibleTempDataInode.Inode); closeErr != nil {
			log.LogErrorf("PutObject: close stream fail: volume(%v) inode(%v) err(%v)",
				v.name, invisibleTempDataInode.Inode, closeErr)
		}
	}()

	var (
		md5Hash  = md5.New()
		md5Value string
	)
	if _, err = v.streamWrite(invisibleTempDataInode.Inode, reader, md5Hash); err != nil {
		log.LogErrorf("PutObject: stream write failed: volume(%v) path(%v) inode(%v) err(%v)",
			v.name, path, invisibleTempDataInode.Inode, err)
		return
	}
	// compute file md5
	md5Value = hex.EncodeToString(md5Hash.Sum(nil))

	// flush
	if err = v.ec.Flush(context.Background(), invisibleTempDataInode.Inode); err != nil {
		log.LogErrorf("PutObject: data flush inode fail, inode(%v) err(%v)", invisibleTempDataInode.Inode, err)
		return nil, err
	}

	var finalInode *proto.InodeInfo
	if finalInode, err = v.mw.InodeGet_ll(context.Background(), invisibleTempDataInode.Inode); err != nil {
		log.LogErrorf("PutObject: get final inode fail: volume(%v) path(%v) inode(%v) err(%v)",
			v.name, path, invisibleTempDataInode.Inode, err)
		return
	}

	var etagValue = ETagValue{
		Value:   md5Value,
		PartNum: 0,
		TS:      finalInode.ModifyTime,
	}

	// Save ETag
	if err = v.mw.XAttrSet_ll(context.Background(), finalInode.Inode, []byte(XAttrKeyOSSETag), []byte(etagValue.Encode())); err != nil {
		log.LogErrorf("PutObject: store ETag fail: volume(%v) path(%v) inode(%v) key(%v) val(%v) err(%v)",
			v.name, path, invisibleTempDataInode.Inode, XAttrKeyOSSETag, md5Value, err)
		return nil, err
	}
	// If MIME information is valid, use extended attributes for storage.
	if opt != nil && opt.MIMEType != "" {
		if err = v.mw.XAttrSet_ll(context.Background(), invisibleTempDataInode.Inode, []byte(XAttrKeyOSSMIME), []byte(opt.MIMEType)); err != nil {
			log.LogErrorf("PutObject: store MIME fail: volume(%v) path(%v) inode(%v) mime(%v) err(%v)",
				v.name, path, invisibleTempDataInode.Inode, opt.MIMEType, err)
			return nil, err
		}
	}
	// If request contain content-disposition header, store it to xattr
	if opt != nil && len(opt.Disposition) > 0 {
		if err = v.mw.XAttrSet_ll(context.Background(), invisibleTempDataInode.Inode, []byte(XAttrKeyOSSDISPOSITION), []byte(opt.Disposition)); err != nil {
			log.LogErrorf("PutObject: store disposition fail: volume(%v) path(%v) inode(%v) disposition value(%v) err(%v)",
				v.name, path, invisibleTempDataInode.Inode, opt.Disposition, err)
		}
	}
	// If tagging have been specified, use extend attributes for storage.
	if opt != nil && opt.Tagging != nil {
		var encoded = opt.Tagging.Encode()
		if err = v.mw.XAttrSet_ll(context.Background(), invisibleTempDataInode.Inode, []byte(XAttrKeyOSSTagging), []byte(encoded)); err != nil {
			log.LogErrorf("PutObject: store Tagging fail: volume(%v) path(%v) inode(%v) value(%v) err(%v)",
				v.name, path, invisibleTempDataInode.Inode, encoded, err)
			return nil, err
		}
	}
	// If request contain cache-control header, store it to xattr
	if opt != nil && len(opt.CacheControl) > 0 {
		if err = v.mw.XAttrSet_ll(context.Background(), invisibleTempDataInode.Inode, []byte(XAttrKeyOSSCacheControl), []byte(opt.CacheControl)); err != nil {
			log.LogErrorf("PutObject: store cache-control fail: volume(%v) path(%v) inode(%v) cache-control value(%v) err(%v)",
				v.name, path, invisibleTempDataInode.Inode, opt.CacheControl, err)
			return nil, err
		}
	}
	// If request contain expires header, store it to xattr
	if opt != nil && len(opt.Expires) > 0 {
		if err = v.mw.XAttrSet_ll(context.Background(), invisibleTempDataInode.Inode, []byte(XAttrKeyOSSExpires), []byte(opt.Expires)); err != nil {
			log.LogErrorf("PutObject: store expires fail: volume(%v) path(%v) inode(%v) expires value(%v) err(%v)",
				v.name, path, invisibleTempDataInode.Inode, opt.Expires, err)
			return nil, err
		}
	}
	// If user-defined metadata have been specified, use extend attributes for storage.
	if opt != nil && len(opt.Metadata) > 0 {
		for name, value := range opt.Metadata {
			if err = v.mw.XAttrSet_ll(context.Background(), invisibleTempDataInode.Inode, []byte(name), []byte(value)); err != nil {
				log.LogErrorf("PutObject: store user-defined metadata fail: "+
					"volume(%v) path(%v) inode(%v) key(%v) value(%v) err(%v)",
					v.name, path, invisibleTempDataInode.Inode, name, value, err)
				return nil, err
			}
			log.LogDebugf("PutObject: store user-defined metadata: "+
				"volume(%v) path(%v) inode(%v) key(%v) value(%v)",
				v.name, path, invisibleTempDataInode.Inode, name, value)
		}
	}

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
		log.LogErrorf("PutObject: apply new inode to dentry fail: volume(%v) path(%v) parentID(%v) name(%v) inode(%v) err(%v)",
			v.name, path, parentId, lastPathItem.Name, invisibleTempDataInode.Inode, err)
		return
	}
	return fsInfo, nil
}

func (v *Volume) applyInodeToDEntry(parentId uint64, name string, inode uint64) (err error) {
	var existMode uint32

	for {
		_, existMode, err = v.mw.Lookup_ll(context.Background(), parentId, name)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("applyInodeToDEntry: meta lookup fail: volume(%v) parentID(%v) name(%v) err(%v)", v.name, parentId, name, err)
			return
		}

		if err == syscall.ENOENT {
			err = v.applyInodeToNewDentry(parentId, name, inode)
			if err == syscall.EEXIST {
				// The dentry used to not exist but have been created before appling inode to new dentry.
				continue
			}
			if err != nil {
				log.LogErrorf("applyInodeToDEntry: apply inode to new dentry fail: volume(%v) parentID(%v) name(%v) inode(%v) err(%v)",
					v.name, parentId, name, inode, err)
				return
			}
			log.LogDebugf("applyInodeToDEntry: apply inode to new dentry: volume(%v) parentID(%v) name(%v) inode(%v)",
				v.name, parentId, name, inode)
		} else {
			if os.FileMode(existMode).IsDir() {
				log.LogErrorf("applyInodeToDEntry: target mode conflict: volume(%v) parentID(%v) name(%v) mode(%v)",
					v.name, parentId, name, os.FileMode(existMode).String())
				err = syscall.EINVAL
				return
			}
			err = v.applyInodeToExistDentry(parentId, name, inode)
			if err == syscall.ENOENT {
				// The dentry used to exist but have been deleted before applying inode to exist dentry.
				continue
			}
			if err != nil {
				log.LogErrorf("applyInodeToDEntry: apply inode to exist dentry fail: volume(%v) parentID(%v) name(%v) inode(%v) err(%v)",
					v.name, parentId, name, inode, err)
				return
			}
		}
		return
	}
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
		dentries, err = v.mw.ReadDir_ll(context.Background(), ino)
		if err != nil || len(dentries) > 0 {
			return
		}
	}
	log.LogWarnf("DeletePath: delete: volume(%v) path(%v) inode(%v)", v.name, path, ino)
	if _, err = v.mw.Delete_ll(context.Background(), parent, name, mode.IsDir()); err != nil {
		return
	}

	// Evict inode
	if err = v.ec.EvictStream(context.Background(), ino); err != nil {
		log.LogWarnf("DeletePath EvictStream: path(%v) inode(%v)", path, ino)
	}
	log.LogWarnf("DeletePath: evict: volume(%v) path(%v) inode(%v)", v.name, path, ino)
	if err = v.mw.Evict(context.Background(), ino); err != nil {
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

	// Iterate all the meta partition to create multipart id
	multipartID, err = v.mw.InitMultipart_ll(context.Background(), path, extend)
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
	if tempInodeInfo, err = v.mw.InodeCreate_ll(context.Background(), DefaultFileMode, 0, 0, nil); err != nil {
		log.LogErrorf("WritePart: meta create inode fail: multipartID(%v) partID(%v) err(%v)",
			multipartId, partId, err)
		return nil, err
	}
	log.LogDebugf("WritePart: meta create temp file inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
		v.name, path, multipartId, partId, tempInodeInfo.Inode)

	defer func() {
		// An error has caused the entire process to fail. Delete the inode and release the written data.
		if err != nil || exist {
			log.LogWarnf("WritePart: unlink part inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
				v.name, path, multipartId, partId, tempInodeInfo.Inode)
			_, _ = v.mw.InodeUnlink_ll(context.Background(), tempInodeInfo.Inode)
			log.LogWarnf("WritePart: evict part inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
				v.name, path, multipartId, partId, tempInodeInfo.Inode)
			_ = v.mw.Evict(context.Background(), tempInodeInfo.Inode)
		}
	}()

	if err = v.ec.OpenStream(tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WritePart: data open stream fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
			v.name, path, multipartId, partId, tempInodeInfo.Inode, err)
		return nil, err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(context.Background(), tempInodeInfo.Inode); closeErr != nil {
			log.LogErrorf("WritePart: data close stream fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
				v.name, path, multipartId, partId, tempInodeInfo.Inode, closeErr)
		}
	}()
	// Write data to data node
	var (
		size    uint64
		etag    string
		md5Hash = md5.New()
	)
	if size, err = v.streamWrite(tempInodeInfo.Inode, reader, md5Hash); err != nil {
		return nil, err
	}
	// compute file md5
	etag = hex.EncodeToString(md5Hash.Sum(nil))

	// flush
	if err = v.ec.Flush(context.Background(), tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WritePart: data flush inode fail: volume(%v) inode(%v) err(%v)", v.name, tempInodeInfo.Inode, err)
		return nil, err
	}
	// update temp file inode to meta with session
	err = v.mw.AddMultipartPart_ll(context.Background(), path, multipartId, partId, size, etag, tempInodeInfo.Inode)
	if err == syscall.EEXIST {
		// Result success but cleanup data.
		err = nil
		exist = true
	}
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
	if multipartInfo, err = v.mw.GetMultipart_ll(context.Background(), path, multipartID); err != nil {
		log.LogErrorf("AbortMultipart: meta get multipart fail: volume(%v) multipartID(%v) path(%v) err(%v)",
			v.name, multipartID, path, err)
		return
	}
	// release part data
	for _, part := range multipartInfo.Parts {
		log.LogWarnf("AbortMultipart: unlink part inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
			v.name, path, multipartID, part.ID, part.Inode)
		if _, err = v.mw.InodeUnlink_ll(context.Background(), part.Inode); err != nil {
			log.LogErrorf("AbortMultipart: meta inode unlink fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
				v.name, path, multipartID, part.ID, part.Inode, err)
		}
		log.LogWarnf("AbortMultipart: evict part inode: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
			v.name, path, multipartID, part.ID, part.Inode)
		if err = v.mw.Evict(context.Background(), part.Inode); err != nil {
			log.LogErrorf("AbortMultipart: meta inode evict fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
				v.name, path, multipartID, part.ID, part.Inode, err)
		}
		log.LogDebugf("AbortMultipart: multipart part data released: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v)",
			v.name, path, multipartID, part.ID, part.Inode)
	}

	if err = v.mw.RemoveMultipart_ll(context.Background(), path, multipartID); err != nil {
		log.LogErrorf("AbortMultipart: meta abort multipart fail: volume(%v) path(%v) multipartID(%v) err(%v)",
			v.name, path, multipartID, err)
		return err
	}
	log.LogDebugf("AbortMultipart: meta abort multipart: volume(%v) path(%v) multipartID(%v) path(%v)",
		v.name, path, multipartID, path)
	return nil
}

func (v *Volume) CompleteMultipart(path, multipartID string, multipartInfo *proto.MultipartInfo) (fsFileInfo *FSFileInfo, err error) {
	defer func() {
		log.LogInfof("Audit: CompleteMultipart: volume(%v) path(%v) multipartID(%v) err(%v)",
			v.name, path, multipartID, err)
	}()

	parts := multipartInfo.Parts
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })

	// create inode for complete data
	var completeInodeInfo *proto.InodeInfo
	if completeInodeInfo, err = v.mw.InodeCreate_ll(context.Background(), DefaultFileMode, 0, 0, nil); err != nil {
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
			if deleteErr := v.mw.InodeDelete_ll(context.Background(), completeInodeInfo.Inode); deleteErr != nil {
				log.LogErrorf("CompleteMultipart: meta delete complete inode fail: volume(%v) path(%v) multipartID(%v) inode(%v) err(%v)",
					v.name, path, multipartID, completeInodeInfo.Inode, err)
			}
		}
	}()

	// merge complete extent keys
	var size uint64
	var completeExtentKeys = make([]proto.ExtentKey, 0)
	var fileOffset uint64
	for _, part := range parts {
		var eks []proto.ExtentKey
		if _, _, eks, err = v.mw.GetExtents(context.Background(), part.Inode); err != nil {
			log.LogErrorf("CompleteMultipart: meta get extents fail: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
				v.name, path, multipartID, part.ID, part.Inode, err)
			return
		}

		// recompute offsets of extent keys
		var eksSize uint64
		for _, ek := range eks {
			eksSize += uint64(ek.Size)
			ek.FileOffset = fileOffset
			fileOffset += uint64(ek.Size)
			completeExtentKeys = append(completeExtentKeys, ek)
		}
		if part.Size != eksSize {
			log.LogErrorf("CompleteMultipart: part extents length not match with part size: volume(%v) path(%v) multipartID(%v) partID(%v) inode(%v) partSize(%v) eksSize(%v) eksLength(%v)",
				v.name, path, multipartID, part.ID, part.Inode, part.Size, eksSize, len(eks))
			return nil, errors.New("part extents length not match with part size")
		}
		size += part.Size
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

	if err = v.mw.AppendExtentKeys(context.Background(), completeInodeInfo.Inode, completeExtentKeys); err != nil {
		log.LogErrorf("CompleteMultipart: meta append extent keys fail: volume(%v) path(%v) multipartID(%v) inode(%v) err(%v)",
			v.name, path, multipartID, completeInodeInfo.Inode, err)
		return
	}

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
	if finalInode, err = v.mw.InodeGet_ll(context.Background(), completeInodeInfo.Inode); err != nil {
		log.LogErrorf("CompleteMultipart: get inode fail: volume(%v) inode(%v) err(%v)",
			v.name, completeInodeInfo.Inode, err)
		return
	}

	var etagValue = ETagValue{
		Value:   md5Val,
		PartNum: len(parts),
		TS:      finalInode.ModifyTime,
	}
	if err = v.mw.XAttrSet_ll(context.Background(), finalInode.Inode, []byte(XAttrKeyOSSETag), []byte(etagValue.Encode())); err != nil {
		log.LogErrorf("CompleteMultipart: save ETag fail: volume(%v) inode(%v) err(%v)",
			v.name, completeInodeInfo, err)
		return
	}
	// set user modified system metadata, self defined metadata and tag
	extend := multipartInfo.Extend
	if len(extend) > 0 {
		for key, value := range extend {
			if err = v.mw.XAttrSet_ll(context.Background(), completeInodeInfo.Inode, []byte(key), []byte(value)); err != nil {
				log.LogErrorf("CompleteMultipart: store multipart extend fail: volume(%v) path(%v) inode(%v) key(%v) value(%v) err(%v)",
					v.name, path, completeInodeInfo.Inode, key, value, err)
				return nil, err
			}
		}
	}

	// remove multipart
	err = v.mw.RemoveMultipart_ll(context.Background(), path, multipartID)
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
		if err = v.mw.InodeDelete_ll(context.Background(), part.Inode); err != nil {
			log.LogErrorf("CompleteMultipart: destroy part inode fail: volume(%v) multipartID(%v) partID(%v) inode(%v) err(%v)",
				v.name, multipartID, part.ID, part.Inode, err)
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
			if writeN, _, err = v.ec.Write(context.Background(), inode, offset, buf[:readN], false, false); err != nil {
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
		if closeErr := v.ec.CloseStream(context.Background(), inode); closeErr != nil {
			log.LogWarnf("appendInodeHash: data close stream fail: inode(%v) err(%v)",
				inode, err)
		}
		if evictErr := v.ec.EvictStream(context.Background(), inode); evictErr != nil {
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
		n, err = v.ec.Read(context.Background(), inode, buf, offset, size)
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
	if err = v.mw.DentryCreate_ll(context.Background(), parentID, name, inode, DefaultFileMode); err != nil {
		if err != syscall.EEXIST {
			log.LogErrorf("applyInodeToNewDentry: meta dentry create fail: volume(%v) parentID(%v) name(%v) inode(%v) mode(%v) err(%v)",
				v.name, parentID, name, inode, DefaultFileMode, err)
		}
		return err
	}
	return
}

func (v *Volume) applyInodeToExistDentry(parentID uint64, name string, inode uint64) (err error) {
	var oldInode uint64
	oldInode, err = v.mw.DentryUpdate_ll(context.Background(), parentID, name, inode)
	if err != nil {
		log.LogErrorf("applyInodeToExistDentry: meta update dentry fail: volume(%v) parentID(%v) name(%v) inode(%v) err(%v)",
			v.name, parentID, name, inode, err)
		return
	}

	// unlink and evict old inode
	log.LogWarnf("applyInodeToExistDentry: unlink inode: volume(%v) inode(%v)", v.name, oldInode)
	if _, err = v.mw.InodeUnlink_ll(context.Background(), oldInode); err != nil {
		log.LogWarnf("applyInodeToExistDentry: unlink inode fail: volume(%v) inode(%v) err(%v)",
			v.name, oldInode, err)
	}

	log.LogWarnf("applyInodeToExistDentry: evict inode: volume(%v) inode(%v)", v.name, oldInode)
	if err = v.mw.Evict(context.Background(), oldInode); err != nil {
		log.LogWarnf("applyInodeToExistDentry: evict inode fail: volume(%v) inode(%v) err(%v)",
			v.name, oldInode, err)
	}
	err = nil
	return
}

func (v *Volume) loadUserDefinedMetadata(inode uint64) (metadata map[string]string, err error) {
	var storedXAttrKeys []string
	if storedXAttrKeys, err = v.mw.XAttrsList_ll(context.Background(), inode); err != nil {
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
	if xattrs, err = v.mw.BatchGetXAttr(context.Background(), []uint64{inode}, xattrKeys); err != nil {
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

func (v *Volume) ReadInode(ino uint64, writer io.Writer, offset, size uint64) error {
	var err error

	// read file data
	var inoInfo *proto.InodeInfo
	if inoInfo, err = v.mw.InodeGet_ll(context.Background(), ino); err != nil {
		return err
	}
	if offset >= inoInfo.Size {
		return nil
	}

	if err = v.ec.OpenStreamWithSize(ino, int(offset+size)); err != nil {
		log.LogErrorf("ReadFile: data open stream fail, Inode(%v) err(%v)", ino, err)
		return err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(context.Background(), ino); closeErr != nil {
			log.LogErrorf("ReadFile: data close stream fail: inode(%v) err(%v)", ino, closeErr)
		}
	}()

	var upper = size + offset
	if upper > inoInfo.Size {
		upper = inoInfo.Size - offset
	}

	var n int
	var tmp = make([]byte, 2*util.BlockSize)

	for {
		var rest = upper - uint64(offset)
		if rest == 0 {
			break
		}
		var readSize = len(tmp)
		if uint64(readSize) > rest {
			readSize = int(rest)
		}
		n, err = v.ec.Read(context.Background(), ino, tmp, int(offset), readSize)
		if err != nil && err != io.EOF {
			log.LogErrorf("ReadInode: data read fail: volume(%v) inode(%v) offset(%v) size(%v) err(%v)",
				v.name, ino, offset, size, err)
			exporter.Warning(fmt.Sprintf("read data fail: volume(%v) inode(%v) offset(%v) size(%v) err(%v)",
				v.name, ino, offset, readSize, err))
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
	return v.ReadInode(ino, writer, offset, size)
}

func (v *Volume) ObjectMeta(path string) (info *FSFileInfo, err error) {

	// process path
	var inode uint64
	var mode os.FileMode
	var inoInfo *proto.InodeInfo

	var retry = 0
	for {
		if _, inode, _, mode, err = v.recursiveLookupTarget(path); err != nil {
			return
		}

		inoInfo, err = v.mw.InodeGet_ll(context.Background(), inode)
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

	if mode.IsDir() {
		// Folder has specific ETag and MIME type.
		etagValue = DirectoryETagValue()
		mimeType = HeaderValueContentTypeDirectory
	} else {
		// Try to get the advanced attributes stored in the extended attributes.
		// The following advanced attributes apply to the object storage:
		// 1. Etag (MD5)
		// 2. MIME type
		var xattrs []*proto.XAttrInfo
		var xattrKeys = []string{XAttrKeyOSSETag, XAttrKeyOSSETagDeprecated, XAttrKeyOSSMIME, XAttrKeyOSSDISPOSITION,
			XAttrKeyOSSCacheControl, XAttrKeyOSSExpires}
		if xattrs, err = v.mw.BatchGetXAttr(context.Background(), []uint64{inode}, xattrKeys); err != nil {
			log.LogErrorf("ObjectMeta: meta get xattr fail, volume(%v) inode(%v) path(%v) keys(%v) err(%v)",
				v.name, inode, path, strings.Join(xattrKeys, ","), err)
			return
		}
		if len(xattrs) > 0 && xattrs[0].Inode == inode {
			var xattr = xattrs[0]
			var rawETag = string(xattr.Get(XAttrKeyOSSETag))
			if len(rawETag) == 0 {
				rawETag = string(xattr.Get(XAttrKeyOSSETagDeprecated))
			}
			if len(rawETag) > 0 {
				etagValue = ParseETagValue(rawETag)
			}

			mimeType = string(xattr.Get(XAttrKeyOSSMIME))
			disposition = string(xattr.Get(XAttrKeyOSSDISPOSITION))
			cacheControl = string(xattr.Get(XAttrKeyOSSCacheControl))
			expires = string(xattr.Get(XAttrKeyOSSExpires))
		}
	}

	// Load user-defined metadata
	var metadata map[string]string
	if metadata, err = v.loadUserDefinedMetadata(inode); err != nil {
		log.LogErrorf("ObjectMeta: load user-defined metadata fail: volume(%v) inode(%v) path(%v) err(%v)",
			v.name, inode, path, err)
		return
	}

	// Validating ETag value.
	if !mode.IsDir() && (!etagValue.Valid() || etagValue.TS.Before(inoInfo.ModifyTime)) {
		// The ETag is invalid or outdated then generate a new ETag and make update.
		if etagValue, err = v.updateETag(inoInfo.Inode, int64(inoInfo.Size), inoInfo.ModifyTime); err != nil {
			log.LogErrorf("ObjectMeta: update ETag fail: volume(%v) path(%v) inode(%v) err(%v)",
				v.name, path, inoInfo.Inode, err)
		}
		log.LogDebugf("ObjectMeta: update ETag: volume(%v) path(%v) inode(%v) etagValue(%v)",
			v.name, path, inoInfo.Inode, etagValue)
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
		_ = v.ec.Close(context.Background())
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
	for pathIterator.HasNext() {
		var pathItem = pathIterator.Next()
		var curIno uint64
		var curMode uint32
		curIno, curMode, err = v.mw.Lookup_ll(context.Background(), parent, pathItem.Name)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("recursiveLookupPath: lookup fail, parentID(%v) name(%v) fail err(%v)",
				parent, pathItem.Name, err)
			return
		}
		if err == syscall.ENOENT {
			return
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
		curIno, curMode, err = v.mw.Lookup_ll(context.Background(), ino, pathItem.Name)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("recursiveMakeDirectory: lookup fail, parentID(%v) name(%v) fail err(%v)",
				ino, pathItem.Name, err)
			return
		}
		if err == syscall.ENOENT {
			var info *proto.InodeInfo
			info, err = v.mw.Create_ll(context.Background(), ino, pathItem.Name, uint32(DefaultDirMode), 0, 0, nil)
			if err != nil && err == syscall.EEXIST {
				existInode, mode, e := v.mw.Lookup_ll(context.Background(), ino, pathItem.Name)
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
		curIno, curMode, lookupErr := v.mw.Lookup_ll(context.Background(), parentId, dir)
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
			inodeInfo, createErr = v.mw.Create_ll(context.Background(), parentId, dir, uint32(DefaultDirMode), 0, 0, nil)
			if createErr != nil && createErr != syscall.EEXIST {
				log.LogErrorf("lookupDirectories: meta create fail, parentID(%v) name(%v) mode(%v) err(%v)", parentId, dir, os.ModeDir, createErr)
				return 0, createErr
			}
			// retry lookup if it exists.
			if createErr == syscall.EEXIST {
				curIno, curMode, lookupErr = v.mw.Lookup_ll(context.Background(), parentId, dir)
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

func (v *Volume) listFilesV1(prefix, marker, delimiter string, maxKeys uint64) (infos []*FSFileInfo, prefixes Prefixes, nextMarker string, err error) {
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

	log.LogDebugf("listFilesV1: find parent ID, prefix(%v) marker(%v) delimiter(%v) parentId(%v) dirs(%v)", prefix, marker, delimiter, parentId, len(dirs))

	// Init the value that queried result count.
	// Check this value when adding key to contents or common prefix,
	// return if it reach to max keys
	var rc uint64
	// recursion scan
	infos, prefixMap, nextMarker, _, err = v.recursiveScan(infos, prefixMap, parentId, maxKeys, rc, dirs, prefix, marker, delimiter)
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

func (v *Volume) listFilesV2(prefix, startAfter, contToken, delimiter string, maxKeys uint64) (infos []*FSFileInfo, prefixes Prefixes, nextMarker string, err error) {
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

	log.LogDebugf("listFilesV2: find parent ID, prefix(%v) marker(%v) delimiter(%v) parentId(%v) dirs(%v)", prefix, marker, delimiter, parentId, len(dirs))

	// Init the value that queried result count.
	// Check this value when adding key to contents or common prefix,
	// return if it reach to max keys
	var rc uint64
	// recursion scan
	infos, prefixMap, nextMarker, _, err = v.recursiveScan(infos, prefixMap, parentId, maxKeys, rc, dirs, prefix, marker, delimiter)
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

		curIno, curMode, err := v.mw.Lookup_ll(context.Background(), parentId, dir)

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
func (v *Volume) recursiveScan(fileInfos []*FSFileInfo, prefixMap PrefixMap, parentId, maxKeys, rc uint64,
	dirs []string, prefix, marker, delimiter string) ([]*FSFileInfo, PrefixMap, string, uint64, error) {
	var err error
	var nextMarker string

	var currentPath = strings.Join(dirs, pathSep) + pathSep

	if len(dirs) > 0 && prefix != "" && strings.HasSuffix(currentPath, prefix) {
		// When the current scanning position is not the root directory, a prefix matching
		// check is performed on the current directory first.
		//
		// The reason for this is that according to the definition of Amazon S3's ListObjects
		// interface, directory entries that meet the exact prefix match will be returned as
		// a Content result, not as a CommonPrefix.

		// Add check to marker, if request contain marker, current path must greater than marker,
		// otherwise can not put it to prefix map
		if (len(marker) > 0 && currentPath > marker) || len(marker) == 0 {
			fileInfo := &FSFileInfo{
				Inode: parentId,
				Path:  currentPath,
			}
			// If the number of matches reaches the threshold given by maxKey,
			// stop scanning and return results.
			// To get the next marker, first compare the result counts with the max key
			// it means that when result count reach the amx key, continue to find the next key as the next marker
			if rc >= maxKeys {
				return fileInfos, prefixMap, currentPath, rc, nil
			}
			fileInfos = append(fileInfos, fileInfo)
			rc++
		}
	}

	// During the process of scanning the child nodes of the current directory, there may be other
	// parallel operations that may delete the current directory.
	// If got the syscall.ENOENT error when invoke readdir, it means that the above situation has occurred.
	// At this time, stops process and returns success.
	var children []proto.Dentry
	children, err = v.mw.ReadDir_ll(context.Background(), parentId)
	if err != nil && err != syscall.ENOENT {
		return fileInfos, prefixMap, "", 0, err
	}
	if err == syscall.ENOENT {
		return fileInfos, prefixMap, "", 0, nil
	}

	for _, child := range children {
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
			if os.FileMode(child.Type).IsDir() && path < marker {
				fileInfos, prefixMap, nextMarker, rc, err = v.recursiveScan(fileInfos, prefixMap, child.Inode, maxKeys, rc, append(dirs, child.Name), prefix, marker, delimiter)
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

		fileInfo := &FSFileInfo{
			Inode: child.Inode,
			Path:  path,
		}
		if rc >= maxKeys {
			return fileInfos, prefixMap, path, rc, nil
		}
		fileInfos = append(fileInfos, fileInfo)
		rc++

		if os.FileMode(child.Type).IsDir() {
			fileInfos, prefixMap, nextMarker, rc, err = v.recursiveScan(fileInfos, prefixMap, child.Inode, maxKeys, rc, append(dirs, child.Name), prefix, marker, delimiter)
			if err != nil {
				return fileInfos, prefixMap, nextMarker, rc, err
			}
			if rc >= maxKeys && nextMarker != "" {
				return fileInfos, prefixMap, nextMarker, rc, err
			}
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
	inodeInfos := v.mw.BatchInodeGet(context.Background(), inodes)
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
	xattrs, err := v.mw.BatchGetXAttr(context.Background(), inodes, keys)
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
	if err = v.mw.XAttrSet_ll(context.Background(), inode, []byte(XAttrKeyOSSETag), []byte(etagValue.Encode())); err != nil {
		return
	}
	return
}

func (v *Volume) ListMultipartUploads(prefix, delimiter, keyMarker string, multipartIdMarker string,
	maxUploads uint64) ([]*FSUpload, string, string, bool, []string, error) {
	sessions, err := v.mw.ListMultipart_ll(context.Background(), prefix, delimiter, keyMarker, multipartIdMarker, maxUploads)
	if err != nil {
		return nil, "", "", false, nil, err
	}

	uploads := make([]*FSUpload, 0)
	prefixes := make([]string, 0)
	prefixMap := make(map[string]interface{})

	var nextUpload *proto.MultipartInfo
	var NextMarker string
	var NextSessionIdMarker string
	var IsTruncated bool

	if len(sessions) == 0 {
		return nil, "", "", false, nil, nil
	}

	// get maxUploads number sessions from combined sessions
	if len(sessions) > int(maxUploads) {
		nextUpload = sessions[maxUploads]
		sessions = sessions[:maxUploads]
		NextMarker = nextUpload.Path
		NextSessionIdMarker = nextUpload.ID
		IsTruncated = true
	}

	for _, session := range sessions {
		var tempKey = session.Path
		if len(prefix) > 0 {
			pIndex := strings.Index(tempKey, prefix)
			tempKeyRunes := []rune(tempKey)
			tempKey = string(tempKeyRunes[pIndex+len(prefix):])
		}

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
	if len(prefixMap) > 0 {
		for prefix := range prefixMap {
			prefixes = append(prefixes, prefix)
		}
	}
	sort.SliceStable(prefixes, func(i, j int) bool {
		return prefixes[i] < prefixes[j]
	})
	return uploads, NextMarker, NextSessionIdMarker, IsTruncated, prefixes, nil
}

func (v *Volume) ListParts(path, uploadId string, maxParts, partNumberMarker uint64) (parts []*FSPart, nextMarker uint64, isTruncated bool, err error) {
	multipartInfo, err := v.mw.GetMultipart_ll(context.Background(), path, uploadId)
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
	if sInodeInfo, err = sv.mw.InodeGet_ll(context.Background(), sInode); err != nil {
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
		if closeErr := sv.ec.CloseStream(context.Background(), sInode); closeErr != nil {
			log.LogErrorf("CopyFile: close source path stream fail: source path(%v) source path inode(%v) err(%v)",
				sourcePath, sInode, closeErr)
		}
	}()

	// if source path is same with target path, just reset file metadata
	// source path is same with target path, and metadata directive is not 'REPLACE', object node do nothing
	if targetPath == sourcePath {
		if metaDirective != MetadataDirectiveReplace {
			log.LogInfof("CopyFile: target path is equal with source path, object node do nothing, source path(%v) target path(%v) err(%v)",
				sourcePath, targetPath, err)
		} else {
			// replace system metadata : 'Content-Type' and 'Content-Disposition', if user specified,
			// replace user defined metadata
			// If MIME information is valid, use extended attributes for storage.
			if opt != nil && opt.MIMEType != "" {
				if err = v.mw.XAttrSet_ll(context.Background(), sInode, []byte(XAttrKeyOSSMIME), []byte(opt.MIMEType)); err != nil {
					log.LogErrorf("CopyFile: store MIME fail: volume(%v) source path(%v) inode(%v) mime(%v) err(%v)",
						sv.name, sourcePath, sInode, opt.MIMEType, err)
					return nil, err
				}
			}
			if opt != nil && opt.Disposition != "" {
				if err = v.mw.XAttrSet_ll(context.Background(), sInode, []byte(XAttrKeyOSSDISPOSITION), []byte(opt.Disposition)); err != nil {
					log.LogErrorf("CopyFile: store content disposition fail: volume(%v) source path(%v) inode(%v) disposition(%v) err(%v)",
						sv.name, sourcePath, sInode, opt.Disposition, err)
					return nil, err
				}
			}
			if opt != nil && opt.CacheControl != "" {
				if err = v.mw.XAttrSet_ll(context.Background(), sInode, []byte(XAttrKeyOSSCacheControl), []byte(opt.CacheControl)); err != nil {
					log.LogErrorf("CopyFile: store content cache-control fail: volume(%v) source path(%v) inode(%v) cache-control(%v) err(%v)",
						sv.name, sourcePath, sInode, opt.CacheControl, err)
					return nil, err
				}
			}
			if opt != nil && opt.Expires != "" {
				if err = v.mw.XAttrSet_ll(context.Background(), sInode, []byte(XAttrKeyOSSExpires), []byte(opt.Expires)); err != nil {
					log.LogErrorf("CopyFile: store content expires fail: volume(%v) source path(%v) inode(%v) expires(%v) err(%v)",
						sv.name, sourcePath, sInode, opt.Expires, err)
					return nil, err
				}
			}
			// If user-defined metadata have been specified, use extend attributes for storage.
			if opt != nil && len(opt.Metadata) > 0 {
				for name, value := range opt.Metadata {
					if err = v.mw.XAttrSet_ll(context.Background(), sInode, []byte(name), []byte(value)); err != nil {
						log.LogErrorf("CopyFile: store user-defined metadata fail: "+
							"volume(%v) source path(%v) inode(%v) key(%v) value(%v) err(%v)",
							sv.name, sourcePath, sInode, name, value, err)
						return nil, err
					}
				}
			}
			log.LogInfof("CopyFile: target path is equal with source path, replace metadata, source path(%v) target path(%v) opt(%v)",
				sourcePath, targetPath, opt)
		}
		return sv.ObjectMeta(sourcePath)
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
		if tInodeInfo, err = v.mw.InodeGet_ll(context.Background(), tParentId); err != nil {
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
	if tInodeInfo, err = v.mw.InodeCreate_ll(context.Background(), uint32(sMode), 0, 0, nil); err != nil {
		return
	}
	defer func() {
		// An error has caused the entire process to fail. Delete the inode and release the written data.
		if err != nil {
			log.LogWarnf("CopyFile: unlink target temp inode: volume(%v) path(%v) inode(%v) ",
				v.name, targetPath, tInodeInfo.Inode)
			_, _ = v.mw.InodeUnlink_ll(context.Background(), tInodeInfo.Inode)
			log.LogWarnf("CopyFile: evict target temp inode: volume(%v) path(%v) inode(%v)",
				v.name, targetPath, tInodeInfo.Inode)
			_ = v.mw.Evict(context.Background(), tInodeInfo.Inode)
		}
	}()
	if err = v.ec.OpenStream(tInodeInfo.Inode); err != nil {
		return
	}
	defer func() {
		if closeErr := v.ec.CloseStream(context.Background(), tInodeInfo.Inode); closeErr != nil {
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
		buf         = make([]byte, 2*util.BlockSize)
		hashBuf     = make([]byte, 2*util.BlockSize)
	)
	for {
		readSize = len(buf)
		if (int(fileSize) - readOffset) <= 0 {
			break
		}
		if (int(fileSize) - readOffset) < len(buf) {
			readSize = int(fileSize) - readOffset
		}
		readN, err = sv.ec.Read(context.Background(), sInode, buf, readOffset, readSize)
		if err != nil && err != io.EOF {
			return
		}
		if readN > 0 {
			if writeN, _, err = v.ec.Write(context.Background(), tInodeInfo.Inode, writeOffset, buf[:readN], false, false); err != nil {
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
	if err = v.ec.Flush(context.Background(), tInodeInfo.Inode); err != nil {
		log.LogErrorf("CopyFile: data flush inode fail, volume(%v) inode(%v), path (%v) err(%v)", v.name, tInodeInfo.Inode, targetPath, err)
		return
	}
	md5Value = hex.EncodeToString(md5Hash.Sum(nil))
	log.LogDebugf("Audit: copy file: write file finished, volume(%v), path(%v), etag(%v)", v.name, targetPath, md5Value)

	var finalInode *proto.InodeInfo
	if finalInode, err = v.mw.InodeGet_ll(context.Background(), tInodeInfo.Inode); err != nil {
		log.LogErrorf("CopyFile: get finished target path final inode fail: volume(%v) path(%v) inode(%v) err(%v)",
			v.name, targetPath, tInodeInfo.Inode, err)
		return
	}
	var etagValue = ETagValue{
		Value:   md5Value,
		PartNum: 0,
		TS:      finalInode.ModifyTime,
	}

	// Save target file ETag
	if err = v.mw.XAttrSet_ll(context.Background(), finalInode.Inode, []byte(XAttrKeyOSSETag), []byte(etagValue.Encode())); err != nil {
		log.LogErrorf("CopyFile: store target file ETag fail: volume(%v) path(%v) inode(%v) key(%v) val(%v) err(%v)",
			v.name, targetPath, tInodeInfo.Inode, XAttrKeyOSSETag, md5Value, err)
		return
	}

	// copy source file metadata to write target file metadata
	if metaDirective != MetadataDirectiveReplace {
		// get source file xattr keys
		var keys []string
		if keys, err = sv.mw.XAttrsList_ll(context.Background(), sInode); err != nil {
			if err == syscall.ENOENT {
				log.LogErrorf("CopyFile: volume list extend attributes fail: volume(%v) source path(%v) err(%v)",
					sv.name, sourcePath, err)
				return
			}
			log.LogErrorf("CopyFile: volume list extend attributes fail: volume(%v) source path(%v) err(%v)",
				sv.name, sourcePath, err)
			return
		}
		// batch get source file xattr values
		var xattrs []*proto.XAttrInfo
		if xattrs, err = sv.mw.BatchGetXAttr(context.Background(), []uint64{sInode}, keys); err != nil {
			log.LogErrorf("CopyFile: meta get xattr fail, volume(%v) source path(%v) inode(%v) keys(%v) err(%v)",
				sv.name, sourcePath, sInode, strings.Join(keys, ","), err)
			return
		}
		// set tar xattr
		if len(xattrs) > 0 {
			for xk, xv := range xattrs[0].XAttrs {
				if xk == XAttrKeyOSSETag {
					continue
				}
				if err = v.mw.XAttrSet_ll(context.Background(), tInodeInfo.Inode, []byte(xk), []byte(xv)); err != nil {
					log.LogErrorf("CopyFile: set target xattr fail: volume(%v) target path(%v) inode(%v) xattr key(%v) xattr value(%v) err(%v)",
						v.name, targetPath, tInodeInfo.Inode, xk, xv, err)
					return
				}
			}
		}
	} else {
		if opt != nil && opt.MIMEType != "" {
			if err = v.mw.XAttrSet_ll(context.Background(), tInodeInfo.Inode, []byte(XAttrKeyOSSMIME), []byte(opt.MIMEType)); err != nil {
				log.LogErrorf("CopyFile: store MIME fail: volume(%v) target path(%v) inode(%v) mime(%v) err(%v)",
					v.name, targetPath, tInodeInfo.Inode, opt.MIMEType, err)
				return nil, err
			}
		}
		if opt != nil && opt.Disposition != "" {
			if err = v.mw.XAttrSet_ll(context.Background(), tInodeInfo.Inode, []byte(XAttrKeyOSSDISPOSITION), []byte(opt.Disposition)); err != nil {
				log.LogErrorf("CopyFile: store content disposition fail: volume(%v) target path(%v) inode(%v) mime(%v) err(%v)",
					v.name, targetPath, tInodeInfo.Inode, opt.Disposition, err)
				return nil, err
			}
		}
		if opt != nil && opt.CacheControl != "" {
			if err = v.mw.XAttrSet_ll(context.Background(), tInodeInfo.Inode, []byte(XAttrKeyOSSCacheControl), []byte(opt.CacheControl)); err != nil {
				log.LogErrorf("CopyFile: store content cache-control fail: volume(%v) target path(%v) inode(%v) cache-control(%v) err(%v)",
					v.name, targetPath, tInodeInfo.Inode, opt.CacheControl, err)
				return nil, err
			}
		}
		if opt != nil && opt.Expires != "" {
			if err = v.mw.XAttrSet_ll(context.Background(), tInodeInfo.Inode, []byte(XAttrKeyOSSExpires), []byte(opt.Expires)); err != nil {
				log.LogErrorf("CopyFile: store content expires fail: volume(%v) target path(%v) inode(%v) expires(%v) err(%v)",
					v.name, targetPath, tInodeInfo.Inode, opt.Expires, err)
				return nil, err
			}
		}
		// If user-defined metadata have been specified, use extend attributes for storage.
		if opt != nil && len(opt.Metadata) > 0 {
			for name, value := range opt.Metadata {
				if err = v.mw.XAttrSet_ll(context.Background(), tInodeInfo.Inode, []byte(name), []byte(value)); err != nil {
					log.LogErrorf("CopyFile: store user-defined metadata fail: "+
						"volume(%v) target path(%v) inode(%v) key(%v) value(%v) err(%v)",
						v.name, targetPath, tInodeInfo.Inode, name, value, err)
					return nil, err
				}
			}
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
	return
}

func (v *Volume) copyFile(parentID uint64, newFileName string, sourceFileInode uint64, mode uint32) (info *proto.InodeInfo, err error) {

	if err = v.mw.DentryCreate_ll(context.Background(), parentID, newFileName, sourceFileInode, mode); err != nil {
		return
	}
	if info, err = v.mw.InodeLink_ll(context.Background(), sourceFileInode); err != nil {
		return
	}
	return
}

func (v *Volume) isPublicRead() bool {
	return v.mw != nil && v.mw.OSSBucketPolicy() == proto.OSSBucketPolicyPublicRead
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
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = metaWrapper.Close()
		}
	}()
	var extentConfig = &data.ExtentConfig{
		Volume:            config.Volume,
		Masters:           config.Masters,
		FollowerRead:      true,
		TinySize:          util.MB * 8,
		OnInsertExtentKey: metaWrapper.InsertExtentKey,
		OnGetExtents:      metaWrapper.GetExtents,
		OnTruncate:        metaWrapper.Truncate,
	}
	var extentClient *data.ExtentClient
	if extentClient, err = data.NewExtentClient(extentConfig); err != nil {
		return nil, err
	}

	v := &Volume{
		mw:         metaWrapper,
		ec:         extentClient,
		name:       config.Volume,
		store:      config.Store,
		createTime: metaWrapper.VolCreateTime(),
		closeCh:    make(chan struct{}),
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
