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
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"hash"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"

	"crypto/md5"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"

	"github.com/chubaofs/chubaofs/util"
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
}

// OSSMeta is bucket policy and ACL metadata.
type OSSMeta struct {
	policy     *Policy
	acl        *AccessControlPolicy
	policyLock sync.RWMutex
	aclLock    sync.RWMutex
}

func (v *Volume) loadPolicy() (p *Policy) {
	v.om.policyLock.RLock()
	p = v.om.policy
	v.om.policyLock.RUnlock()
	return
}

func (v *Volume) storePolicy(p *Policy) {
	v.om.policyLock.Lock()
	v.om.policy = p
	v.om.policyLock.Unlock()
	return
}

func (v *Volume) loadACL() (p *AccessControlPolicy) {
	v.om.aclLock.RLock()
	p = v.om.acl
	v.om.aclLock.RUnlock()
	return
}

func (v *Volume) storeACL(p *AccessControlPolicy) {
	v.om.aclLock.Lock()
	v.om.acl = p
	v.om.aclLock.Unlock()
	return
}

// Volume is a high-level encapsulation of meta sdk and data sdk methods.
// A high-level approach that exposes the semantics of object storage to the outside world.
// Volume escapes high-level object storage semantics to low-level POSIX semantics.
type Volume struct {
	mw         *meta.MetaWrapper
	ec         *stream.ExtentClient
	store      Store // Storage for ACP management
	name       string
	om         *OSSMeta
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
	if policy != nil {
		v.storePolicy(policy)
	}

	var acl *AccessControlPolicy
	if acl, err = v.loadBucketACL(); err != nil {
		return
	}
	if acl != nil {
		v.storeACL(acl)
	}
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
	policy = &Policy{}
	if err = json.Unmarshal(data, policy); err != nil {
		return
	}
	return
}

func (v *Volume) loadBucketACL() (*AccessControlPolicy, error) {
	data, err2 := v.store.Get(v.name, bucketRootPath, OSS_ACL_KEY)
	if err2 != nil {
		return nil, err2
	}
	acl := &AccessControlPolicy{}
	err3 := xml.Unmarshal(data, acl)
	if err3 != nil {
		return nil, err3
	}
	return acl, nil
}

func (v *Volume) OSSMeta() *OSSMeta {
	return v.om
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

func (v *Volume) SetXAttr(path string, key string, data []byte) error {
	var err error
	var inode uint64
	if inode, err = v.getInodeFromPath(path); err != nil && err != syscall.ENOENT {
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
	return v.mw.XAttrSet_ll(inode, []byte(key), data)
}

func (v *Volume) GetXAttr(path string, key string) (info *proto.XAttrInfo, err error) {
	var inode uint64
	inode, err = v.getInodeFromPath(path)
	if err != nil {
		return
	}
	if info, err = v.mw.XAttrGet_ll(inode, key); err != nil {
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
	if err = v.mw.XAttrDel_ll(inode, key); err != nil {
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
	if keys, err = v.mw.XAttrsList_ll(inode); err != nil {
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
func (v *Volume) ListFilesV1(request *ListBucketRequestV1) ([]*FSFileInfo, string, bool, []string, error) {
	//prefix, delimiter, marker string, maxKeys uint64

	marker := request.marker
	prefix := request.prefix
	maxKeys := request.maxKeys
	delimiter := request.delimiter

	var err error
	var infos []*FSFileInfo
	var prefixes Prefixes

	infos, prefixes, err = v.listFilesV1(prefix, marker, delimiter, maxKeys)
	if err != nil {
		log.LogErrorf("ListFilesV1: list fail: volume(%v) prefix(%v) marker(%v) delimiter(%v) maxKeys(%v) err(%v)",
			v.name, prefix, marker, delimiter, maxKeys, err)
		return nil, "", false, nil, err
	}

	var nextMarker string
	var isTruncated bool

	if len(infos) > int(maxKeys) {
		nextMarker = infos[maxKeys].Path
		infos = infos[:maxKeys]
		isTruncated = true
	}

	return infos, nextMarker, isTruncated, prefixes, nil
}

// ListFilesV2 returns file and directory entry list information that meets the parameters.
// It supports parameters such as prefix, delimiter, and paging.
// It is a data plane logical encapsulation of the object storage interface ListObjectsV2.
func (v *Volume) ListFilesV2(request *ListBucketRequestV2) ([]*FSFileInfo, uint64, string, bool, []string, error) {
	delimiter := request.delimiter
	maxKeys := request.maxKeys
	prefix := request.prefix
	contToken := request.contToken
	startAfter := request.startAfter

	var err error
	var infos []*FSFileInfo
	var prefixes Prefixes

	infos, prefixes, err = v.listFilesV2(prefix, startAfter, contToken, delimiter, maxKeys)
	if err != nil {
		log.LogErrorf("ListFilesV2: list fail: volume(%v) prefix(%v) startAfter(%v) contToken(%v) delimiter(%v) maxKeys(%v) err(%v)",
			v.name, prefix, startAfter, contToken, delimiter, maxKeys, err)
		return nil, 0, "", false, nil, err
	}

	var keyCount uint64
	var nextToken string
	var isTruncated bool

	keyCount = uint64(len(infos))
	if len(infos) > int(maxKeys) {
		nextToken = infos[maxKeys].Path
		infos = infos[:maxKeys]
		isTruncated = true
		keyCount = maxKeys
	}

	return infos, keyCount, nextToken, isTruncated, prefixes, nil
}

// WriteObject creates or updates target path objects and data.
// Differentiate whether a target is a file or a directory by identifying its MIME type.
// When the MIME type is "application/directory", the target object is a directory.
// During processing, conflicts may occur because the actual type of the target object is
// different from the expected type.
//
// For example, create a directory called "backup", but a file called "backup" already exists.
// When a conflict occurs, the method returns an EISDIR or ENOTDIR error.
//
// An EISDIR error is returned indicating that a part of the target path expected to be a file
// but actual is a directory.
// An ENOTDIR error is returned indicating that a part of the target path expected to be a directory
// but actual is a file.
func (v *Volume) WriteObject(path string, reader io.Reader, mimeType string) (fsInfo *FSFileInfo, err error) {
	defer func() {
		// Audit behavior
		log.LogInfof("Audit: WriteObject: volume(%v) path(%v) err(%v)", v.name, path, err)
	}()
	// The path is processed according to the content-type. If it is a directory type,
	// a path separator is appended at the end of the path, so the recursiveMakeDirectory
	// method can be processed directly in recursion.
	var fixedPath = path
	if mimeType == HeaderValueContentTypeDirectory && !strings.HasSuffix(path, pathSep) {
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
			ModifyTime: time.Now(),
			ETag:       EmptyContentMD5String,
			Inode:      rootIno,
			MIMEType:   HeaderValueContentTypeDirectory,
		}
		return fsInfo, nil
	}
	var parentId uint64
	if parentId, err = v.recursiveMakeDirectory(fixedPath); err != nil {
		log.LogErrorf("WriteObject: recursive make directory fail: volume(%v) path(%v) err(%v)",
			v.name, path, err)
		return
	}
	var lastPathItem = pathItems[len(pathItems)-1]
	if lastPathItem.IsDirectory {
		// If the last path node is a directory, then it has been processed by the previous logic.
		// Just get the information of this node and return.
		var info *proto.InodeInfo
		if info, err = v.mw.InodeGet_ll(parentId); err != nil {
			return
		}
		fsInfo = &FSFileInfo{
			Path:       path,
			Size:       0,
			Mode:       DefaultDirMode,
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
		return
	}
	if err == nil && os.FileMode(lookupMode).IsDir() {
		err = syscall.EISDIR
		return
	}

	// Intermediate data during the writing of new versions is managed through invisible files.
	// This file has only inode but no dentry. In this way, this temporary file can be made invisible
	// in the true sense. In order to avoid the adverse impact of other user operations on temporary data.
	var invisibleTempDataInode *proto.InodeInfo
	if invisibleTempDataInode, err = v.mw.InodeCreate_ll(DefaultFileMode, 0, 0, nil); err != nil {
		return
	}
	defer func() {
		// An error has caused the entire process to fail. Delete the inode and release the written data.
		if err != nil {
			log.LogWarnf("WriteObject: unlink temp inode: volume(%v) path(%v) inode(%v)",
				v.name, path, invisibleTempDataInode.Inode)
			_, _ = v.mw.InodeUnlink_ll(invisibleTempDataInode.Inode)
			log.LogWarnf("WriteObject: evict temp inode: volume(%v) path(%v) inode(%v)",
				v.name, path, invisibleTempDataInode.Inode)
			_ = v.mw.Evict(invisibleTempDataInode.Inode)
		}
	}()
	if err = v.ec.OpenStream(invisibleTempDataInode.Inode); err != nil {
		return
	}
	defer func() {
		if closeErr := v.ec.CloseStream(invisibleTempDataInode.Inode); closeErr != nil {
			log.LogErrorf("WriteObject: close stream fail: volume(%v) inode(%v) err(%v)",
				v.name, invisibleTempDataInode.Inode, closeErr)
		}
	}()

	var (
		md5Hash = md5.New()
		size    uint64
		etag    string
	)
	if size, err = v.streamWrite(invisibleTempDataInode.Inode, reader, md5Hash); err != nil {
		return
	}
	// compute file md5
	etag = hex.EncodeToString(md5Hash.Sum(nil))

	// flush
	if err = v.ec.Flush(invisibleTempDataInode.Inode); err != nil {
		log.LogErrorf("WriteObject: data flush inode fail, inode(%v) err(%v)", invisibleTempDataInode.Inode, err)
		return nil, err
	}

	var (
		existMode uint32
	)
	_, existMode, err = v.mw.Lookup_ll(parentId, lastPathItem.Name)
	if err != nil && err != syscall.ENOENT {
		log.LogErrorf("WriteObject: meta lookup fail: parentID(%v) name(%v) err(%v)", parentId, lastPathItem.Name, err)
		return
	}

	if err == syscall.ENOENT {
		if err = v.applyInodeToNewDentry(parentId, lastPathItem.Name, invisibleTempDataInode.Inode); err != nil {
			log.LogErrorf("WriteObject: apply inode to new dentry fail: parentID(%v) name(%v) inode(%v) err(%v)",
				parentId, lastPathItem.Name, invisibleTempDataInode.Inode, err)
			return
		}
		log.LogDebugf("WriteObject: apply inode to new dentry: parentID(%v) name(%v) inode(%v)",
			parentId, lastPathItem.Name, invisibleTempDataInode.Inode)
	} else {
		if os.FileMode(existMode).IsDir() {
			log.LogErrorf("WriteObject: target mode conflict: parentID(%v) name(%v) mode(%v)",
				parentId, lastPathItem.Name, os.FileMode(existMode).String())
			err = syscall.EEXIST
			return
		}
		if err = v.applyInodeToExistDentry(parentId, lastPathItem.Name, invisibleTempDataInode.Inode); err != nil {
			log.LogErrorf("WriteObject: apply inode to exist dentry fail: parentID(%v) name(%v) inode(%v) err(%v)",
				parentId, lastPathItem.Name, invisibleTempDataInode.Inode, err)
			return
		}
	}

	// Save ETag
	if err = v.mw.XAttrSet_ll(invisibleTempDataInode.Inode, []byte(XAttrKeyOSSETag), []byte(etag)); err != nil {
		log.LogErrorf("WriteObject: meta set xattr fail, inode(%v) key(%v) val(%v) err(%v)",
			invisibleTempDataInode, XAttrKeyOSSETag, etag, err)
		return nil, err
	}
	// If MIME information is valid, use extended attributes for storage.
	if mimeType != "" {
		if err = v.mw.XAttrSet_ll(invisibleTempDataInode.Inode, []byte(XAttrKeyOSSMIME), []byte(mimeType)); err != nil {
			log.LogErrorf("WriteObject: meta set xattr fail, inode(%v) key(%v) val(%v) err(%v)",
				invisibleTempDataInode, XAttrKeyOSSMIME, mimeType, err)
			return nil, err
		}
	}

	// create file info
	fsInfo = &FSFileInfo{
		Path:       path,
		Size:       int64(size),
		Mode:       os.FileMode(lookupMode),
		ModifyTime: time.Now(),
		ETag:       etag,
		Inode:      invisibleTempDataInode.Inode,
	}

	return fsInfo, nil
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
		dentries, err = v.mw.ReadDir_ll(ino)
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
	log.LogWarnf("DeletePath: evict: volume(%v) path(%v) inode(%v)", v.name, path, ino)
	if err = v.mw.Evict(ino); err != nil {
		log.LogWarnf("DeletePath Evict: path(%v) inode(%v)", path, ino)
	}
	err = nil
	return
}

func (v *Volume) InitMultipart(path string) (multipartID string, err error) {
	defer func() {
		log.LogInfof("Audit: InitMultipart: volume(%v) path(%v) multipartID(%v) err(%v)", v.name, path, multipartID, err)
	}()
	// Invoke meta service to get a session id
	// Create parent path

	dirs, _ := splitPath(path)
	// process path
	var parentId uint64
	if parentId, err = v.lookupDirectories(dirs, true); err != nil {
		log.LogErrorf("InitMultipart: lookup directories fail: path(%v) err(%v)", path, err)
		return "", err
	}

	// save parent id to meta
	multipartID, err = v.mw.InitMultipart_ll(path, parentId)
	if err != nil {
		log.LogErrorf("InitMultipart: meta init multipart fail: path(%v) err(%v)", path, err)
		return "", err
	}
	return multipartID, nil
}

func (v *Volume) WritePart(path string, multipartId string, partId uint16, reader io.Reader) (*FSFileInfo, error) {
	var err error
	defer func() {
		// Audit behavior
		log.LogInfof("Audit: WritePart: volume(%v) path(%v) multipartID(%v) partID(%v) err(%v)",
			v.name, path, multipartId, partId, err)
	}()
	var parentId uint64
	var fInfo *FSFileInfo
	dirs, fileName := splitPath(path)
	// process path
	if parentId, err = v.lookupDirectories(dirs, true); err != nil {
		log.LogErrorf("WritePart: lookup directories fail, path(%v) err(%v)", path, err)
		return nil, err
	}

	// create temp file (inode only, invisible for user)
	var tempInodeInfo *proto.InodeInfo
	if tempInodeInfo, err = v.mw.InodeCreate_ll(DefaultFileMode, 0, 0, nil); err != nil {
		log.LogErrorf("WritePart: meta create inode fail: multipartID(%v) partID(%v) err(%v)",
			multipartId, parentId, err)
		return nil, err
	}
	log.LogDebugf("WritePart: meta create temp file inode: multipartID(%v) partID(%v) inode(%v)",
		multipartId, parentId, tempInodeInfo.Inode)

	if err = v.ec.OpenStream(tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WritePart: data open stream fail, inode(%v) err(%v)", tempInodeInfo.Inode, err)
		return nil, err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(tempInodeInfo.Inode); closeErr != nil {
			log.LogErrorf("WritePart: data close stream fail, inode(%v) err(%v)", tempInodeInfo.Inode, closeErr)
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
	if err = v.ec.Flush(tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WritePart: data flush inode fail, inode(%v) err(%v)", tempInodeInfo.Inode, err)
		return nil, err
	}

	// save temp file MD5
	if err = v.mw.XAttrSet_ll(tempInodeInfo.Inode, []byte(XAttrKeyOSSETag), []byte(etag)); err != nil {
		log.LogErrorf("WritePart: meta set xattr fail, inode(%v) key(%v) val(%v) err(%v)",
			tempInodeInfo, XAttrKeyOSSETag, etag, err)
		return nil, err
	}

	// update temp file inode to meta with session
	if err = v.mw.AddMultipartPart_ll(multipartId, parentId, partId, size, etag, tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WritePart: meta add multipart part fail: multipartID(%v) parentID(%v) partID(%v) inode(%v) size(%v) MD5(%v) err(%v)",
			multipartId, parentId, parentId, tempInodeInfo.Inode, size, etag, err)
	}
	log.LogDebugf("WritePart: meta add multipart part: multipartID(%v) parentID(%v) partID(%v) inode(%v) size(%v) MD5(%v)",
		multipartId, parentId, parentId, tempInodeInfo.Inode, size, etag)

	// create file info
	fInfo = &FSFileInfo{
		Path:       fileName,
		Size:       int64(size),
		Mode:       os.FileMode(DefaultFileMode),
		ModifyTime: time.Now(),
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
	var parentId uint64

	dirs, _ := splitPath(path)
	// process path
	if parentId, err = v.lookupDirectories(dirs, true); err != nil {
		log.LogErrorf("AbortMultipart: lookup directories fail: volume(%v) multipartID(%v) path(%v) dirs(%v) err(%v)",
			v.name, multipartID, path, dirs, err)
		return err
	}
	// get multipart info
	var multipartInfo *proto.MultipartInfo
	if multipartInfo, err = v.mw.GetMultipart_ll(multipartID, parentId); err != nil {
		log.LogErrorf("AbortMultipart: meta get multipart fail: volume(%v) multipartID(%v) parentID(%v) path(%v) err(%v)",
			v.name, multipartID, parentId, path, err)
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

	if err = v.mw.RemoveMultipart_ll(multipartID, parentId); err != nil {
		log.LogErrorf("AbortMultipart: meta abort multipart fail: volume(%v) path(%v) multipartID(%v) parentID(%v) err(%v)",
			v.name, path, multipartID, parentId, err)
		return err
	}
	log.LogDebugf("AbortMultipart: meta abort multipart: volume(%v) path(%v) multipartID(%v) path(%v)",
		v.name, path, multipartID, path)
	return nil
}

func (v *Volume) CompleteMultipart(path string, multipartID string) (fsFileInfo *FSFileInfo, err error) {
	log.LogInfof("CompleteMultipart: volume(%v) path(%v) multipartID(%v)",
		v.name, path, multipartID)
	var parentId uint64

	dirs, filename := splitPath(path)
	// process path
	if parentId, err = v.lookupDirectories(dirs, true); err != nil {
		log.LogErrorf("CompleteMultipart: lookup directories fail, multipartID(%v) path(%v) dirs(%v) err(%v)",
			multipartID, path, dirs, err)
		return
	}

	// get multipart info
	var multipartInfo *proto.MultipartInfo
	if multipartInfo, err = v.mw.GetMultipart_ll(multipartID, parentId); err != nil {
		log.LogErrorf("CompleteMultipart: meta get multipart fail: volume(%v) multipartID(%v) parentID(%v) path(%v) err(%v)",
			v.name, multipartID, parentId, path, err)
		return
	}

	// sort multipart parts
	parts := multipartInfo.Parts
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })

	// create inode for complete data
	var completeInodeInfo *proto.InodeInfo
	if completeInodeInfo, err = v.mw.InodeCreate_ll(DefaultFileMode, 0, 0, nil); err != nil {
		log.LogErrorf("CompleteMultipart: meta inode create fail: volume(%v) err(%v)", v.name, err)
		return
	}
	log.LogDebugf("CompleteMultipart: meta inode create: inode(%v)", completeInodeInfo.Inode)
	defer func() {
		if err != nil {
			log.LogWarnf("CompleteMultipart: destroy inode: volume(%v) inode(%v)",
				v.name, completeInodeInfo.Inode)
			if deleteErr := v.mw.InodeDelete_ll(completeInodeInfo.Inode); deleteErr != nil {
				log.LogErrorf("CompleteMultipart: meta delete complete inode fail: volume(%v) inode(%v) err(%v)",
					v.name, completeInodeInfo.Inode, err)
			}
		}
	}()

	// merge complete extent keys
	var size uint64
	var completeExtentKeys = make([]proto.ExtentKey, 0)
	var fileOffset uint64
	for _, part := range parts {
		var eks []proto.ExtentKey
		if _, _, eks, err = v.mw.GetExtents(part.Inode); err != nil {
			log.LogErrorf("CompleteMultipart: meta get extents fail: volume(%v) inode(%v) err(%v)",
				v.name, part.Inode, err)
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

	// compute md5 hash
	var md5Val string
	if len(parts) == 1 {
		md5Val = parts[0].MD5
	} else {
		var md5Hash = md5.New()
		var reuseBuf = make([]byte, util.BlockSize)
		for _, part := range parts {
			if err = v.appendInodeHash(md5Hash, part.Inode, part.Size, reuseBuf); err != nil {
				log.LogErrorf("CompleteMultipart: append part hash fail: partID(%v) inode(%v) err(%v)",
					part.ID, part.Inode, err)
				return
			}
		}
		md5Val = hex.EncodeToString(md5Hash.Sum(nil))
	}
	log.LogDebugf("CompleteMultipart: merge parts: numParts(%v) MD5(%v)", len(parts), md5Val)

	if err = v.mw.AppendExtentKeys(completeInodeInfo.Inode, completeExtentKeys); err != nil {
		log.LogErrorf("CompleteMultipart: meta append extent keys fail: inode(%v) err(%v)", completeInodeInfo.Inode, err)
		return
	}

	var (
		existMode uint32
	)
	_, existMode, err = v.mw.Lookup_ll(parentId, filename)
	if err != nil && err != syscall.ENOENT {
		log.LogErrorf("CompleteMultipart: meta lookup fail: parentID(%v) name(%v) err(%v)", parentId, filename, err)
		return
	}

	if err == syscall.ENOENT {
		if err = v.applyInodeToNewDentry(parentId, filename, completeInodeInfo.Inode); err != nil {
			log.LogErrorf("CompleteMultipart: apply inode to new dentry fail: parentID(%v) name(%v) inode(%v) err(%v)",
				parentId, filename, completeInodeInfo.Inode, err)
			return
		}
		log.LogDebugf("CompleteMultipart: apply inode to new dentry: parentID(%v) name(%v) inode(%v)",
			parentId, filename, completeInodeInfo.Inode)
	} else {
		if os.FileMode(existMode).IsDir() {
			log.LogErrorf("CompleteMultipart: target mode conflict: parentID(%v) name(%v) mode(%v)",
				parentId, filename, os.FileMode(existMode).String())
			err = syscall.EEXIST
			return
		}
		if err = v.applyInodeToExistDentry(parentId, filename, completeInodeInfo.Inode); err != nil {
			log.LogErrorf("CompleteMultipart: apply inode to exist dentry fail: parentID(%v) name(%v) inode(%v) err(%v)",
				parentId, filename, completeInodeInfo.Inode, err)
			return
		}
	}

	if err = v.mw.XAttrSet_ll(completeInodeInfo.Inode, []byte(XAttrKeyOSSETag), []byte(md5Val)); err != nil {
		log.LogErrorf("CompleteMultipart: save MD5 fail: inode(%v) err(%v)", completeInodeInfo, err)
		return
	}

	// remove multipart
	err = v.mw.RemoveMultipart_ll(multipartID, parentId)
	if err != nil {
		log.LogErrorf("CompleteMultipart: meta complete multipart fail, multipartID(%v) path(%v) parentID(%v) err(%v)",
			multipartID, path, parentId, err)
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

	log.LogDebugf("CompleteMultipart: meta complete multipart: volume(%v) multipartID(%v) path(%v) parentID(%v) inode(%v) md5(%v)",
		v.name, multipartID, path, parentId, completeInodeInfo.Inode, md5Val)

	// create file info
	fInfo := &FSFileInfo{
		Path:       path,
		Size:       int64(size),
		Mode:       os.FileMode(DefaultFileMode),
		ModifyTime: time.Now(),
		ETag:       md5Val,
		Inode:      completeInodeInfo.Inode,
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
			if writeN, err = v.ec.Write(inode, offset, buf[:readN], false); err != nil {
				log.LogErrorf("streamWrite: data write tmp file fail, inode(%v) offset(%v) err(%v)", inode, offset, err)
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

	if err = v.ec.OpenStream(ino); err != nil {
		log.LogErrorf("ReadFile: data open stream fail, Inode(%v) err(%v)", ino, err)
		return err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(ino); closeErr != nil {
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
		n, err = v.ec.Read(ino, tmp, int(offset), readSize)
		if err != nil && err != io.EOF {
			log.LogErrorf("ReadFile: data read fail: volume(%v) path(%v) inode(%v) offset(%v) size(%v) err(%v)",
				v.name, path, ino, offset, size, err)
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

func (v *Volume) ObjectMeta(path string) (info *FSFileInfo, err error) {

	// process path
	var inode uint64
	var mode os.FileMode
	if _, inode, _, mode, err = v.recursiveLookupTarget(path); err != nil {
		return
	}

	// read file data
	var inoInfo *proto.InodeInfo
	if inoInfo, err = v.mw.InodeGet_ll(inode); err != nil {
		log.LogWarnf("ObjectMeta: get inode fail: volume(%v) path(%v) inode(%v)", v.name, path, inode)
		return
	}

	var etag string
	var mimeType string

	if mode.IsDir() {
		// Folder has specific ETag and MIME type.
		etag = EmptyContentMD5String
		mimeType = HeaderValueContentTypeDirectory
	} else {
		// Try to get the advanced attributes stored in the extended attributes.
		// The following advanced attributes apply to the object storage:
		// 1. Etag (MD5)
		// 2. MIME type
		var xattrs []*proto.XAttrInfo
		var xattrKeys = []string{XAttrKeyOSSETag, XAttrKeyOSSETagInvalid, XAttrKeyOSSMIME}
		if xattrs, err = v.mw.BatchGetXAttr([]uint64{inode}, xattrKeys); err != nil {
			log.LogErrorf("ObjectMeta: meta get xattr fail, volume(%v) inode(%v) path(%v) keys(%v) err(%v)",
				v.name, inode, path, strings.Join(xattrKeys, ","), err)
			return
		}
		if len(xattrs) > 0 && xattrs[0].Inode == inode {
			var xattr = xattrs[0]
			etag = string(xattr.Get(XAttrKeyOSSETag))
			if len(etag) == 0 {
				etag = string(xattr.Get(XAttrKeyOSSETagInvalid))
			}
			mimeType = string(xattr.Get(XAttrKeyOSSMIME))
		}
	}

	info = &FSFileInfo{
		Path:       path,
		Size:       int64(inoInfo.Size),
		Mode:       os.FileMode(inoInfo.Mode),
		ModifyTime: inoInfo.ModifyTime,
		ETag:       etag,
		Inode:      inoInfo.Inode,
		MIMEType:   mimeType,
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
		curIno, curMode, err = v.mw.Lookup_ll(ino, pathItem.Name)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("recursiveMakeDirectory: lookup fail, parentID(%v) name(%v) fail err(%v)",
				ino, pathItem.Name, err)
			return
		}
		if err == syscall.ENOENT {
			var info *proto.InodeInfo
			info, err = v.mw.Create_ll(ino, pathItem.Name, uint32(DefaultDirMode), 0, 0, nil)
			if err != nil {
				return
			}
			curIno, curMode = info.Inode, info.Mode
		}
		log.LogDebugf("recursiveMakeDirectory: lookup item: parentID(%v) inode(%v) name(%v) mode(%v)",
			ino, curIno, pathItem.Name, os.FileMode(curMode))
		// Check file mode
		if os.FileMode(curMode).IsDir() != pathItem.IsDirectory {
			err = syscall.ENOTDIR
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

func (v *Volume) listFilesV1(prefix, marker, delimiter string, maxKeys uint64) (infos []*FSFileInfo, prefixes Prefixes, err error) {
	var prefixMap = PrefixMap(make(map[string]struct{}))

	parentId, dirs, err := v.findParentId(prefix)

	// The method returns an ENOENT error, indicating that there
	// are no files or directories matching the prefix.
	if err == syscall.ENOENT {
		return nil, nil, nil
	}

	// Errors other than ENOENT are unexpected errors, method stops and returns it to the caller.
	if err != nil {
		log.LogErrorf("listFilesV1: find parent ID fail, prefix(%v) marker(%v) err(%v)", prefix, marker, err)
		return nil, nil, err
	}

	log.LogDebugf("listFilesV1: find parent ID, prefix(%v) marker(%v) delimiter(%v) parentId(%v) dirs(%v)", prefix, marker, delimiter, parentId, len(dirs))

	// recursion scan
	infos, prefixMap, err = v.recursiveScan(infos, prefixMap, parentId, maxKeys, dirs, prefix, marker, delimiter)
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

	log.LogDebugf("listFilesV1: Volume list dir: Volume(%v) prefix(%v) marker(%v) delimiter(%v) maxKeys(%v) infos(%v) prefixes(%v)",
		v.name, prefix, marker, delimiter, maxKeys, len(infos), len(prefixes))

	return
}

func (v *Volume) listFilesV2(prefix, startAfter, contToken, delimiter string, maxKeys uint64) (infos []*FSFileInfo, prefixes Prefixes, err error) {
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
		return nil, nil, nil
	}

	// Errors other than ENOENT are unexpected errors, method stops and returns it to the caller.
	if err != nil {
		log.LogErrorf("listFilesV2: find parent ID fail, prefix(%v) marker(%v) err(%v)", prefix, marker, err)
		return nil, nil, err
	}

	log.LogDebugf("listFilesV2: find parent ID, prefix(%v) marker(%v) delimiter(%v) parentId(%v) dirs(%v)", prefix, marker, delimiter, parentId, len(dirs))

	// recursion scan
	infos, prefixMap, err = v.recursiveScan(infos, prefixMap, parentId, maxKeys, dirs, prefix, marker, delimiter)
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

	log.LogDebugf("listFilesV2: Volume list dir: Volume(%v) prefix(%v) marker(%v) delimiter(%v) maxKeys(%v) infos(%v) prefixes(%v)",
		v.name, prefix, marker, delimiter, maxKeys, len(infos), len(prefixes))

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
func (v *Volume) recursiveScan(fileInfos []*FSFileInfo, prefixMap PrefixMap, parentId, maxKeys uint64,
	dirs []string, prefix, marker, delimiter string) ([]*FSFileInfo, PrefixMap, error) {
	var err error

	var currentPath = strings.Join(dirs, pathSep) + pathSep
	if len(dirs) > 0 && prefix != "" && strings.HasSuffix(currentPath, prefix) {
		// When the current scanning position is not the root directory, a prefix matching
		// check is performed on the current directory first.
		//
		// The reason for this is that according to the definition of Amazon S3's ListObjects
		// interface, directory entries that meet the exact prefix match will be returned as
		// a Content result, not as a CommonPrefix.
		fileInfo := &FSFileInfo{
			Inode: parentId,
			Path:  currentPath,
		}
		fileInfos = append(fileInfos, fileInfo)

		// If the number of matches reaches the threshold given by maxKey,
		// stop scanning and return results.
		if len(fileInfos) >= int(maxKeys+1) {
			return fileInfos, prefixMap, nil
		}
	}

	// During the process of scanning the child nodes of the current directory, there may be other
	// parallel operations that may delete the current directory.
	// If got the syscall.ENOENT error when invoke readdir, it means that the above situation has occurred.
	// At this time, stops process and returns success.
	var children []proto.Dentry
	children, err = v.mw.ReadDir_ll(parentId)
	if err != nil && err != syscall.ENOENT {
		return fileInfos, prefixMap, err
	}
	if err == syscall.ENOENT {
		return fileInfos, prefixMap, nil
	}

	for _, child := range children {

		var path = strings.Join(append(dirs, child.Name), pathSep)

		if os.FileMode(child.Type).IsDir() {
			path += pathSep
		}
		log.LogDebugf("recursiveScan: process child, path(%v) prefix(%v) marker(%v) delimiter(%v)",
			path, prefix, marker, delimiter)

		if prefix != "" && !strings.HasPrefix(path, prefix) {
			continue
		}
		if marker != "" && path < marker {
			continue
		}
		if delimiter != "" {
			var nonPrefixPart = strings.Replace(path, prefix, "", 1)
			if idx := strings.Index(nonPrefixPart, delimiter); idx >= 0 {
				var commonPrefix = prefix + util.SubString(nonPrefixPart, 0, idx) + delimiter
				prefixMap.AddPrefix(commonPrefix)
				continue
			}
		}
		if os.FileMode(child.Type).IsDir() {
			fileInfos, prefixMap, err = v.recursiveScan(fileInfos, prefixMap, child.Inode, maxKeys, append(dirs, child.Name), prefix, marker, delimiter)
			if err != nil {
				return fileInfos, prefixMap, err
			}
		} else {
			fileInfo := &FSFileInfo{
				Inode: child.Inode,
				Path:  path,
			}
			fileInfos = append(fileInfos, fileInfo)

			// if file numbers is enough, end list dir
			if len(fileInfos) >= int(maxKeys+1) {
				return fileInfos, prefixMap, nil
			}
		}
	}
	return fileInfos, prefixMap, nil
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
			fileInfo.Mode = os.FileMode(inodeInfos[i].Mode)
		}
	}

	// Get MD5 information in batches, then update to fileInfos
	keys := []string{XAttrKeyOSSETag, XAttrKeyOSSETagInvalid}
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
			fileInfo.ETag = EmptyContentMD5String
			continue
		}
		i := sort.Search(len(xattrs), func(i int) bool {
			return xattrs[i].Inode >= fileInfo.Inode
		})
		if i >= 0 && i < len(xattrs) && xattrs[i].Inode == fileInfo.Inode {
			etag, etagInvalid := xattrs[i].Get(XAttrKeyOSSETag), xattrs[i].Get(XAttrKeyOSSETagInvalid)
			if len(etag) != 0 {
				fileInfo.ETag = string(etag)
			}
			if len(etagInvalid) != 0 {
				fileInfo.ETag = string(etagInvalid)
			}
		}
	}
	return
}

func (v *Volume) ListMultipartUploads(prefix, delimiter, keyMarker string, multipartIdMarker string,
	maxUploads uint64) ([]*FSUpload, string, string, bool, []string, error) {
	sessions, err := v.mw.ListMultipart_ll(prefix, delimiter, keyMarker, multipartIdMarker, maxUploads)
	if err != nil {
		return nil, "", "", false, nil, err
	}

	uploads := make([]*FSUpload, 0)
	prefixes := make([]string, 0)
	prefixMap := make(map[string]string)

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
		if delimiter != "" && strings.Contains(session.Path, delimiter) {
			idx := strings.Index(session.Path, delimiter)
			prefix := util.SubString(session.Path, 0, idx)
			prefixes = append(prefixes, prefix)
			continue
		}
		fsUpload := &FSUpload{
			Key:          session.Path,
			UploadId:     session.ID,
			Initiated:    formatTimeISO(session.InitTime),
			StorageClass: StorageClassStandard,
		}
		uploads = append(uploads, fsUpload)
	}
	if len(prefixMap) > 0 {
		for prefix := range prefixMap {
			prefixes = append(prefixes, prefix)
		}
	}

	return uploads, NextMarker, NextSessionIdMarker, IsTruncated, prefixes, nil
}

func (v *Volume) ListParts(path, uploadId string, maxParts, partNumberMarker uint64) (parts []*FSPart, nextMarker uint64, isTruncated bool, err error) {
	var parentId uint64
	dirs, _ := splitPath(path)
	// process path
	if parentId, err = v.lookupDirectories(dirs, true); err != nil {
		return nil, 0, false, err
	}

	multipartInfo, err := v.mw.GetMultipart_ll(uploadId, parentId)
	if err != nil {
		log.LogErrorf("ListPart: get multipart upload fail: volume(%v) uploadID(%v) err(%v)", v.name, uploadId, err)
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

func (v *Volume) CopyFile(targetPath, sourcePath string) (info *FSFileInfo, err error) {

	sourceDirs, sourceFilename := splitPath(sourcePath)
	// process source targetPath
	var sourceParentId uint64
	if sourceParentId, err = v.lookupDirectories(sourceDirs, false); err != nil {
		return nil, err
	}

	// find source file inode
	var sourceFileInode uint64
	var lookupMode uint32
	sourceFileInode, lookupMode, err = v.mw.Lookup_ll(sourceParentId, sourceFilename)
	if err != nil {
		return nil, err
	}
	if os.FileMode(lookupMode).IsDir() {
		return nil, syscall.ENOENT
	}

	// get new file parent id
	newDirs, newFilename := splitPath(targetPath)
	// process source targetPath
	var newParentId uint64
	if newParentId, err = v.lookupDirectories(newDirs, true); err != nil {
		return nil, err
	}

	inodeInfo, err := v.copyFile(newParentId, newFilename, sourceFileInode, DefaultFileMode)
	if err != nil {
		log.LogErrorf("CopyFile: meta copy file fail: newParentID(%v) newName(%v) sourceIno(%v) err(%v)",
			newParentId, newFilename, sourceFileInode, err)
		return nil, err
	}

	var xattrs []*proto.XAttrInfo
	var xattrKeys = []string{XAttrKeyOSSETag, XAttrKeyOSSETagInvalid}
	if xattrs, err = v.mw.BatchGetXAttr([]uint64{sourceFileInode}, xattrKeys); err != nil {
		log.LogErrorf("CopyFile: meta get xattr fail, volume(%v) inode(%v) path(%v) keys(%v) err(%v)",
			v.name, sourceFileInode, targetPath, strings.Join(xattrKeys, ","), err)
		return
	}
	var etag string
	var mimeType string
	if len(xattrs) > 0 && xattrs[0].Inode == sourceFileInode {
		var xattr = xattrs[0]
		etag = string(xattr.Get(XAttrKeyOSSETag))
		if len(etag) == 0 {
			etag = string(xattr.Get(XAttrKeyOSSETagInvalid))
		}
		mimeType = string(xattr.Get(XAttrKeyOSSMIME))
	}

	info = &FSFileInfo{
		Path:       targetPath,
		Size:       int64(inodeInfo.Size),
		Mode:       os.FileMode(inodeInfo.Size),
		ModifyTime: inodeInfo.ModifyTime,
		ETag:       etag,
		Inode:      inodeInfo.Inode,
		MIMEType:   mimeType,
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
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = metaWrapper.Close()
		}
	}()
	var extentConfig = &stream.ExtentConfig{
		Volume:            config.Volume,
		Masters:           config.Masters,
		FollowerRead:      true,
		OnAppendExtentKey: metaWrapper.AppendExtentKey,
		OnGetExtents:      metaWrapper.GetExtents,
		OnTruncate:        metaWrapper.Truncate,
	}
	var extentClient *stream.ExtentClient
	if extentClient, err = stream.NewExtentClient(extentConfig); err != nil {
		return nil, err
	}

	v := &Volume{
		mw:         metaWrapper,
		ec:         extentClient,
		name:       config.Volume,
		store:      config.Store,
		om:         new(OSSMeta),
		createTime: metaWrapper.VolCreateTime(),
		closeCh:    make(chan struct{}),
		onAsyncTaskError: func(err error) {
			if err == syscall.ENOENT {
				config.OnAsyncTaskError.OnError(proto.ErrVolNotExists)
			}
		},
	}
	go v.syncOSSMeta()
	return v, nil
}
