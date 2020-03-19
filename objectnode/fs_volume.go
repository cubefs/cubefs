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
	"github.com/tiglabs/raft/logger"
)

const (
	rootIno               = proto.RootIno
	OSSMetaUpdateDuration = time.Duration(time.Second * 30)
)

// Used to validate interface implementation
var _ Volume = &volume{}

type OSSMeta struct {
	policy     *Policy
	acl        *AccessControlPolicy
	policyLock sync.RWMutex
	aclLock    sync.RWMutex
}

func (v *volume) loadPolicy() (p *Policy) {
	v.om.policyLock.RLock()
	p = v.om.policy
	v.om.policyLock.RUnlock()
	return
}

func (v *volume) storePolicy(p *Policy) {
	v.om.policyLock.Lock()
	v.om.policy = p
	v.om.policyLock.Unlock()
	return
}

func (v *volume) loadACL() (p *AccessControlPolicy) {
	v.om.aclLock.RLock()
	p = v.om.acl
	v.om.aclLock.RUnlock()
	return
}

func (v *volume) storeACL(p *AccessControlPolicy) {
	v.om.aclLock.Lock()
	v.om.acl = p
	v.om.aclLock.Unlock()
	return
}

// This struct is implementation of Volume interface
type volume struct {
	mw     *meta.MetaWrapper
	ec     *stream.ExtentClient
	vm     *volumeManager
	name   string
	om     *OSSMeta
	ticker *time.Ticker
	//ms     Store

	closeOnce sync.Once
	closingCh chan struct{}
	closedCh  chan struct{}
}

func (v *volume) syncOSSMeta() {
	defer v.ticker.Stop()
	v.ticker = time.NewTicker(OSSMetaUpdateDuration)
	for {
		select {
		case <-v.ticker.C:
			v.loadOSSMeta()
		case <-v.closingCh:
			v.closedCh <- struct{}{}
			return
		}
	}
}

func (v *volume) stopOSSMetaSync() {
	v.closingCh <- struct{}{}

	select {
	case <-v.closedCh:
		break
	}
}

// update volume meta info
func (v *volume) loadOSSMeta() {
	policy, _ := v.loadBucketPolicy()
	if policy != nil {
		v.storePolicy(policy)
	}

	acl, _ := v.loadBucketACL()
	if acl != nil {
		v.storeACL(acl)
	}
}

// load bucket policy from vm
func (v *volume) loadBucketPolicy() (policy *Policy, err error) {
	var store Store
	store, err = v.vm.GetStore()
	if err != nil {
		return
	}
	var data []byte
	data, err = store.Get(v.name, bucketRootPath, XAttrKeyOSSPolicy)
	if err != nil {
		log.LogErrorf("loadBucketPolicy: load bucket policy fail: volume(%v) err(%v)", v.name, err)
		return
	}
	policy = &Policy{}
	if err = json.Unmarshal(data, policy); err != nil {
		return
	}
	return
}

func (v *volume) loadBucketACL() (*AccessControlPolicy, error) {
	store, err1 := v.vm.GetStore()
	if err1 != nil {
		return nil, err1
	}
	data, err2 := store.Get(v.name, bucketRootPath, OSS_ACL_KEY)
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

func (v *volume) OSSMeta() *OSSMeta {
	return v.om
}

func (v *volume) getInodeFromPath(path string) (inode uint64, err error) {
	// path == "/"
	if path == "/" {
		return volumeRootInode, nil
	}

	dirs, filename := splitPath(path)
	log.LogInfof("dirs %v %v %v", path, len(dirs), filename)

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

func (v *volume) SetXAttr(path string, key string, data []byte) error {
	inode, err1 := v.getInodeFromPath(path)
	if err1 != nil {
		return err1
	}
	return v.mw.XAttrSet_ll(inode, []byte(key), data)
}

func (v *volume) GetXAttr(path string, key string) (info *proto.XAttrInfo, err error) {
	var inode uint64
	inode, err = v.getInodeFromPath(path)
	if err != nil {
		return
	}
	if info, err = v.mw.XAttrGet_ll(inode, key); err != nil {
		log.LogErrorf("GetXAttr: meta get xattr fail: path(%v) inode(%v) err(%v)", path, inode, err)
		return
	}
	return
}

func (v *volume) DeleteXAttr(path string, key string) (err error) {
	inode, err1 := v.getInodeFromPath(path)
	if err1 != nil {
		err = err1
		return
	}
	if err = v.mw.XAttrDel_ll(inode, key); err != nil {
		log.LogErrorf("SetXAttr: meta set xattr fail: path(%v) inode(%v) err(%v)", path, inode, err)
		return
	}
	return
}

func (v *volume) OSSSecure() (accessKey, secretKey string) {
	return v.mw.OSSSecure()
}

func (v *volume) ListFilesV1(request *ListBucketRequestV1) ([]*FSFileInfo, string, bool, []string, error) {
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

func (v *volume) ListFilesV2(request *ListBucketRequestV2) ([]*FSFileInfo, uint64, string, bool, []string, error) {

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

func (v *volume) WriteFile(path string, reader io.Reader) (*FSFileInfo, error) {
	var err error
	var fInfo *FSFileInfo

	dirs, filename := splitPath(path)

	// process path
	var parentId uint64
	if parentId, err = v.lookupDirectories(dirs, true); err != nil {
		return nil, err
	}
	log.LogDebugf("WriteFile: lookup directories, path(%v) parentId(%v)", path, parentId)
	// check file
	var lookupMode uint32
	_, lookupMode, err = v.mw.Lookup_ll(parentId, filename)
	if err != nil && err != syscall.ENOENT {
		return nil, err
	}
	if err == nil && os.FileMode(lookupMode).IsDir() {
		return nil, syscall.EEXIST
	}
	// create  temp file
	var tempFilename = tempFileName(filename)
	var tempInodeInfo *proto.InodeInfo
	if tempInodeInfo, err = v.mw.Create_ll(parentId, tempFilename, 0600, 0, 0, nil); err != nil {
		log.LogErrorf("WriteFile: create temp file fail, parent(%v) name(%v) err(%v)", parentId, tempFilename, err)
		return nil, err
	}
	log.LogDebugf("WriteFile: create temp file, parent(%v) name(%v) inode(%v)", parentId, tempFilename, tempInodeInfo.Inode)

	// write data
	var buf = make([]byte, 128*1024)
	var readN, writeN, offset int

	var fileMd5 string
	var md5Buf = make([]byte, 128*1024)
	var md5Hash = md5.New()

	if err = v.ec.OpenStream(tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WriteFile: open stream fail, inode(%v) err(%v)", tempInodeInfo.Inode, err)
		return nil, err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(tempInodeInfo.Inode); closeErr != nil {
			log.LogErrorf("WriteFile: close stream fail, inode(%v) err(%v)", tempInodeInfo.Inode, closeErr)
		}
		if evictErr := v.ec.EvictStream(tempInodeInfo.Inode); evictErr != nil {
			log.LogErrorf("WriteFile: evict stream fail, inode(%v) err(%v)", tempInodeInfo.Inode, evictErr)
		}
	}()

	for {
		readN, err = reader.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if readN > 0 {
			if writeN, err = v.ec.Write(tempInodeInfo.Inode, offset, buf[:readN], false); err != nil {
				log.LogErrorf("WriteFile: write data fail, inode(%v) offset(%v) err(%v)", tempInodeInfo.Inode, offset, err)
				return nil, err
			}
			log.LogDebugf("WriteFile: write data, inode(%v) offset(%v) bytes(%v)", tempInodeInfo.Inode, offset, readN)
			offset += writeN
			// copy to md5 buffer, and then write to md5
			copy(md5Buf, buf[:readN])
			md5Hash.Write(md5Buf[:readN])
		}
		if err == io.EOF {
			break
		}
	}

	// compute file md5
	fileMd5 = hex.EncodeToString(md5Hash.Sum(nil))

	// flush
	if err = v.ec.Flush(tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WriteFile: flush data fail, inode(%v) err(%v)", tempInodeInfo.Inode, err)
		return nil, err
	}

	// rename temp file to origin
	if err = v.mw.Rename_ll(parentId, tempFilename, parentId, filename); err != nil {
		log.LogErrorf("WriteFile: rename temp file fail, (%v_%v) to (%v_%v) err(%v)", parentId, tempFilename, parentId, filename, err)
		return nil, err
	}
	log.LogDebugf("WriteFile: rename temp file, (%v_%v) to (%v_%v)",
		parentId, tempFilename, parentId, filename)

	// set file md5 to meta data
	if err = v.mw.XAttrSet_ll(tempInodeInfo.Inode, []byte(XAttrKeyOSSETag), []byte(fileMd5)); err != nil {
		log.LogErrorf("WriteFile: set xattr fail, inode(%v) key(%v) val(%v) err(%v)", tempInodeInfo.Inode, XAttrKeyOSSETag, fileMd5, err)
		return nil, err
	}

	// create file info
	fInfo = &FSFileInfo{
		Path:       path,
		Size:       int64(writeN),
		Mode:       os.FileMode(lookupMode),
		ModifyTime: time.Now(),
		ETag:       fileMd5,
		Inode:      tempInodeInfo.Inode,
	}

	return fInfo, nil
}

func (v *volume) DeleteFile(path string) error {
	var err error
	dirs, filename := splitPath(path)

	// process path
	var parentId uint64
	if parentId, err = v.lookupDirectories(dirs, false); err != nil {
		return err
	}
	// lookup target inode
	var inode uint64
	var mode uint32
	if inode, mode, err = v.mw.Lookup_ll(parentId, filename); err != nil {
		return err
	}
	if os.FileMode(mode).IsDir() {
		return syscall.ENOENT
	}

	// remove file
	if _, err = v.mw.Delete_ll(parentId, filename, false); err != nil {
		return err
	}

	// evict inode
	if err = v.ec.EvictStream(inode); err != nil {
		return err
	}
	if err = v.mw.Evict(inode); err != nil {
		return err
	}

	return nil
}

func (v *volume) InitMultipart(path string) (multipartID string, err error) {
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

func (v *volume) WritePart(path string, multipartId string, partId uint16, reader io.Reader) (*FSFileInfo, error) {
	var parentId uint64
	var err error
	var fInfo *FSFileInfo

	dirs, fileName := splitPath(path)
	// process path
	if parentId, err = v.lookupDirectories(dirs, true); err != nil {
		log.LogErrorf("WritePart: lookup directories fail, path(%v) err(%v)", path, err)
		return nil, err
	}

	// create temp file (inode only, invisible for user)
	var tempInodeInfo *proto.InodeInfo
	if tempInodeInfo, err = v.mw.InodeCreate_ll(0600, 0, 0, nil); err != nil {
		log.LogErrorf("WritePart: meta create inode fail: multipartID(%v) partID(%v) err(%v)",
			multipartId, parentId, err)
		return nil, err
	}
	log.LogDebugf("WritePart: meta create temp file inode: multipartID(%v) partID(%v) inode(%v)",
		multipartId, parentId, tempInodeInfo.Inode)

	// write data
	var buf = make([]byte, 128*1024)
	var readN, writeN, offset int

	var fileMd5 string
	var md5Buf = make([]byte, 128*1024)
	var md5Hash = md5.New()
	var size uint64

	if err = v.ec.OpenStream(tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WritePart: data open stream fail, inode(%v) err(%v)", tempInodeInfo.Inode, err)
		return nil, err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(tempInodeInfo.Inode); closeErr != nil {
			log.LogErrorf("WritePart: data close stream fail, inode(%v) err(%v)", tempInodeInfo.Inode, closeErr)
		}
	}()

	for {
		readN, err = reader.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if readN > 0 {
			if writeN, err = v.ec.Write(tempInodeInfo.Inode, offset, buf[:readN], false); err != nil {
				log.LogErrorf("WritePart: data write tmp file fail, inode(%v) offset(%v) err(%v)", tempInodeInfo.Inode, offset, err)
				return nil, err
			}
			offset += writeN
			// copy to md5 buffer, and then write to md5
			size += uint64(readN)
			copy(md5Buf, buf[:readN])
			md5Hash.Write(md5Buf[:readN])
		}
		if err == io.EOF {
			break
		}
	}

	// compute file md5
	fileMd5 = hex.EncodeToString(md5Hash.Sum(nil))

	// flush
	if err = v.ec.Flush(tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WritePart: data flush inode fail, inode(%v) err(%v)", tempInodeInfo.Inode, err)
		return nil, err
	}

	// save temp file MD5
	if err = v.mw.XAttrSet_ll(tempInodeInfo.Inode, []byte(XAttrKeyOSSETag), []byte(fileMd5)); err != nil {
		log.LogErrorf("WritePart: meta set xattr fail, inode(%v) key(%v) val(%v) err(%v)",
			tempInodeInfo, XAttrKeyOSSETag, fileMd5, err)
		return nil, err
	}

	// update temp file inode to meta with session
	if err = v.mw.AddMultipartPart_ll(multipartId, parentId, partId, size, fileMd5, tempInodeInfo.Inode); err != nil {
		log.LogErrorf("WritePart: meta add multipart part fail: multipartID(%v) parentID(%v) partID(%v) inode(%v) size(%v) MD5(%v) err(%v)",
			multipartId, parentId, parentId, tempInodeInfo.Inode, size, fileMd5, err)
	}
	log.LogDebugf("WritePart: meta add multipart part: multipartID(%v) parentID(%v) partID(%v) inode(%v) size(%v) MD5(%v)",
		multipartId, parentId, parentId, tempInodeInfo.Inode, size, fileMd5)

	// create file info
	fInfo = &FSFileInfo{
		Path:       fileName,
		Size:       int64(writeN),
		Mode:       os.FileMode(0600),
		ModifyTime: time.Now(),
		ETag:       fileMd5,
		Inode:      tempInodeInfo.Inode,
	}
	return fInfo, nil
}

func (v *volume) AbortMultipart(path string, multipartID string) (err error) {
	// TODO: cleanup data while abort multipart
	var parentId uint64

	dirs, _ := splitPath(path)
	// process path
	if parentId, err = v.lookupDirectories(dirs, true); err != nil {
		log.LogErrorf("AbortMultipart: lookup directories fail, multipartID(%v) path(%v) dirs(%v) err(%v)",
			multipartID, path, dirs, err)
		return err
	}
	// get multipart info
	var multipartInfo *proto.MultipartInfo
	if multipartInfo, err = v.mw.GetMultipart_ll(multipartID, parentId); err != nil {
		log.LogErrorf("AbortMultipart: meta get multipart fail, multipartID(%v) parentID(%v) path(%v) err(%v)",
			multipartID, parentId, path, err)
		return
	}
	// release part data
	for _, part := range multipartInfo.Parts {
		if _, err = v.mw.InodeUnlink_ll(part.Inode); err != nil {
			log.LogErrorf("AbortMultipart: meta inode unlink fail: multipartID(%v) partID(%v) inode(%v) err(%v)",
				multipartID, part.ID, part.Inode, err)
			return
		}
		if err = v.mw.Evict(part.Inode); err != nil {
			log.LogErrorf("AbortMultipart: meta inode evict fail: multipartID(%v) partID(%v) inode(%v) err(%v)",
				multipartID, part.ID, part.Inode, err)
		}
		log.LogDebugf("AbortMultipart: multipart part data released: multipartID(%v) partID(%v) inode(%v)",
			multipartID, part.ID, part.Inode)
	}

	if err = v.mw.RemoveMultipart_ll(multipartID, parentId); err != nil {
		log.LogErrorf("AbortMultipart: meta abort multipart fail, multipartID(%v) parentID(%v) err(%v)",
			multipartID, parentId, err)
		return err
	}
	log.LogDebugf("AbortMultipart: meta abort multipart, multipartID(%v) path(%v)",
		multipartID, path)
	return nil
}

func (v *volume) CompleteMultipart(path string, multipartID string) (fsFileInfo *FSFileInfo, err error) {

	const mode = 0600

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
		log.LogErrorf("CompleteMultipart: meta get multipart fail, multipartID(%v) parentID(%v) path(%v) err(%v)",
			multipartID, parentId, path, err)
		return
	}

	// sort multipart parts
	parts := multipartInfo.Parts
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })

	// create inode for complete data
	var completeInodeInfo *proto.InodeInfo
	if completeInodeInfo, err = v.mw.InodeCreate_ll(mode, 0, 0, nil); err != nil {
		log.LogErrorf("CompleteMultipart: meta inode create fail: err(%v)", err)
		return
	}
	log.LogDebugf("CompleteMultipart: meta inode create: inode(%v)", completeInodeInfo.Inode)
	defer func() {
		if err != nil {
			if deleteErr := v.mw.InodeDelete_ll(completeInodeInfo.Inode); deleteErr != nil {
				log.LogErrorf("CompleteMultipart: meta delete complete inode fail: inode(%v) err(%v)",
					completeInodeInfo.Inode, err)
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
			log.LogErrorf("CompleteMultipart: meta get extents fail: inode(%v) err(%v)", part.Inode, err)
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
		if err = v.mw.InodeDelete_ll(part.Inode); err != nil {
			log.LogErrorf("CompleteMultipart: ")
		}
	}

	log.LogDebugf("CompleteMultipart: meta complete multipart, multipartID(%v) path(%v) parentID(%v) inode(%v) md5(%v)",
		multipartID, path, parentId, completeInodeInfo.Inode, md5Val)

	// create file info
	fInfo := &FSFileInfo{
		Path:       path,
		Size:       int64(size),
		Mode:       os.FileMode(0600),
		ModifyTime: time.Now(),
		ETag:       md5Val,
		Inode:      completeInodeInfo.Inode,
	}
	return fInfo, nil
}

func (v *volume) appendInodeHash(h hash.Hash, inode uint64, total uint64, preAllocatedBuf []byte) (err error) {
	if err = v.ec.OpenStream(inode); err != nil {
		log.LogErrorf("appendInodeHash: data open stream fail: inode(%v) err(%v)",
			inode, err)
		return
	}
	defer func() {
		if closeErr := v.ec.CloseStream(inode); closeErr != nil {
			log.LogErrorf("appendInodeHash: data close stream fail: inode(%v) err(%v)",
				inode, err)
		}
		if evictErr := v.ec.EvictStream(inode); evictErr != nil {
			log.LogErrorf("appendInodeHash: data evict stream: inode(%v) err(%v)",
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

func (v *volume) applyInodeToNewDentry(parentID uint64, name string, inode uint64) (err error) {
	const mode = 0600
	if err = v.mw.DentryCreate_ll(parentID, name, inode, mode); err != nil {
		log.LogErrorf("applyInodeToNewDentry: meta dentry create fail: parentID(%v) name(%v) inode(%v) mode(%v) err(%v)",
			parentID, name, inode, mode, err)
		return err
	}
	return
}

func (v *volume) applyInodeToExistDentry(parentID uint64, name string, inode uint64) (err error) {
	var oldInode uint64
	oldInode, err = v.mw.DentryUpdate_ll(parentID, name, inode)
	if err != nil {
		log.LogErrorf("applyInodeToExistDentry: meta update dentry fail: parentID(%v) name(%v) inode(%v) err(%v)",
			parentID, name, inode, err)
		return
	}

	defer func() {
		if err != nil {
			// rollback dentry update
			if _, updateErr := v.mw.DentryUpdate_ll(parentID, name, oldInode); updateErr != nil {
				log.LogErrorf("CompleteMultipart: meta rollback dentry update fail: parentID(%v) name(%v) inode(%v) err(%v)",
					parentID, name, oldInode, updateErr)
			}
		}
	}()

	// unlink and evict old inode
	if _, err = v.mw.InodeUnlink_ll(oldInode); err != nil {
		log.LogErrorf("CompleteMultipart: meta unlink old inode fail: inode(%v) err(%v)",
			oldInode, err)
		return
	}

	defer func() {
		if err != nil {
			// rollback unlink
			if _, linkErr := v.mw.InodeLink_ll(oldInode); linkErr != nil {
				log.LogErrorf("CompleteMultipart: meta rollback inode unlink fail: inode(%v) err(%v)",
					oldInode, linkErr)
			}
		}
	}()

	if err = v.mw.Evict(oldInode); err != nil {
		log.LogErrorf("CompleteMultipart: meta evict old inode fail: inode(%v) err(%v)",
			oldInode, err)
		return
	}
	return
}

func (v *volume) ReadFile(path string, writer io.Writer, offset, size uint64) error {
	var err error
	dirs, filename := splitPath(path)

	// process path
	var parentId uint64
	if parentId, err = v.lookupDirectories(dirs, false); err != nil {
		return err
	}
	log.LogDebugf("ReadFile: lookup directories, path(%v) parentId(%v)", path, parentId)
	// check file
	var fileInode uint64
	var lookupMode uint32
	fileInode, lookupMode, err = v.mw.Lookup_ll(parentId, filename)
	if err != nil {
		return err
	}
	if os.FileMode(lookupMode).IsDir() {
		return syscall.ENOENT
	}
	// read file data
	var fileInodeInfo *proto.InodeInfo
	fileInodeInfo, err = v.mw.InodeGet_ll(fileInode)
	if err != nil {
		return err
	}

	if err = v.ec.OpenStream(fileInode); err != nil {
		log.LogErrorf("ReadFile: data open stream fail, Inode(%v) err(%v)", fileInode, err)
		return err
	}
	defer func() {
		if closeErr := v.ec.CloseStream(fileInode); closeErr != nil {
			log.LogErrorf("ReadFile: data close stream fail: inode(%v) err(%v)", fileInode, closeErr)
		}
	}()

	var upper = size + offset
	if upper > fileInodeInfo.Size {
		upper = fileInodeInfo.Size - offset
	}

	var n int
	var tmp = make([]byte, util.BlockSize)
	for {
		var rest = upper - uint64(offset)
		if rest == 0 {
			break
		}
		var readSize = len(tmp)
		if uint64(readSize) > rest {
			readSize = int(rest)
		}
		n, err = v.ec.Read(fileInode, tmp, int(offset), readSize)
		if err != nil && err != io.EOF {
			log.LogErrorf("ReadFile: data read fail, inode(%v) offset(%v) size(%v) err(%v)", fileInode, offset, size, err)
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

func (v *volume) FileInfo(path string) (info *FSFileInfo, err error) {
	dirs, filename := splitPath(path)

	// process path
	var parentId uint64
	if parentId, err = v.lookupDirectories(dirs, false); err != nil {
		return
	}
	// check file
	var fileInode uint64
	var lookupMode uint32
	if fileInode, lookupMode, err = v.mw.Lookup_ll(parentId, filename); err != nil {
		return
	}
	if os.FileMode(lookupMode).IsDir() {
		return nil, syscall.ENOENT
	}
	// read file data
	var fileInodeInfo *proto.InodeInfo
	if fileInodeInfo, err = v.mw.InodeGet_ll(fileInode); err != nil {
		return
	}

	var xAttrInfo *proto.XAttrInfo
	if xAttrInfo, err = v.mw.XAttrGet_ll(fileInode, XAttrKeyOSSETag); err != nil {
		logger.Error("FileInfo: meta get xattr fail, inode(%v) path(%v) err(%v)", fileInode, path, err)
		return
	}
	md5Val := xAttrInfo.XAttrs[XAttrKeyOSSETag]

	info = &FSFileInfo{
		Path:       path,
		Size:       int64(fileInodeInfo.Size),
		Mode:       os.FileMode(fileInodeInfo.Mode),
		ModifyTime: fileInodeInfo.ModifyTime,
		ETag:       md5Val,
		Inode:      fileInodeInfo.Inode,
	}
	return
}

func (v *volume) Close() error {
	v.closeOnce.Do(func() {
		v.mw.Close()
		v.stopOSSMetaSync()
	})
	return nil
}

func (v *volume) lookupDirectories(dirs []string, autoCreate bool) (inode uint64, err error) {
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
			inodeInfo, createErr = v.mw.Create_ll(parentId, dir, uint32(os.ModeDir), 0, 0, nil)
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

func (v *volume) listFilesV1(prefix, marker, delimiter string, maxKeys uint64) (infos []*FSFileInfo, prefixes Prefixes, err error) {
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

	// recursion call listDir method
	infos, prefixMap, err = v.listDir(infos, prefixMap, parentId, maxKeys, dirs, prefix, marker, delimiter)
	if err != nil {
		log.LogErrorf("listFilesV1: volume list dir fail: volume(%v) err(%v)", v.name, err)
		return
	}

	// supply size and MD5
	if err = v.supplyListFileInfo(infos); err != nil {
		log.LogDebugf("listFilesV1: supply list file info fail, err(%v)", err)
		return
	}

	prefixes = prefixMap.Prefixes()

	log.LogDebugf("listFilesV1: volume list dir: volume(%v) prefix(%v) marker(%v) delimiter(%v) maxKeys(%v) infos(%v) prefixes(%v)",
		v.name, prefix, marker, delimiter, maxKeys, len(infos), len(prefixes))

	return
}

func (v *volume) listFilesV2(prefix, startAfter, contToken, delimiter string, maxKeys uint64) (infos []*FSFileInfo, prefixes Prefixes, err error) {
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

	// recursion call listDir method
	infos, prefixMap, err = v.listDir(infos, prefixMap, parentId, maxKeys, dirs, prefix, marker, delimiter)
	if err != nil {
		log.LogErrorf("listFilesV2: volume list dir fail, volume(%v) err(%v)", v.name, err)
		return
	}

	// supply size and MD5
	err = v.supplyListFileInfo(infos)
	if err != nil {
		log.LogDebugf("listFilesV2: supply list file info fail, err(%v)", err)
		return
	}

	prefixes = prefixMap.Prefixes()

	log.LogDebugf("listFilesV2: volume list dir: volume(%v) prefix(%v) marker(%v) delimiter(%v) maxKeys(%v) infos(%v) prefixes(%v)",
		v.name, prefix, marker, delimiter, maxKeys, len(infos), len(prefixes))

	return
}

func (v *volume) findParentId(prefix string) (inode uint64, prefixDirs []string, err error) {
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

func (v *volume) listDir(fileInfos []*FSFileInfo, prefixMap PrefixMap, parentId, maxKeys uint64, dirs []string, prefix, marker, delimiter string) ([]*FSFileInfo, PrefixMap, error) {

	children, err := v.mw.ReadDir_ll(parentId)
	if err != nil {
		return fileInfos, prefixMap, err
	}

	for _, child := range children {

		var path = strings.Join(append(dirs, child.Name), "/")

		if os.FileMode(child.Type).IsDir() {
			path += "/"
		}
		log.LogDebugf("listDir: process child, path(%v) prefix(%v) marker(%v) delimiter(%v)",
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
			fileInfos, prefixMap, err = v.listDir(fileInfos, prefixMap, child.Inode, maxKeys, append(dirs, child.Name), prefix, marker, delimiter)
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

func (v *volume) supplyListFileInfo(fileInfos []*FSFileInfo) (err error) {
	// get all file info indoes
	inoInfoMap := make(map[uint64]*proto.InodeInfo)
	var inodes []uint64
	for _, fileInfo := range fileInfos {
		inodes = append(inodes, fileInfo.Inode)
	}

	// Get size information in batches, then update to fileInfos
	batchInodeInfos := v.mw.BatchInodeGet(inodes)
	for _, inodeInfo := range batchInodeInfos {
		inoInfoMap[inodeInfo.Inode] = inodeInfo
	}
	for _, fileInfo := range fileInfos {
		inoInfo := inoInfoMap[fileInfo.Inode]
		if inoInfo != nil {
			fileInfo.Size = int64(inoInfo.Size)
			fileInfo.ModifyTime = inoInfo.ModifyTime
			fileInfo.Mode = os.FileMode(inoInfo.Mode)
		}
	}

	// Get MD5 information in batches, then update to fileInfos
	md5Map := make(map[uint64]string)
	keys := []string{XAttrKeyOSSETag}
	batchXAttrInfos, err := v.mw.BatchGetXAttr(inodes, keys)
	if err != nil {
		logger.Error("supplyListFileInfo: batch get xattr fail, inodes(%v), err(%v)", inodes, err)
		return
	}
	for _, xAttrInfo := range batchXAttrInfos {
		md5Map[xAttrInfo.Inode] = xAttrInfo.XAttrs[XAttrKeyOSSETag]
	}
	for _, fileInfo := range fileInfos {
		fileInfo.ETag = md5Map[fileInfo.Inode]
	}
	return
}

func (v *volume) ListMultipartUploads(prefix, delimiter, keyMarker string, multipartIdMarker string, maxUploads uint64) ([]*FSUpload, string, string, bool, []string, error) {
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
	//var sessionResult []*proto.MultipartInfo

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

func (v *volume) ListParts(path, sessionId string, maxParts, partNumberMarker uint64) (parts []*FSPart, nextMarker uint64, isTruncated bool, err error) {
	var parentId uint64
	dirs, _ := splitPath(path)
	// process path
	if parentId, err = v.lookupDirectories(dirs, true); err != nil {
		return nil, 0, false, err
	}

	multipartInfo, err := v.mw.GetMultipart_ll(sessionId, parentId)
	if err != nil {
		log.LogErrorf("Get session(%s) parts failed cause: %v", sessionId, err)
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

func (v *volume) CopyFile(targetPath, sourcePath string) (info *FSFileInfo, err error) {

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

	inodeInfo, err := v.copyFile(newParentId, newFilename, sourceFileInode, 0600)
	if err != nil {
		log.LogErrorf("CopyFile: meta copy file fail: newParentID(%v) newName(%v) sourceIno(%v) err(%v)",
			newParentId, newFilename, sourceFileInode, err)
		return nil, err
	}

	xAttrInfo, err := v.mw.XAttrGet_ll(sourceFileInode, XAttrKeyOSSETag)
	if err != nil {
		log.LogErrorf("CopyFile: meta set xattr fail: inode(%v) err(%v)", sourceFileInode, err)
		return nil, err
	}
	md5Val := xAttrInfo.XAttrs[XAttrKeyOSSETag]

	info = &FSFileInfo{
		Path:       targetPath,
		Size:       int64(inodeInfo.Size),
		Mode:       os.FileMode(inodeInfo.Size),
		ModifyTime: inodeInfo.ModifyTime,
		ETag:       md5Val,
		Inode:      inodeInfo.Inode,
	}
	return
}

func (v *volume) copyFile(parentID uint64, newFileName string, sourceFileInode uint64, mode uint32) (info *proto.InodeInfo, err error) {

	if err = v.mw.DentryCreate_ll(parentID, newFileName, sourceFileInode, mode); err != nil {
		return
	}
	if info, err = v.mw.InodeLink_ll(sourceFileInode); err != nil {
		return
	}
	return
}

func newVolume(masters []string, vol string) (*volume, error) {
	var err error
	opt := &proto.MountOptions{
		Volname:      vol,
		Owner:        "",
		Master:       strings.Join(masters, ","),
		FollowerRead: true,
		Authenticate: false,
	}

	var mw *meta.MetaWrapper
	if mw, err = meta.NewMetaWrapper(opt, false); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			mw.Close()
		}
	}()
	var ec *stream.ExtentClient
	if ec, err = stream.NewExtentClient(opt, mw.AppendExtentKey, mw.GetExtents, mw.Truncate); err != nil {
		return nil, err
	}

	v := &volume{mw: mw, ec: ec, name: vol, om: new(OSSMeta)}
	go v.syncOSSMeta()
	return v, nil
}
