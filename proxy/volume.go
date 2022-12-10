// Copyright 2022 The CubeFS Authors.
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

package proxy

import (
	"io"
	"io/fs"
	"os"
	"path"
	"syscall"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/lib"
	"github.com/cubefs/cubefs/util/log"
)

func (p *ProxyNode) getVol(volId uint32) (vol *Volume, err error) {
	name := buildVolName(volId)
	p.volRW.RLock()
	vol, ok := p.volMap[name]
	p.volRW.RUnlock()
	if ok {
		return vol, nil
	}

	// try to fetch vol info from master
	volInfo, err := p.mc.AdminAPI().GetVolumeSimpleInfo(name)
	if err != nil {
		return
	}

	err = p.newVol(volInfo.Owner, volInfo.Name)
	if err != nil {
		return
	}

	p.volRW.RLock()
	vol = p.volMap[name]
	p.volRW.RUnlock()

	return
}

func (p *ProxyNode) newVol(owner, volName string) (err error) {

	vol := &Volume{}

	var metaConfig = &meta.MetaConfig{
		Volume:  volName,
		Owner:   owner,
		Masters: p.mc.Nodes(),
	}

	mw, err := meta.NewMetaWrapper(metaConfig)
	if err != nil {
		return errors.Trace(err, "NewMetaWrapper failed!"+err.Error())
	}

	var extentConfig = &stream.ExtentConfig{
		Volume:            volName,
		Masters:           p.mc.Nodes(),
		FollowerRead:      true,
		NearRead:          true,
		OnAppendExtentKey: mw.AppendExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
	}

	ec, err := stream.NewExtentClient(extentConfig)
	if err != nil {
		return errors.Trace(err, "NewExtentClient failed!")
	}
	vol.ec = ec
	vol.name = volName
	vol.mw = mw

	p.volRW.Lock()
	p.volMap[volName] = vol
	p.volRW.Unlock()

	return
}

type Volume struct {
	mw   MetaOp
	ec   DataOp
	name string
}

func (v *Volume) getInodeByPath(dir string) (ino uint64, err error) {
	ino, err = v.mw.LookupPath(dir)
	if err != nil {
		return ino, lib.SyscallToErr(err)
	}
	return
}

func toFileMode(mode uint32) fs.FileMode {
	m := fs.FileMode(mode)
	return m.Perm()
}

func (v *Volume) mkdir(dir string, mode uint32) error {
	parentDir, name := path.Split(dir)
	ino, err := v.getInodeByPath(parentDir)
	if err != nil {
		return err
	}

	fmode := proto.Mode(os.ModeDir | toFileMode(mode))
	_, err = v.mw.Create_ll(ino, name, fmode, 0, 0, nil)
	if err != nil {
		return lib.SyscallToErr(err)
	}

	return nil
}

const maxLimitOnce = 1000

type InoInfo struct {
	Inode  uint64 `json:"ino"`
	Mode   uint32 `json:"mode"`
	Nlink  uint32 `json:"nlink"`
	Size   uint64 `json:"sz"`
	Uid    uint32 `json:"uid"`
	Gid    uint32 `json:"gid"`
	MTime  uint64 `json:"mt"`
	CTime  uint64 `json:"ct"`
	ATime  uint64 `json:"at"`
	Target string `json:"target"`
}

type DirEntInfo struct {
	Info *InoInfo `json:"info"`
	proto.Dentry
}

func (v *Volume) readDirLimitWithIno(ino uint64, from string, limit int) (ins []DirEntInfo, err error) {
	dents, err := v.mw.ReadDirLimit_ll(ino, from, uint64(limit))
	if err != nil {
		return nil, lib.SyscallToErr(err)
	}

	inodes := make([]uint64, 0, len(dents))
	dirInfos := make([]DirEntInfo, 0, len(dents))

	for _, child := range dents {
		inodes = append(inodes, child.Inode)
	}

	inoMap := make(map[uint64]*proto.InodeInfo)

	infos, err := v.mw.BatchInodeGetWith(inodes)
	if err != nil {
		return nil, lib.SyscallToErr(err)
	}

	for _, info := range infos {
		inoMap[info.Inode] = info
	}

	for _, ent := range dents {
		entInfo := DirEntInfo{
			Dentry: ent,
			Info:   buildInoInfo(inoMap[ent.Inode]),
		}

		dirInfos = append(dirInfos, entInfo)
	}

	return dirInfos, err
}

func buildInoInfo(ifo *proto.InodeInfo) *InoInfo {
	return &InoInfo{
		Inode:  ifo.Inode,
		Mode:   ifo.Mode,
		Nlink:  ifo.Nlink,
		Size:   ifo.Size,
		Uid:    ifo.Uid,
		Gid:    ifo.Gid,
		Target: string(ifo.Target),
		MTime:  uint64(ifo.ModifyTime.Unix()),
		CTime:  uint64(ifo.CreateTime.Unix()),
		ATime:  uint64(ifo.AccessTime.Unix()),
	}
}

func (v *Volume) readDirLimit(dir, from string, limit int) ([]DirEntInfo, error) {
	if limit > maxLimitOnce {
		limit = maxLimitOnce
	}

	ino, err := v.getInodeByPath(dir)
	if err != nil {
		return nil, err
	}

	return v.readDirLimitWithIno(ino, from, limit)
}

func (v *Volume) rmdir(dir string) (err error) {

	parentDir, name := path.Split(dir)
	ino, err := v.getInodeByPath(parentDir)
	if err != nil {
		return err
	}

	_, err = v.mw.Delete_ll(ino, name, true)
	if err != nil {
		return lib.SyscallToErr(err)
	}

	return err
}

const (
	CreateFlag = 1
	OpenFlag   = 2
)

func (v *Volume) Open(dir string, mode uint32, flag uint8) (ino uint64, err error) {
	if flag == OpenFlag {
		return v.lookup(dir)
	}

	parent, name := path.Split(dir)
	ino, err = v.getInodeByPath(parent)
	if err != nil {
		return 0, err
	}

	fmode := proto.Mode(toFileMode(mode))
	info, err := v.mw.Create_ll(ino, name, fmode, 0, 0, nil)
	if err != nil {
		log.LogErrorf("Create: parent(%v) req(%s) err(%v)", ino, dir, err)
		return 0, lib.SyscallToErr(err)
	}

	return info.Inode, nil
}

func (v *Volume) lookup(dir string) (ino uint64, err error) {
	ino, err = v.getInodeByPath(dir)
	if err != nil {
		return 0, err
	}

	return ino, nil
}

func (v *Volume) writeFile(ino uint64, offset int, data []byte) (size int, err error) {
	err = v.ec.OpenStream(ino)
	if err != nil {
		return 0, lib.SyscallToErr(err)
	}

	size, err = v.ec.Write(ino, offset, data, proto.FlagsSyncWrite)
	if err != nil {
		return 0, lib.SyscallToErr(err)
	}

	err = v.ec.Flush(ino)
	if err != nil {
		log.LogErrorf("flush data failed after write. ino [%d], err %s", ino, err.Error())
		return 0, lib.SyscallToErr(err)
	}

	return
}

func (v *Volume) readFile(ino uint64, offset, len int) (data []byte, err error) {
	err = v.ec.OpenStream(ino)
	if err != nil {
		return nil, lib.SyscallToErr(err)
	}

	data = make([]byte, len)
	read, err := v.ec.Read(ino, data, offset, len)
	if err != nil && err != io.EOF {
		return nil, lib.SyscallToErr(err)
	}

	data = data[:read]
	return data, nil
}

func (v *Volume) fsync(ino uint64) (err error) {
	err = v.ec.Flush(ino)
	if err != nil {
		return lib.SyscallToErr(err)
	}

	return
}

func (v *Volume) truncate(filepath string, offset int) (err error) {
	ino, err := v.getInodeByPath(filepath)
	if err != nil {
		return err
	}

	err = v.ec.Truncate(ino, offset)
	if err != nil {
		return lib.SyscallToErr(err)
	}

	return
}

func (v *Volume) unlink(filePath string) (err error) {
	dir, name := path.Split(filePath)
	parIno, err := v.getInodeByPath(dir)
	if err != nil {
		return err
	}

	_, err = v.mw.Delete_ll(parIno, name, false)
	if err != nil {
		return lib.SyscallToErr(err)
	}

	return
}

func (v *Volume) link(oldPath, newPath string) (err error) {
	dir, name := path.Split(oldPath)
	parIno, err := v.getInodeByPath(dir)
	if err != nil {
		return err
	}

	ino, mode, err := v.mw.Lookup_ll(parIno, name)
	if err != nil {
		return lib.SyscallToErr(err)
	}

	if !proto.IsRegular(mode) {
		log.LogErrorf("Link: not regular, path(%v) mode(%v)", oldPath, os.FileMode(mode))
		return lib.SyscallToErr(syscall.EPERM)
	}

	newDir, newName := path.Split(newPath)
	newParIno, err := v.getInodeByPath(newDir)
	if err != nil {
		return err
	}

	_, err = v.mw.Link(newParIno, newName, ino)
	if err != nil {
		log.LogErrorf("Link: oldpath(%s) newpath(%s) err(%v)", oldPath, newPath, err)
		return lib.SyscallToErr(err)
	}

	return
}

// common

func (v *Volume) symlink(oldPath, newPath string) (err error) {
	dir, name := path.Split(newPath)
	parIno, err := v.getInodeByPath(dir)
	if err != nil {
		return err
	}

	_, err = v.mw.Create_ll(parIno, name, proto.Mode(os.ModeSymlink|os.ModePerm), 0, 0, []byte(oldPath))
	if err != nil {
		log.LogErrorf("Symlink: oldPath(%v) NewName(%v) err(%v)", oldPath, newPath, err)
		return lib.SyscallToErr(err)
	}

	return
}

func (v *Volume) setXattr(srcPath, key string, data []byte) (err error) {
	ino, err := v.getInodeByPath(srcPath)
	if err != nil {
		return err
	}

	err = v.mw.XAttrSet_ll(ino, []byte(key), data)
	if err != nil {
		log.LogErrorf("xattrSet: path(%v) key (%s) err(%v)", srcPath, key, err)
		return lib.SyscallToErr(err)
	}

	return
}

func (v *Volume) getXattr(srcPath, key string) (data []byte, err error) {
	ino, err := v.getInodeByPath(srcPath)
	if err != nil {
		return nil, err
	}

	info, err := v.mw.XAttrGet_ll(ino, key)
	if err != nil {
		return nil, lib.SyscallToErr(err)
	}

	data = info.Get(key)
	return
}

func (v *Volume) removeXattr(srcPath, key string) (err error) {
	ino, err := v.getInodeByPath(srcPath)
	if err != nil {
		return err
	}

	err = v.mw.XAttrDel_ll(ino, key)
	if err != nil {
		return lib.SyscallToErr(err)
	}

	return
}

func (v *Volume) listXattr(srcPath string) (keys []string, err error) {
	ino, err := v.getInodeByPath(srcPath)
	if err != nil {
		return nil, err
	}

	keys, err = v.mw.XAttrsList_ll(ino)
	if err != nil {
		return nil, lib.SyscallToErr(err)
	}

	return keys, nil
}

func (v *Volume) setAttr(srcPath string, flag uint8, mode, uid, gid uint32, atime, mtime int64) (err error) {
	ino, err := v.getInodeByPath(srcPath)
	if err != nil {
		return err
	}

	if flag&uint8(proto.AttrMode) != 0 {
		ifo, err := v.mw.InodeGet_ll(ino)
		if err != nil {
			return lib.SyscallToErr(err)
		}

		mode = mode & 0777
		mode = ifo.Mode&uint32(os.ModeType) | mode
	}

	err = v.mw.Setattr(ino, uint32(flag), mode, uid, gid, atime, mtime)
	if err != nil {
		return lib.SyscallToErr(err)
	}

	return
}

func (v *Volume) rename(srcPath, destPath string) (err error) {
	srcDir, srcName := path.Split(srcPath)
	parIno, err := v.getInodeByPath(srcDir)
	if err != nil {
		return err
	}

	destDir, destName := path.Split(destPath)
	destIno, err := v.getInodeByPath(destDir)
	if err != nil {
		return err
	}

	err = v.mw.Rename_ll(parIno, srcName, destIno, destName, true)
	if err != nil {
		return lib.SyscallToErr(err)
	}

	return
}

func (v *Volume) stat(srcPath string) (info *InoInfo, err error) {
	ino, err := v.getInodeByPath(srcPath)
	if err != nil {
		return
	}

	ifo, err := v.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, lib.SyscallToErr(err)
	}

	info = buildInoInfo(ifo)
	return
}

type statInfo struct {
	Files    int `json:"files"`
	Folders  int `json:"folders"`
	Fbytes   int `json:"fbytes"`
	Rfiles   int `json:"rfiles"`
	Rfolders int `json:"rfolders"`
	Rbytes   int `json:"rbytes"`
}

func (s *statInfo) addChildStat(info *statInfo) {
	s.Rfiles = s.Rfiles + info.Rfiles + info.Files
	s.Rfolders = s.Rfolders + info.Rfolders + info.Folders
	s.Rbytes = s.Rbytes + info.Fbytes + info.Rbytes
}

func (v *Volume) statDir(dir string) (info *statInfo, err error) {
	ino, err := v.getInodeByPath(dir)
	if err != nil {
		return nil, err
	}

	return v.getStatByIno(ino)
}

//  rfiles not contain current files
func (v *Volume) getStatByIno(ino uint64) (info *statInfo, err error) {
	info = &statInfo{}
	ents, err := v.mw.ReadDir_ll(ino)
	if len(ents) == 0 {
		return
	}

	var files, dirs int
	inos := make([]uint64, 0, len(ents))
	for _, e := range ents {
		if proto.IsDir(e.Type) {
			subStat, err := v.getStatByIno(e.Inode)
			if err != nil {
				return nil, lib.SyscallToErr(err)
			}
			info.addChildStat(subStat)
			dirs++
			continue
		}
		files++
		if proto.IsRegular(e.Type) {
			inos = append(inos, e.Inode)
		}
	}

	info.Files = files
	info.Folders = dirs

	infos, err := v.mw.BatchInodeGetWith(inos)
	if err != nil {
		return nil, lib.SyscallToErr(err)
	}

	for _, e := range infos {
		info.Fbytes += int(e.Size)
	}

	return info, nil
}
