package master

import (
	"github.com/tiglabs/baudstorage/proto"
)

/*this struct define chunk file metadata on  dataNode */
type FileMetaOnNode struct {
	Crc       uint32
	LocAddr   string
	LocIndex  uint8
	LastObjID uint64
	NeedleCnt int
	Size      uint32
}

type FileInCore struct {
	Name       string
	MarkDel    bool
	LastModify int64
	Metas      []*FileMetaOnNode
}

func NewFileMetaOnNode(volCrc uint32, volLoc string, volLocIndex int, lastObjID uint64, needleCnt int, size uint32) (fm *FileMetaOnNode) {
	fm = new(FileMetaOnNode)
	fm.Crc = volCrc
	fm.LocAddr = volLoc
	fm.LocIndex = uint8(volLocIndex)
	fm.LastObjID = lastObjID
	fm.NeedleCnt = needleCnt
	fm.Size = size
	return
}

func (fm *FileMetaOnNode) getLocationAddr() (loc string) {
	return fm.LocAddr
}

func (fm *FileMetaOnNode) getFileCrc() (crc uint32) {
	return fm.Crc
}

func NewFileInCore(name string) (fc *FileInCore) {
	fc = new(FileInCore)
	fc.Name = name
	fc.MarkDel = false
	fc.Metas = make([]*FileMetaOnNode, 0)

	return
}

/*use a File and volLocation update FileInCore,
range all FileInCore.NodeInfos,update crc and reportTime*/
func (fc *FileInCore) updateFileInCore(volID uint64, vf *proto.File, volLoc *DataReplica, volLocIndex int) {
	if vf.MarkDel == true {
		fc.MarkDel = true
	}

	if vf.Modified > fc.LastModify {
		fc.LastModify = vf.Modified
	}

	isFind := false
	for i := 0; i < len(fc.Metas); i++ {
		if fc.Metas[i].getLocationAddr() == volLoc.Addr {
			fc.Metas[i].Crc = vf.Crc
			fc.Metas[i].LastObjID = vf.LastObjID
			fc.Metas[i].NeedleCnt = vf.NeedleCnt
			fc.Metas[i].Size = vf.Size
			isFind = true
			break
		}
	}

	if isFind == false {
		fm := NewFileMetaOnNode(vf.Crc, volLoc.Addr, volLocIndex, vf.LastObjID, vf.NeedleCnt, vf.Size)
		fc.Metas = append(fc.Metas, fm)
	}

}

func (fc *FileInCore) getFileMetaByAddr(replica *DataReplica) (fm *FileMetaOnNode, ok bool) {
	for i := 0; i < len(fc.Metas); i++ {
		fm = fc.Metas[i]
		if fm.LocAddr == replica.Addr {
			ok = true
			return
		}
	}

	return
}
