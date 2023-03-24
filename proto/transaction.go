package proto

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strconv"
)

//const (
//	TxInodeType  int = 0
//	TxDentryType int = 1
//)
//
//type TxItemInfo interface {
//	GetKey() string
//	GetTxId() (string, error)
//	SetTxId(txID string)
//	SetTimeout(timeout uint)
//	SetCreateTime(createTime int64)
//}

const (
	DefaultTransactionTimeout = 5 //seconds
)

const (
	TxOpMaskOff     uint8 = 0x00
	TxOpMaskCreate  uint8 = 0x01
	TxOpMaskMkdir   uint8 = 0x02
	TxOpMaskRemove  uint8 = 0x04
	TxOpMaskRename  uint8 = 0x08
	TxOpMaskMknod   uint8 = 0x10
	TxOpMaskSymlink uint8 = 0x20
	TxOpMaskLink    uint8 = 0x40
	TxOpMaskAll     uint8 = 0x7F
)

var GTxMaskMap = map[string]uint8{
	"off":     TxOpMaskOff,
	"create":  TxOpMaskCreate,
	"mkdir":   TxOpMaskMkdir,
	"remove":  TxOpMaskRemove,
	"rename":  TxOpMaskRename,
	"mknod":   TxOpMaskMknod,
	"symlink": TxOpMaskSymlink,
	"link":    TxOpMaskLink,
	"all":     TxOpMaskAll,
}

func GetMaskString(mask uint8) (maskStr string) {
	for k, v := range GTxMaskMap {
		if k == "all" {
			continue
		}
		if mask&v > 0 {
			if maskStr == "" {
				maskStr = k
			} else {
				maskStr = maskStr + "|" + k
			}
		}
	}
	if maskStr == "" {
		maskStr = "off"
	}
	return
}

type TxInodeInfo struct {
	Ino        uint64
	MpID       uint64
	CreateTime int64 //time.Now().Unix()
	Timeout    uint32
	TxID       string
	MpMembers  string
}

func NewTxInodeInfo(members string, ino uint64, mpID uint64) *TxInodeInfo {
	return &TxInodeInfo{
		Ino:       ino,
		MpID:      mpID,
		MpMembers: members, //todo_tx: add all members in case of any failed member situation
	}
}

func (info *TxInodeInfo) String() string {
	data, err := json.Marshal(info)
	if err != nil {
		return ""
	}
	return string(data)
}

func (info *TxInodeInfo) Marshal() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	if err = binary.Write(buff, binary.BigEndian, &info.Ino); err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, &info.MpID); err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, &info.CreateTime); err != nil {
		return nil, err
	}
	if err = binary.Write(buff, binary.BigEndian, &info.Timeout); err != nil {
		return nil, err
	}

	id := []byte(info.TxID)
	idSize := uint32(len(id))
	if err = binary.Write(buff, binary.BigEndian, &idSize); err != nil {
		return nil, err
	}
	if _, err = buff.Write(id); err != nil {
		return nil, err
	}

	addr := []byte(info.MpMembers)
	addrSize := uint32(len(addr))
	if err = binary.Write(buff, binary.BigEndian, &addrSize); err != nil {
		return nil, err
	}
	if _, err = buff.Write(addr); err != nil {
		return nil, err
	}

	result = buff.Bytes()
	return
}

func (info *TxInodeInfo) Unmarshal(raw []byte) (err error) {
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &info.Ino); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.MpID); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.CreateTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &info.Timeout); err != nil {
		return
	}

	idSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &idSize); err != nil {
		return
	}
	if idSize > 0 {
		id := make([]byte, idSize)
		if _, err = io.ReadFull(buff, id); err != nil {
			return
		}
		info.TxID = string(id)
	}

	addrSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &addrSize); err != nil {
		return
	}
	if addrSize > 0 {
		addr := make([]byte, addrSize)
		if _, err = io.ReadFull(buff, addr); err != nil {
			return
		}
		info.MpMembers = string(addr)
	}

	return
}

//func (info *TxInodeInfo) SetMpID(mpID uint64) {
//	info.MpID = mpID
//}

func (info *TxInodeInfo) GetIno() uint64 {
	return info.Ino
}

func (info *TxInodeInfo) GetKey() uint64 {
	//if info.Ino == 0 {
	//	return "", errors.New("ino is not set")
	//}
	return info.Ino
}

func (info *TxInodeInfo) GetTxId() (string, error) {
	if info.TxID == "" {
		return "", errors.New("txID is not set")
	}
	return info.TxID, nil
}

func (info *TxInodeInfo) SetTxId(txID string) {
	info.TxID = txID
}

func (info *TxInodeInfo) SetTimeout(timeout uint32) {
	info.Timeout = timeout
}

func (info *TxInodeInfo) SetCreateTime(createTime int64) {
	info.CreateTime = createTime
}

type TxDentryInfo struct {
	ParentId   uint64 // FileID value of the parent inode.
	Name       string // Name of the current dentry.
	MpMembers  string
	TxID       string
	MpID       uint64
	CreateTime int64 //time.Now().Unix()
	Timeout    uint32
}

func NewTxDentryInfo(members string, parentId uint64, name string, mpID uint64) *TxDentryInfo {
	return &TxDentryInfo{
		ParentId:  parentId,
		Name:      name,
		MpMembers: members,
		MpID:      mpID, //todo_tx: add all members in case of any failed member situation
	}
}

func (info *TxDentryInfo) String() string {
	data, err := json.Marshal(info)
	if err != nil {
		return ""
	}
	return string(data)
}

func (info *TxDentryInfo) Marshal() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	if err = binary.Write(buff, binary.BigEndian, &info.ParentId); err != nil {
		panic(err)
	}

	name := []byte(info.Name)
	nameSize := uint32(len(name))
	if err = binary.Write(buff, binary.BigEndian, &nameSize); err != nil {
		panic(err)
	}
	if _, err = buff.Write(name); err != nil {
		panic(err)
	}

	addr := []byte(info.MpMembers)
	addrSize := uint32(len(addr))
	if err = binary.Write(buff, binary.BigEndian, &addrSize); err != nil {
		panic(err)
	}
	if _, err = buff.Write(addr); err != nil {
		panic(err)
	}

	id := []byte(info.TxID)
	idSize := uint32(len(id))
	if err = binary.Write(buff, binary.BigEndian, &idSize); err != nil {
		panic(err)
	}
	if _, err = buff.Write(id); err != nil {
		panic(err)
	}

	if err = binary.Write(buff, binary.BigEndian, &info.MpID); err != nil {
		panic(err)
	}

	if err = binary.Write(buff, binary.BigEndian, &info.CreateTime); err != nil {
		panic(err)
	}

	if err = binary.Write(buff, binary.BigEndian, &info.Timeout); err != nil {
		panic(err)
	}
	result = buff.Bytes()
	return
}

func (info *TxDentryInfo) Unmarshal(raw []byte) (err error) {
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &info.ParentId); err != nil {
		return
	}

	nameSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &nameSize); err != nil {
		return
	}
	if nameSize > 0 {
		name := make([]byte, nameSize)
		if _, err = io.ReadFull(buff, name); err != nil {
			return
		}
		info.Name = string(name)
	}

	addrSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &addrSize); err != nil {
		return
	}
	if addrSize > 0 {
		addr := make([]byte, addrSize)
		if _, err = io.ReadFull(buff, addr); err != nil {
			return
		}
		info.MpMembers = string(addr)
	}

	idSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &idSize); err != nil {
		return
	}
	if idSize > 0 {
		id := make([]byte, idSize)
		if _, err = io.ReadFull(buff, id); err != nil {
			return
		}
		info.TxID = string(id)
	}

	if err = binary.Read(buff, binary.BigEndian, &info.MpID); err != nil {
		return
	}

	if err = binary.Read(buff, binary.BigEndian, &info.CreateTime); err != nil {
		return
	}

	if err = binary.Read(buff, binary.BigEndian, &info.Timeout); err != nil {
		return
	}
	return
}

func (info *TxDentryInfo) GetKey() string {
	//if info.ParentId == 0 || info.Name == "" {
	//	return "", errors.New("parentId or name is required")
	//}
	return strconv.FormatUint(info.ParentId, 10) + "_" + info.Name
}

func (info *TxDentryInfo) GetTxId() (string, error) {
	if info.TxID == "" {
		return "", errors.New("txID is not set")
	}
	return info.TxID, nil
}

func (info *TxDentryInfo) SetTxId(txID string) {
	info.TxID = txID
}

func (info *TxDentryInfo) SetTimeout(timeout uint32) {
	info.Timeout = timeout
}

func (info *TxDentryInfo) SetCreateTime(createTime int64) {
	info.CreateTime = createTime
}

//type StartTransactionRequest struct {
//	timeout uint
//	itemMap map[string]TxItemInfo
//}

const (
	TxTypeUndefined uint32 = iota
	TxTypeCreate
	TxTypeMkdir
	TxTypeRemove
	TxTypeRename
	TxTypeMknod
	TxTypeSymlink
	TxTypeLink
)

type TransactionInfo struct {
	TxID       string // "metapartitionId_atomicId", if empty, mp should be TM, otherwise it will be RM
	TxType     uint32
	TmID       int64
	CreateTime int64 //time.Now().Unix()
	Timeout    uint32
	//ItemMap    map[string]TxItemInfo
	TxInodeInfos  map[uint64]*TxInodeInfo
	TxDentryInfos map[string]*TxDentryInfo
}

func NewTransactionInfo(timeout uint32, txType uint32) *TransactionInfo {
	return &TransactionInfo{
		Timeout:       timeout,
		TxInodeInfos:  make(map[uint64]*TxInodeInfo, 0),
		TxDentryInfos: make(map[string]*TxDentryInfo, 0),
		TmID:          -1,
		TxType:        txType,
	}
}

func (txInfo *TransactionInfo) IsInitialized() bool {
	if txInfo.TxID != "" && txInfo.TmID != -1 {
		return true
	}
	return false
}

func (txInfo *TransactionInfo) String() string {
	data, err := json.Marshal(txInfo)
	if err != nil {
		return ""
	}
	return string(data)
}

func (txInfo *TransactionInfo) Copy() *TransactionInfo {
	newInfo := NewTransactionInfo(txInfo.Timeout, txInfo.TxType)
	newInfo.TxID = txInfo.TxID
	//newInfo.TxType = txInfo.TxType
	newInfo.TmID = txInfo.TmID
	newInfo.CreateTime = txInfo.CreateTime
	for k, v := range txInfo.TxInodeInfos {
		newInfo.TxInodeInfos[k] = v
	}
	for k, v := range txInfo.TxDentryInfos {
		newInfo.TxDentryInfos[k] = v
	}
	return newInfo
}

func (txInfo *TransactionInfo) Marshal() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, 256))
	id := []byte(txInfo.TxID)
	idSize := uint32(len(id))
	if err = binary.Write(buff, binary.BigEndian, &idSize); err != nil {
		return nil, err
	}
	if _, err = buff.Write(id); err != nil {
		return nil, err
	}

	if err = binary.Write(buff, binary.BigEndian, &txInfo.TxType); err != nil {
		return nil, err
	}

	if err = binary.Write(buff, binary.BigEndian, &txInfo.TmID); err != nil {
		return nil, err
	}

	if err = binary.Write(buff, binary.BigEndian, &txInfo.CreateTime); err != nil {
		return nil, err
	}

	if err = binary.Write(buff, binary.BigEndian, &txInfo.Timeout); err != nil {
		return nil, err
	}

	inodeNum := uint32(len(txInfo.TxInodeInfos))
	if err = binary.Write(buff, binary.BigEndian, &inodeNum); err != nil {
		return nil, err
	}

	for _, txInodeInfo := range txInfo.TxInodeInfos {
		bs, err := txInodeInfo.Marshal()
		if err != nil {
			return nil, err
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return nil, err
		}
		if _, err := buff.Write(bs); err != nil {
			return nil, err
		}
	}

	dentryNum := uint32(len(txInfo.TxDentryInfos))
	if err = binary.Write(buff, binary.BigEndian, &dentryNum); err != nil {
		panic(err)
	}
	for _, txDentryInfo := range txInfo.TxDentryInfos {
		bs, err := txDentryInfo.Marshal()
		if err != nil {
			return nil, err
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return nil, err
		}
		if _, err := buff.Write(bs); err != nil {
			return nil, err
		}
	}

	return buff.Bytes(), nil
}

func (txInfo *TransactionInfo) Unmarshal(raw []byte) (err error) {
	buff := bytes.NewBuffer(raw)
	idSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &idSize); err != nil {
		return
	}
	if idSize > 0 {
		id := make([]byte, idSize)
		if _, err = io.ReadFull(buff, id); err != nil {
			return
		}
		txInfo.TxID = string(id)
	}

	if err = binary.Read(buff, binary.BigEndian, &txInfo.TxType); err != nil {
		return
	}

	if err = binary.Read(buff, binary.BigEndian, &txInfo.TmID); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &txInfo.CreateTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &txInfo.Timeout); err != nil {
		return
	}

	var inodeNum uint32
	if err = binary.Read(buff, binary.BigEndian, &inodeNum); err != nil {
		return
	}
	var dataLen uint32
	for i := uint32(0); i < inodeNum; i++ {
		if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return
		}
		data := make([]byte, int(dataLen))
		if _, err = buff.Read(data); err != nil {
			return
		}
		txInodeInfo := NewTxInodeInfo("", 0, 0)
		if err = txInodeInfo.Unmarshal(data); err != nil {
			return
		}
		txInfo.TxInodeInfos[txInodeInfo.GetKey()] = txInodeInfo
	}

	var dentryNum uint32
	if err = binary.Read(buff, binary.BigEndian, &dentryNum); err != nil {
		return
	}

	for i := uint32(0); i < dentryNum; i++ {
		if err = binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return
		}
		data := make([]byte, int(dataLen))
		if _, err = buff.Read(data); err != nil {
			return
		}
		txDentryInfo := NewTxDentryInfo("", 0, "", 0)
		if err = txDentryInfo.Unmarshal(data); err != nil {
			return
		}
		txInfo.TxDentryInfos[txDentryInfo.GetKey()] = txDentryInfo
	}

	return
}
