package metanode

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"sort"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) fsmCalcMetaPartitionMd5Sum(msg *storeMsg) error {
	if msg.snap == nil {
		log.LogErrorf("fsmCalcMetaPartitionMd5Sum mp[%v] snap is nil", mp.config.PartitionId)
		return nil
	}
	applyID := msg.applyIndex
	log.LogWarnf("fsmCalcMetaPartitionMd5Sum mp[%v] applyID: %v", mp.config.PartitionId, applyID)
	defer func() {
		msg.snap.Close()
		log.LogWarnf("fsmCalcMetaPartitionMd5Sum mp[%v] applyID: %v end", mp.config.PartitionId, applyID)
	}()
	h := md5.New()
	if applyID == 0 && mp.Md5ApplyId == 0 {
		mp.Md5Sum = hex.EncodeToString(h.Sum(nil))
		return nil
	}
	if mp.Md5ApplyId >= applyID {
		return nil
	}

	buff := bytes.NewBuffer(make([]byte, 0, 4096))
	calculateFuncs := []func(msg *storeMsg, h hash.Hash, buff *bytes.Buffer) (err error){
		CalculateInodeMd5Sum,
		CalculateDentryMd5Sum,
		CalculateExtentMd5Sum,
		CalculateMultipartMd5Sum,
		CalculateTxInfoMd5Sum,
		CalculateTxRbInodeMd5Sum,
		CalculateTxRbDentryMd5Sum,
		CalculateUniqCheckerMd5Sum,
		CalculateMultiVersionMd5Sum,
	}
	for _, calculateFunc := range calculateFuncs {
		err := calculateFunc(msg, h, buff)
		if err != nil {
			log.LogErrorf("mp[%v] failed to calculate md5 sum, err(%v)", mp.config.PartitionId, err)
			return err
		}
	}

	if mp.Md5ApplyId < applyID {
		mp.Md5Sum = hex.EncodeToString(h.Sum(nil))
		mp.Md5ApplyId = applyID
	}

	return nil
}

// writeInodeToBufferForMd5 serializes inode for MD5 calculation
// It reuses MarshalValueV2WithSkip to ensure consistency with inode marshal logic
// while skipping time fields (CreateTime, AccessTime, ModifyTime, LeaseExpireTime) for compatibility
func writeInodeToBufferForMd5(inode *Inode, buff *bytes.Buffer) (err error) {
	// Use MarshalValueV2WithSkip with skipTimeFields=true to maintain compatibility
	tmpBuf := GetInodeBuf()
	defer PutInodeBuf(tmpBuf)

	inode.MarshalValueV2WithSkip(tmpBuf, true)
	valBytes := tmpBuf.Bytes()
	_, err = buff.Write(valBytes)
	return
}

func CalculateInodeMd5Sum(msg *storeMsg, h hash.Hash, buff *bytes.Buffer) (err error) {
	err = msg.snap.RangeReuseInode(func(inode *Inode) bool {
		buff.Reset()
		err = writeInodeToBufferForMd5(inode, buff)
		if err != nil {
			log.LogErrorf("[CalculateInodeMd5Sum] failed to write inode, err(%v)", err)
			return false
		}
		_, err = buff.WriteTo(h)
		if err != nil {
			log.LogErrorf("[CalculateInodeMd5Sum] failed to write inode, err(%v)", err)
			return false
		}
		return true
	})
	return
}

func WriteDentryToBuffer(dentry *Dentry, buff *bytes.Buffer) (err error) {
	err = binary.Write(buff, binary.BigEndian, dentry.ParentId)
	if err != nil {
		log.LogErrorf("[WriteDentryToBuffer] failed to write dentry, err(%v)", err)
		return
	}
	err = binary.Write(buff, binary.BigEndian, dentry.Inode)
	if err != nil {
		log.LogErrorf("[WriteDentryToBuffer] failed to write dentry, err(%v)", err)
		return
	}
	err = binary.Write(buff, binary.BigEndian, dentry.Type)
	if err != nil {
		log.LogErrorf("[WriteDentryToBuffer] failed to write dentry, err(%v)", err)
		return
	}
	_, err = buff.WriteString(dentry.Name)
	if err != nil {
		log.LogErrorf("[WriteDentryToBuffer] failed to write dentry, err(%v)", err)
		return
	}
	if dentry.multiSnap != nil {
		err = binary.Write(buff, binary.BigEndian, dentry.multiSnap.VerSeq)
		if err != nil {
			log.LogErrorf("[WriteDentryToBuffer] failed to write dentry, err(%v)", err)
			return
		}
		for _, multiVersion := range dentry.multiSnap.dentryList {
			err = WriteDentryToBuffer(multiVersion, buff)
			if err != nil {
				log.LogErrorf("[WriteDentryToBuffer] failed to write dentry, err(%v)", err)
				return
			}
		}
	}

	return nil
}

func CalculateDentryMd5Sum(msg *storeMsg, h hash.Hash, buff *bytes.Buffer) (err error) {
	err = msg.snap.RangeReuseDentry(func(dentry *Dentry) bool {
		buff.Reset()
		err = WriteDentryToBuffer(dentry, buff)
		if err != nil {
			log.LogErrorf("[CalculateDentryMd5Sum] failed to write dentry, err(%v)", err)
			return false
		}
		_, err = buff.WriteTo(h)
		if err != nil {
			log.LogErrorf("[CalculateDentryMd5Sum] failed to write dentry, err(%v)", err)
			return false
		}
		return true
	})

	return
}

func WriteExtentToBuffer(extent *Extend, buff *bytes.Buffer) (err error) {
	err = binary.Write(buff, binary.BigEndian, extent.inode)
	if err != nil {
		log.LogErrorf("[WriteExtentToBuffer] failed to write extent, err(%v)", err)
		return
	}
	_, err = buff.Write(extent.Quota)
	if err != nil {
		log.LogErrorf("[WriteExtentToBuffer] failed to write extent, err(%v)", err)
		return
	}
	keys := make([]string, 0, len(extent.dataMap))
	for key := range extent.dataMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		_, err = buff.WriteString(key)
		if err != nil {
			log.LogErrorf("[WriteExtentToBuffer] failed to write extent, err(%v)", err)
			return
		}
		_, err = buff.Write(extent.dataMap[key])
		if err != nil {
			log.LogErrorf("[WriteExtentToBuffer] failed to write extent, err(%v)", err)
			return
		}
	}

	if extent.multiSnap != nil {
		err = binary.Write(buff, binary.BigEndian, extent.multiSnap.verSeq)
		if err != nil {
			log.LogErrorf("[WriteExtentToBuffer] failed to write extent, err(%v)", err)
			return
		}
		for _, multiVersion := range extent.multiSnap.multiVers {
			err = WriteExtentToBuffer(multiVersion, buff)
			if err != nil {
				log.LogErrorf("[WriteExtentToBuffer] failed to write extent, err(%v)", err)
				return
			}
		}
	}

	return nil
}

func CalculateExtentMd5Sum(msg *storeMsg, h hash.Hash, buff *bytes.Buffer) (err error) {
	err = msg.snap.Range(ExtendType, func(i interface{}) bool {
		e := i.(*Extend)
		buff.Reset()
		err = WriteExtentToBuffer(e, buff)
		if err != nil {
			log.LogErrorf("[CalculateExtentMd5Sum] failed to write extent, err(%v)", err)
			return false
		}
		_, err = buff.WriteTo(h)
		if err != nil {
			log.LogErrorf("[CalculateExtentMd5Sum] failed to write extent, err(%v)", err)
			return false
		}
		return true
	})

	return
}

func CalculateMultipartMd5Sum(msg *storeMsg, h hash.Hash, buff *bytes.Buffer) (err error) {
	err = msg.snap.Range(MultipartType, func(i interface{}) bool {
		m := i.(*Multipart)
		buff.Reset()
		_, err = buff.WriteString(m.id)
		if err != nil {
			log.LogErrorf("[CalculateMultipartMd5Sum] failed to write multipart, err(%v)", err)
			return false
		}
		_, err = buff.WriteString(m.key)
		if err != nil {
			log.LogErrorf("[CalculateMultipartMd5Sum] failed to write multipart, err(%v)", err)
			return false
		}
		for _, part := range m.parts {
			err = binary.Write(buff, binary.BigEndian, part.ID)
			if err != nil {
				log.LogErrorf("[CalculateMultipartMd5Sum] failed to write multipart, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(part.MD5)
			if err != nil {
				log.LogErrorf("[CalculateMultipartMd5Sum] failed to write multipart, err(%v)", err)
				return false
			}
			err = binary.Write(buff, binary.BigEndian, part.Size)
			if err != nil {
				log.LogErrorf("[CalculateMultipartMd5Sum] failed to write multipart, err(%v)", err)
				return false
			}
			err = binary.Write(buff, binary.BigEndian, part.Inode)
			if err != nil {
				log.LogErrorf("[CalculateMultipartMd5Sum] failed to write multipart, err(%v)", err)
				return false
			}
		}
		keys := make([]string, 0, len(m.extend))
		for key := range m.extend {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			_, err = buff.WriteString(key)
			if err != nil {
				log.LogErrorf("[CalculateMultipartMd5Sum] failed to write multipart, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(m.extend[key])
			if err != nil {
				log.LogErrorf("[CalculateMultipartMd5Sum] failed to write multipart, err(%v)", err)
				return false
			}
		}

		_, err = buff.WriteTo(h)
		if err != nil {
			log.LogErrorf("[CalculateMultipartMd5Sum] failed to write multipart, err(%v)", err)
			return false
		}
		return true
	})

	return
}

func CalculateTxInfoMd5Sum(msg *storeMsg, h hash.Hash, buff *bytes.Buffer) (err error) {
	err = msg.snap.Range(TransactionType, func(i interface{}) bool {
		tx := i.(*proto.TransactionInfo)
		buff.Reset()

		_, err = buff.WriteString(tx.TxID)
		if err != nil {
			log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
			return false
		}
		err = binary.Write(buff, binary.BigEndian, tx.TxType)
		if err != nil {
			log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
			return false
		}
		err = binary.Write(buff, binary.BigEndian, tx.TmID)
		if err != nil {
			log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
			return false
		}
		err = binary.Write(buff, binary.BigEndian, tx.State)
		if err != nil {
			log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
			return false
		}
		err = binary.Write(buff, binary.BigEndian, tx.RMFinish)
		if err != nil {
			log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
			return false
		}
		inodeKeys := make([]uint64, 0, len(tx.TxInodeInfos))
		for txId := range tx.TxInodeInfos {
			inodeKeys = append(inodeKeys, txId)
		}
		sort.Slice(inodeKeys, func(i, j int) bool {
			return inodeKeys[i] < inodeKeys[j]
		})
		for _, key := range inodeKeys {
			err = binary.Write(buff, binary.BigEndian, key)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
			info := tx.TxInodeInfos[key]
			err = binary.Write(buff, binary.BigEndian, info.Ino)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
			err = binary.Write(buff, binary.BigEndian, info.MpID)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(info.TxID)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(info.MpMembers)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
		}

		dentryKeys := make([]string, 0, len(tx.TxDentryInfos))
		for dentryKey := range tx.TxDentryInfos {
			dentryKeys = append(dentryKeys, dentryKey)
		}
		sort.Strings(dentryKeys)
		for _, key := range dentryKeys {
			_, err = buff.WriteString(key)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
			info := tx.TxDentryInfos[key]
			err = binary.Write(buff, binary.BigEndian, info.ParentId)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(info.Name)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(info.MpMembers)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(info.TxID)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
			err = binary.Write(buff, binary.BigEndian, info.MpID)
			if err != nil {
				log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
				return false
			}
		}

		_, err = buff.WriteTo(h)
		if err != nil {
			log.LogErrorf("[CalculateTxInfoMd5Sum] failed to write tx info, err(%v)", err)
			return false
		}
		return true
	})

	return
}

func CalculateTxRbInodeMd5Sum(msg *storeMsg, h hash.Hash, buff *bytes.Buffer) (err error) {
	err = msg.snap.Range(TransactionRollbackInodeType, func(i interface{}) bool {
		rbInode := i.(*TxRollbackInode)
		buff.Reset()
		if rbInode.inode != nil {
			err = writeInodeToBufferForMd5(rbInode.inode, buff)
			if err != nil {
				log.LogErrorf("[CalculateTxRbInodeMd5Sum] failed to write tx rb inode, err(%v)", err)
				return false
			}
		}
		if rbInode.txInodeInfo != nil {
			err = binary.Write(buff, binary.BigEndian, rbInode.txInodeInfo.Ino)
			if err != nil {
				log.LogErrorf("[CalculateTxRbInodeMd5Sum] failed to write tx rb inode, err(%v)", err)
				return false
			}
			err = binary.Write(buff, binary.BigEndian, rbInode.txInodeInfo.MpID)
			if err != nil {
				log.LogErrorf("[CalculateTxRbInodeMd5Sum] failed to write tx rb inode, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(rbInode.txInodeInfo.TxID)
			if err != nil {
				log.LogErrorf("[CalculateTxRbInodeMd5Sum] failed to write tx rb inode, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(rbInode.txInodeInfo.MpMembers)
			if err != nil {
				log.LogErrorf("[CalculateTxRbInodeMd5Sum] failed to write tx rb inode, err(%v)", err)
				return false
			}
		}
		err = binary.Write(buff, binary.BigEndian, rbInode.rbType)
		if err != nil {
			log.LogErrorf("[CalculateTxRbInodeMd5Sum] failed to write tx rb inode, err(%v)", err)
			return false
		}
		for _, quotaId := range rbInode.quotaIds {
			err = binary.Write(buff, binary.BigEndian, quotaId)
			if err != nil {
				log.LogErrorf("[CalculateTxRbInodeMd5Sum] failed to write tx rb inode, err(%v)", err)
				return false
			}
		}

		_, err = buff.WriteTo(h)
		if err != nil {
			log.LogErrorf("[CalculateTxRbInodeMd5Sum] failed to write tx rb inode, err(%v)", err)
			return false
		}
		return true
	})
	return
}

func CalculateTxRbDentryMd5Sum(msg *storeMsg, h hash.Hash, buff *bytes.Buffer) (err error) {
	err = msg.snap.Range(TransactionRollbackDentryType, func(i interface{}) bool {
		rbDentry := i.(*TxRollbackDentry)
		buff.Reset()
		if rbDentry.dentry != nil {
			err = WriteDentryToBuffer(rbDentry.dentry, buff)
			if err != nil {
				log.LogErrorf("[CalculateTxRbDentryMd5Sum] failed to write tx rb dentry, err(%v)", err)
				return false
			}
		}
		if rbDentry.txDentryInfo != nil {
			err = binary.Write(buff, binary.BigEndian, rbDentry.txDentryInfo.ParentId)
			if err != nil {
				log.LogErrorf("[CalculateTxRbDentryMd5Sum] failed to write tx rb dentry, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(rbDentry.txDentryInfo.Name)
			if err != nil {
				log.LogErrorf("[CalculateTxRbDentryMd5Sum] failed to write tx rb dentry, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(rbDentry.txDentryInfo.MpMembers)
			if err != nil {
				log.LogErrorf("[CalculateTxRbDentryMd5Sum] failed to write tx rb dentry, err(%v)", err)
				return false
			}
			_, err = buff.WriteString(rbDentry.txDentryInfo.TxID)
			if err != nil {
				log.LogErrorf("[CalculateTxRbDentryMd5Sum] failed to write tx rb dentry, err(%v)", err)
				return false
			}
			err = binary.Write(buff, binary.BigEndian, rbDentry.txDentryInfo.MpID)
			if err != nil {
				log.LogErrorf("[CalculateTxRbDentryMd5Sum] failed to write tx rb dentry, err(%v)", err)
				return false
			}
		}
		err = binary.Write(buff, binary.BigEndian, rbDentry.rbType)
		if err != nil {
			log.LogErrorf("[CalculateTxRbDentryMd5Sum] failed to write tx rb dentry, err(%v)", err)
			return false
		}

		_, err = buff.WriteTo(h)
		if err != nil {
			log.LogErrorf("[CalculateTxRbDentryMd5Sum] failed to write tx rb dentry, err(%v)", err)
			return false
		}
		return true
	})

	return
}

func CalculateUniqCheckerMd5Sum(msg *storeMsg, h hash.Hash, buff *bytes.Buffer) (err error) {
	if msg.uniqChecker == nil {
		return
	}
	keys := make([]uint64, 0, len(msg.uniqChecker.op))
	for key := range msg.uniqChecker.op {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	buff.Reset()
	for _, key := range keys {
		uniqChecker := msg.uniqChecker.op[key]
		err = binary.Write(buff, binary.BigEndian, uniqChecker.uniqid)
		if err != nil {
			log.LogErrorf("[CalculateUniqCheckerMd5Sum] failed to write uniq checker, err(%v)", err)
			return
		}
	}
	_, err = buff.WriteTo(h)
	if err != nil {
		log.LogErrorf("[CalculateUniqCheckerMd5Sum] failed to write uniq checker, err(%v)", err)
		return
	}
	return
}

func CalculateMultiVersionMd5Sum(msg *storeMsg, h hash.Hash, buff *bytes.Buffer) (err error) {
	// Avoid JSON allocations and keep hashing order stable without mutating the source slice.
	vers := make([]*proto.VolVersionInfo, 0, len(msg.multiVerList))
	for _, vv := range msg.multiVerList {
		if vv == nil {
			continue
		}
		vers = append(vers, vv)
	}
	if len(vers) == 0 {
		return
	}

	sort.Slice(vers, func(i, j int) bool {
		return vers[i].Ver < vers[j].Ver
	})

	buff.Reset()
	for _, vv := range vers {
		if err = binary.Write(buff, binary.BigEndian, vv.Ver); err != nil {
			log.LogErrorf("[CalculateMultiVersionMd5Sum] failed to write ver, err(%v)", err)
			return
		}
		if err = binary.Write(buff, binary.BigEndian, vv.DelTime); err != nil {
			log.LogErrorf("[CalculateMultiVersionMd5Sum] failed to write delTime, err(%v)", err)
			return
		}
		if err = binary.Write(buff, binary.BigEndian, vv.Status); err != nil {
			log.LogErrorf("[CalculateMultiVersionMd5Sum] failed to write status, err(%v)", err)
			return
		}
	}

	if _, err = buff.WriteTo(h); err != nil {
		log.LogErrorf("[CalculateMultiVersionMd5Sum] failed to write multi version, err(%v)", err)
		return
	}
	return
}
