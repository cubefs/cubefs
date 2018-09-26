package storage

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
)

type ObjectInfo struct {
	Object        *Object
	SuccessVerify bool
	ErrorMesg     string
}

type BlobFileInfo struct {
	DataFile      string
	IndexFile     string
	Objects       []*ObjectInfo
	NeedVerify    bool
	ErrMesg       string
	AvailChunkCnt int
	Used          int64
	FileBytes     uint64
	DeleteBytes   uint64
	DeletePercent float64
}

func (s *BlobStore) VerfiyObjects(chunkid int, needVerify bool) (blobFileInfo *BlobFileInfo) {
	blobFileInfo = new(BlobFileInfo)
	blobFileInfo.DataFile = path.Join(s.dataDir, fmt.Sprintf("%v", chunkid))
	blobFileInfo.IndexFile = path.Join(s.dataDir, fmt.Sprintf("%v.idx", chunkid))
	blobFileInfo.NeedVerify = needVerify
	blobFileInfo.AvailChunkCnt = s.GetAvailChanLen()
	blobFileInfo.Used = s.UseSize()
	indexfp, err := os.Open(blobFileInfo.IndexFile)
	if err != nil {
		blobFileInfo.ErrMesg = fmt.Sprintf("cannot open indexfile (%v) err(%v)", blobFileInfo.IndexFile)
		return
	}
	defer indexfp.Close()
	datafp, err := os.Open(blobFileInfo.DataFile)
	if err != nil {
		blobFileInfo.ErrMesg = fmt.Sprintf("cannot open datafile (%v) err(%v)", blobFileInfo.DataFile)
		return
	}
	defer datafp.Close()
	cc := s.blobfiles[chunkid]
	if cc == nil {
		blobFileInfo.ErrMesg = fmt.Sprintf("chunkID is not avalid (%v)", chunkid)
		return
	}
	blobFileInfo.FileBytes = cc.tree.fileBytes
	blobFileInfo.DeleteBytes = cc.tree.deleteBytes
	blobFileInfo.DeletePercent = float64(blobFileInfo.DeleteBytes) / float64(blobFileInfo.FileBytes)
	WalkIndexFileAndVerify(indexfp, datafp, blobFileInfo)

	return
}

func WalkIndexFileAndVerify(indexfp *os.File, datafp *os.File, blobFileInfo *BlobFileInfo) (err error) {
	var (
		readOff int64
		count   int
		iter    int
	)

	bytes := make([]byte, ObjectHeaderSize*IndexBatchRead)
	count, err = indexfp.ReadAt(bytes, readOff)
	readOff += int64(count)
	for count > 0 && err == nil || err == io.EOF {
		for iter = 0; iter+ObjectHeaderSize <= count; iter += ObjectHeaderSize {
			ni := new(Object)
			ni.Unmarshal(bytes[iter : iter+ObjectHeaderSize])
			oi := new(ObjectInfo)
			oi.Object = ni
			oi.SuccessVerify = false
			blobFileInfo.Objects = append(blobFileInfo.Objects, oi)
			if ni.Size == TombstoneFileSize {
				oi.SuccessVerify = true
				continue
			}
			if !blobFileInfo.NeedVerify {
				continue
			}
			data := make([]byte, ni.Size)
			_, err = datafp.ReadAt(data, int64(ni.Offset))
			if err != nil {
				oi.ErrorMesg = fmt.Sprintf("oi read datafile err %v", err.Error())
				return
			}
			crc := crc32.ChecksumIEEE(data)
			if crc != ni.Crc {
				oi.ErrorMesg = fmt.Sprintf("expectCrc[%v] actualCrc[%v]", ni.Crc, crc)
			} else {
				oi.SuccessVerify = true
			}
		}

		if err == io.EOF {
			return nil
		}

		count, err = indexfp.ReadAt(bytes, readOff)
		readOff += int64(count)
	}

	return err
}
