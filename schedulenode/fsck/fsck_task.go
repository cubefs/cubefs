package fsck

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/bitset"
	"github.com/cubefs/cubefs/util/log"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

type FSCheckTask struct {
	*proto.Task
	checkType             int
	mpParaCnt             int
	safeCleanInterval     time.Duration
	exportDir             string
	needClean             bool
	inodeIDBitSet         *bitset.ByteSliceBitSet
	dentryInodeIDBitSet   *bitset.ByteSliceBitSet
	obsoleteInodeIDBitset *bitset.ByteSliceBitSet
	obsoleteInodeIDs      []uint64
	safeCleanInodeIDs     []uint64
	obsoleteSize          uint64
	safeCleanSize         uint64
	cleanSize             uint64
	obsoleteInodeFile     *os.File
	mpsView               []*proto.MetaPartitionView
	masterClient          *master.MasterClient
	metaWrapper           *meta.MetaWrapper
}

func NewFSCheckTask(task *proto.Task, mc *master.MasterClient, checkType int, needClean bool, safeCleanIntervalMin time.Duration, exportDir string) *FSCheckTask {
	fsckTask := new(FSCheckTask)
	fsckTask.Task = task
	fsckTask.checkType = checkType
	fsckTask.masterClient = mc
	fsckTask.needClean = needClean
	fsckTask.exportDir = exportDir
	fsckTask.safeCleanInterval = safeCleanIntervalMin
	return fsckTask
}

func (t *FSCheckTask) RunOnce() {
	defer func() {
		if t.metaWrapper != nil {
			t.metaWrapper.Close()
		}
	}()
	log.LogInfof("fsckTask RunOnce, %s %s start", t.Cluster, t.VolName)
	var err error
	if err = t.init(); err != nil {
		log.LogErrorf("fsckTask RunOnce, %s %s init task failed: %v", t.Cluster, t.VolName, err)
		return
	}

	if InodeCheckOpt&t.checkType != 0 {
		if err = t.checkObsoleteInode(); err != nil {
			log.LogErrorf("fsckTask RunOnce, %s %s check inode failed: %v", t.Cluster, t.VolName, err)
		}
	}

	if DentryCheckOpt*t.checkType != 0 {
		t.checkObsoleteDentry()
	}

	if err = os.RemoveAll(t.exportDir); err != nil {
		log.LogErrorf("fsckTask RunOnce, %s %s remove export dir %s failed:%v", t.Cluster, t.VolName, t.exportDir, err)
		return
	}
	return
}

func (t *FSCheckTask) init() (err error) {
	metaConf := &meta.MetaConfig{
		Volume:  t.VolName,
		Masters: t.masterClient.Nodes(),
	}
	t.metaWrapper, err = meta.NewMetaWrapper(metaConf)
	if err != nil {
		return
	}

	if err = os.RemoveAll(t.exportDir); err != nil {
		return
	}

	if err = os.MkdirAll(t.exportDir, 0666); err != nil {
		return
	}


	if t.obsoleteInodeFile, err = os.Create(path.Join(t.exportDir, "obsoleteInode.dump")); err != nil {
		return
	}
	_, _ = t.obsoleteInodeFile.Seek(0, 0)

	t.mpsView, err = t.masterClient.ClientAPI().GetMetaPartitions(t.VolName)
	if err != nil {
		return
	}
	if len(t.mpsView) == 0 {
		err = fmt.Errorf("%s %s with error mp count", t.Cluster, t.VolName)
		return
	}
	var maxInodeID uint64
	for _, mpView := range t.mpsView {
		if mpView.End == uint64(math.MaxInt64) {
			maxInodeID = mpView.MaxInodeID
			break
		}
	}
	log.LogDebugf("FSCheckTask init %s %s maxInodeID: %v", t.Cluster, t.VolName, maxInodeID)

	t.inodeIDBitSet = bitset.NewByteSliceBitSetWithCap(int(maxInodeID+proto.DefaultMetaPartitionInodeIDStep))
	t.dentryInodeIDBitSet = bitset.NewByteSliceBitSetWithCap(int(maxInodeID+proto.DefaultMetaPartitionInodeIDStep))
	t.dentryInodeIDBitSet.Set(1)
	t.mpParaCnt = 10
	return
}

func (t *FSCheckTask) checkObsoleteInode() (err error) {
	log.LogDebugf("fsckTask checkObsoleteInode start, cluster:%v, volumeName:%v", t.Cluster, t.VolName)
	endTime := time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day(), time.Now().Hour(), 0, 0, 0, time.Local).Unix()
	if err = t.getVolumeAllInodeID(endTime); err != nil {
		return
	}
	if err = t.getVolumeAllDentryInodeID(); err != nil {
		return
	}

	if err = t.analyze(); err != nil {
		log.LogErrorf("analyze failed, cluster(%v), volumeName(%v), error(%v)", t.Cluster, t.VolName, err)
		return
	}

	if t.needClean {
		t.dumpAndCleanObsoleteInode()
	} else {
		t.dumpObsoleteInode()
	}

	if t.safeCleanInterval < time.Hour * 24 {
		//verify with old func
		t.checkObsoleteInodeOld()
	}

	log.LogDebugf("checkAndCleanInode finish, cluster:%v, volumeName:%v", t.Cluster, t.VolName)
	return
}

func (t *FSCheckTask) checkObsoleteDentry() {
	//todo
}

func (t *FSCheckTask) getVolumeAllInodeID(endTime int64) (err error) {
	var (
		mpCh       = make(chan *proto.MetaPartitionView, 10)
		errCh      = make(chan error, len(t.mpsView))
		inodeIDsCh = make(chan []uint64, 10)
		wg         sync.WaitGroup
		wg2        sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			inodeIDs, ok := <- inodeIDsCh
			if !ok {
				break
			}
			for _, inodeID := range inodeIDs {
				t.inodeIDBitSet.Set(int(inodeID))
			}

		}
	}()

	go func() {
		defer close(mpCh)
		for _, mp := range t.mpsView {
			mpCh <- mp
		}
	}()

	for index := 0; index < t.mpParaCnt; index++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for {
				mp, ok := <- mpCh
				if !ok {
					break
				}
				metaHttpClient := meta.NewMetaHttpClient(fmt.Sprintf("%s:%v", strings.Split(mp.LeaderAddr, ":")[0], t.masterClient.MetaNodeProfPort), false)
				resp, errGetInode := metaHttpClient.ListAllInodesId(mp.PartitionID, 0, 0, endTime)
				if errGetInode != nil {
					log.LogErrorf("GetVolumeAllInodeID get error, task: %v, partitionID: %v, error: %v", t, mp.PartitionID, errGetInode)
					errCh <- errGetInode
					continue
				}
				log.LogDebugf("cluster: %s, volName: %s, inodeCount: %v, inodes: %v, delInodes: %v", t.Cluster, t.VolName, resp.Count, resp.Inodes, resp.DelInodes)
				inodeIDsCh <- resp.Inodes

			}
		}()
	}
	wg2.Wait()
	close(inodeIDsCh)
	wg.Wait()
	select{
	case err = <-errCh:
		return
	default:
	}
	return
}

func (t *FSCheckTask) getVolumeAllDentryInodeID() (err error) {
	var (
		mpCh            = make(chan *proto.MetaPartitionView, 10)
		errCh           = make(chan error, len(t.mpsView))
		dentryInodeIDCh = make(chan uint64, 10)
		wg              sync.WaitGroup
		wg2             sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			inodeID, ok := <- dentryInodeIDCh
			if !ok {
				break
			}
			t.dentryInodeIDBitSet.Set(int(inodeID))

		}
	}()

	go func() {
		defer close(mpCh)
		for _, mp := range t.mpsView {
			mpCh <- mp
		}
	}()

	for index := 0; index < t.mpParaCnt; index++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for {
				mp, ok := <- mpCh
				if !ok {
					break
				}
				errGetInode := t.GetMetaPartitionAllDentryInodeID(mp.PartitionID, mp.LeaderAddr, dentryInodeIDCh)
				if errGetInode != nil {
					log.LogErrorf("getVolumeAllDentryInodeID get error, task: %v, partitionID: %v, error: %v", t, mp.PartitionID, errGetInode)
					errCh <- errGetInode
					continue
				}

				errGetInode = t.GetMetaPartitionAllDelDentryInodeID(mp.PartitionID, mp.LeaderAddr, dentryInodeIDCh)
				if errGetInode != nil {
					log.LogErrorf("getVolumeAllDentryInodeID get error, task: %v, partitionID: %v, error: %v", t, mp.PartitionID, errGetInode)
					errCh <- errGetInode
					continue
				}
			}
		}()
	}
	wg2.Wait()
	close(dentryInodeIDCh)
	wg.Wait()
	select{
	case err = <-errCh:
		return
	default:
	}
	return
}

func parseDentryLineByLine(body io.ReadCloser, f func(data []byte) error) (err error) {
	var (
		buf    []byte
		respOK bool
	)
	defer func() {
		if !respOK {
			err = fmt.Errorf("response not ok")
			return
		}
	}()
	reader := bufio.NewReader(body)
	for {
		var (
			line     []byte
			isPrefix bool
		)

		line, isPrefix, err = reader.ReadLine()
		buf = append(buf, line...)
		if isPrefix {
			continue
		}

		if err != nil {
			if err == io.EOF {
				if len(buf) == 0 {
					err = nil
				} else {
					buf, respOK = truncateBuffer(buf)
					err = f(buf)
				}
			}
			break
		}

		buf, respOK = truncateBuffer(buf)
		if err = f(buf); err != nil {
			break
		}
		buf = buf[:0]
	}
	return
}

func truncateBuffer(buf []byte) ([]byte, bool) {
	respOK := false
	prefixString := `{"data":[`
	line := string(buf)
	if strings.HasPrefix(line, prefixString) {
		line = line[len(prefixString):]
	}

	suffixString := ","
	if strings.HasSuffix(line, suffixString) {
		line = line[:len(line) - len(suffixString)]
	}
	suffixString = `], "code": 200, "msg": "OK"}`
	if strings.HasSuffix(line, suffixString) {
		respOK = true
		line = line[:len(line) - len(suffixString)]
	}

	buf = []byte(line)
	return buf, respOK
}

func (t *FSCheckTask) GetMetaPartitionAllDelDentryInodeID(partitionID uint64, leaderAddr string, dentryInodeCh chan uint64) (err error) {
	var (
		client  = &http.Client{}
		resp    *http.Response
		cmdLine string
	)
	client.Timeout = 120 * time.Second
	cmdLine = fmt.Sprintf("http://%s:%v/getAllDeletedDentry?pid=%d", strings.Split(leaderAddr, ":")[0], t.masterClient.MetaNodeProfPort, partitionID)
	resp, err = client.Get(cmdLine)
	if err != nil {
		err = fmt.Errorf("get request failed: %v %v", cmdLine, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("invalid status code: %v %v", cmdLine, resp.StatusCode)
		return
	}

	dentry := new(proto.Dentry)
	if err = parseDentryLineByLine(resp.Body, func(data []byte) error {
		if len(data) == 0 {
			return nil
		}
		dentry.Inode = 0
		if errInfo := json.Unmarshal(data, dentry); errInfo != nil {
			return fmt.Errorf("unmarshal [data:%s] failed:%v", string(data), errInfo)
		}
		dentryInodeCh <- dentry.Inode
		//dentryInodeCh <- dentry.ParentId
		return nil
	}); err != nil {
		err = fmt.Errorf("parse dentry failed: %v %v", cmdLine, err)
	}
	return
}

func (t *FSCheckTask) GetMetaPartitionAllDentryInodeID(partitionID uint64, leaderAddr string, dentryInodeCh chan uint64) (err error) {
	var (
		client  = &http.Client{}
		resp    *http.Response
		cmdLine string
	)
	client.Timeout = 120 * time.Second
	cmdLine = fmt.Sprintf("http://%s:%v/getAllDentry?pid=%d", strings.Split(leaderAddr, ":")[0], t.masterClient.MetaNodeProfPort, partitionID)
	resp, err = client.Get(cmdLine)
	if err != nil {
		err = fmt.Errorf("get request failed: %v %v", cmdLine, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("invalid status code: %v %v", cmdLine, resp.StatusCode)
		return
	}

	dentry := new(proto.Dentry)
	if err = parseDentryLineByLine(resp.Body, func(data []byte) error {
		if len(data) == 0 {
			return nil
		}
		dentry.Inode = 0
		if errInfo := json.Unmarshal(data, dentry); errInfo != nil {
			return fmt.Errorf("unmarshal [data:%s] failed:%v", string(data), errInfo)
		}
		//dentryInodeCh <- dentry.ParentId
		dentryInodeCh <- dentry.Inode
		return nil
	}); err != nil {
		err = fmt.Errorf("parse dentry failed: %v %v", cmdLine, err)
		return
	}
	return
}

func (t *FSCheckTask) analyze() (err error)  {
	//log.LogDebugf("cluster: %v, volume: %v inodeIDBitSetInfo: %v, dentryInodeIDBitsetInfo: %v", t.Cluster,
	//	t.VolName, t.inodeIDBitSet, t.dentryInodeIDBitSet)
	t.obsoleteInodeIDBitset = t.inodeIDBitSet.Xor(t.dentryInodeIDBitSet).And(t.inodeIDBitSet)
	for index := 0; index < t.obsoleteInodeIDBitset.Cap(); index++ {
		if t.obsoleteInodeIDBitset.Get(index) {
			t.obsoleteInodeIDs = append(t.obsoleteInodeIDs, uint64(index))
		}
	}
	return
}

func (t *FSCheckTask) dumpObsoleteInode() {
	var (
		inodeInfo *proto.InodeInfo
		err error
	)
	for _, inodeID := range t.obsoleteInodeIDs {
		if inodeInfo, err = t.metaWrapper.InodeGet_ll(context.Background(), inodeID); err != nil {
			log.LogInfof("dumpObsoleteInode InodeGet failed, vol: %v, inodeID: %v, err: %v", t.VolName, inodeID, err)
			continue
		}
		t.exportObsoleteInodeToFile(inodeInfo)
		t.obsoleteSize += inodeInfo.Size
		if inodeInfo.Nlink != 0 || time.Since(inodeInfo.ModifyTime) < t.safeCleanInterval || !proto.IsRegular(inodeInfo.Mode) {
			continue
		}
		t.safeCleanSize += inodeInfo.Size
	}
	_ = t.obsoleteInodeFile.Close()
	log.LogInfof("cluster(%v) volume(%v) obsolete size:%v, safe clean size:%v", t.Cluster, t.VolName, t.obsoleteSize, t.safeCleanSize)
	return
}

func (t *FSCheckTask) dumpAndCleanObsoleteInode() {
	var (
		inodeInfo *proto.InodeInfo
		err       error
	)
	for _, inodeID := range t.obsoleteInodeIDs {
		if inodeInfo, err = t.metaWrapper.InodeGet_ll(context.Background(), inodeID); err != nil {
			log.LogInfof("dumpObsoleteInode InodeGet failed, vol: %v, inodeID: %v, err: %v", t.VolName, inodeID, err)
			continue
		}
		t.exportObsoleteInodeToFile(inodeInfo)
		t.obsoleteSize += inodeInfo.Size
		if inodeInfo.Nlink != 0 || time.Since(inodeInfo.ModifyTime) < t.safeCleanInterval || !proto.IsRegular(inodeInfo.Mode) {
			continue
		}
		t.safeCleanInodeIDs = append(t.safeCleanInodeIDs, inodeID)
		t.safeCleanSize += inodeInfo.Size
		err = t.metaWrapper.Evict(context.Background(), inodeID, false)
		if err != nil {
			if err != syscall.ENOENT {
				log.LogErrorf("cluster(%v) volume(%v) evict inode(%v) failed:%v", t.Cluster, t.VolName, inodeInfo, err)
			}
			continue
		}
		log.LogWritef("%v", inodeInfo)
	}
	_ = t.obsoleteInodeFile.Close()
	log.LogInfof("cluster(%v) volume(%v) obsolete size:%v, safe clean size:%v", t.Cluster, t.VolName, t.obsoleteSize, t.safeCleanSize)
	return
}

func (t *FSCheckTask) exportObsoleteInodeToFile(inode *proto.InodeInfo){
	data, err := json.Marshal(inode)
	if err != nil {
		log.LogErrorf("marshal obsolete inode failed, clusterName: %v, volumeName: %v, inode: %v, err: %v",
			t.Cluster, t.VolName, inode, err)
		return
	}
	if _, err = t.obsoleteInodeFile.Write(data); err != nil {
		log.LogErrorf("export obsolete inode failed, clusterName: %v, volumeName: %v, inode: %v, err: %v",
			t.Cluster, t.VolName, inode, err)
	}
	if _, err = t.obsoleteInodeFile.WriteString("\n"); err != nil {
		log.LogErrorf("export line break(%v) to file failed, err:%v", inode, err)
	}
	return
}

var (
	inodeDumpFileName  = "inode.dump"
	dentryDumpFileName = "dentry.dump"
	delDentryDumpFileName = "delDentry.dump"
)

func (t *FSCheckTask) checkObsoleteInodeOld() {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("checkObsoleteInodeOld error: %v", err)
		}
	}()
	var (
		ifile, dfile, ddfile        *os.File
		safeCleanSize, obsoleteSize uint64
	)
	log.LogDebugf("fsckTask checkObsoleteInodeOld start, cluster:%v, volumeName:%v", t.Cluster, t.VolName)
	if ifile, err = os.Create(fmt.Sprintf("%s/%s", t.exportDir, inodeDumpFileName)); err != nil {
		return
	}
	defer func() {
		ifile.Close()
		os.Remove(fmt.Sprintf("%s/%s", t.exportDir, inodeDumpFileName))
	}()
	if dfile, err = os.Create(fmt.Sprintf("%s/%s", t.exportDir, dentryDumpFileName)); err != nil {
		return
	}
	defer func() {
		dfile.Close()
		os.Remove(fmt.Sprintf("%s/%s", t.exportDir, dentryDumpFileName))
	}()
	if ddfile, err = os.Create(fmt.Sprintf("%s/%s", t.exportDir, delDentryDumpFileName)); err != nil {
		return
	}
	defer func() {
		ddfile.Close()
		os.Remove(fmt.Sprintf("%s/%s", t.exportDir, delDentryDumpFileName))
	}()
	if err = t.importRawDataFromRemote(ifile, dfile, ddfile); err != nil {
		return
	}
	// go back to the beginning of the files
	ifile.Seek(0, 0)
	dfile.Seek(0, 0)
	ddfile.Seek(0, 0)

	var imap map[uint64]*Inode
	imap, _, err = t.analyzeOld(ifile, dfile, ddfile)
	if err != nil {
		return
	}

	fp, err := os.Create(fmt.Sprintf("%s/%s", t.exportDir, "obseleteInode.dump.old"))
	if err != nil {
		return
	}
	defer fp.Close()

	safeCleanInodes := make(map[uint64]byte, len(imap))
	for _, inode := range imap {
		if inode.Valid {
			continue
		}
		if _, err = fp.WriteString(inode.String() + "\n"); err != nil {
			return
		}
		obsoleteSize += inode.Size
		if inode.NLink != 0 || time.Since(time.Unix(inode.ModifyTime, 0)) < t.safeCleanInterval || !proto.IsRegular(inode.Type) {
			continue
		}
		safeCleanSize += inode.Size
		safeCleanInodes[inode.Inode] = 0
	}

	log.LogInfof("check by old logical %s %s obsolete size:%v, safe clean size: %v", t.Cluster, t.VolName, obsoleteSize, safeCleanSize)

	for _, inodeID := range t.safeCleanInodeIDs {
		if _, ok := safeCleanInodes[inodeID]; !ok {
			log.LogCriticalf("%s %s check error, inode id: %v", t.Cluster, t.VolName, inodeID)
		}
	}

	if t.safeCleanSize != safeCleanSize {
		log.LogCriticalf("%s %s with diff safe clean size, expect: %v, actual: %v", t.Cluster, t.VolName, safeCleanSize, t.safeCleanSize)
	}

	return
}

func (t *FSCheckTask) importRawDataFromRemote(ifile, dfile, ddfile *os.File) error {
	/*
	 * Get all the meta partitions info
	 */
	mps, err := t.masterClient.ClientAPI().GetMetaPartitions(t.VolName)
	if err != nil {
		return err
	}

	/*
	 * Note that if we are about to clean obsolete inodes,
	 * we should get all inodes before geting all dentries.
	 */
	for _, mp := range mps {
		cmdline := fmt.Sprintf("http://%s:%v/getAllInodes?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], t.masterClient.MetaNodeProfPort, mp.PartitionID)
		if err = exportToFile(ifile, cmdline); err != nil {
			return err
		}
	}

	for _, mp := range mps {
		cmdline := fmt.Sprintf("http://%s:%v/getAllDentry?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], t.masterClient.MetaNodeProfPort, mp.PartitionID)
		if err = exportToFile(dfile, cmdline); err != nil {
			return err
		}
	}

	for _, mp := range mps {
		cmdline := fmt.Sprintf("http://%s:%v/getAllDeletedDentry?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], t.masterClient.MetaNodeProfPort, mp.PartitionID)
		if err = exportToFile(ddfile, cmdline); err != nil {
			return err
		}
	}
	return nil
}

func exportToFile(fp *os.File, cmdline string) error {
	client := &http.Client{}
	client.Timeout = 120 * time.Second
	resp, err := client.Get(cmdline)
	if err != nil {
		return fmt.Errorf("Get request failed: %v %v", cmdline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	if _, err = io.Copy(fp, resp.Body); err != nil {
		return fmt.Errorf("io Copy failed: %v", err)
	}
	_, err = fp.WriteString("\n")
	return err
}

type Inode struct {
	Inode      uint64
	Type       uint32
	Size       uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	NLink      uint32

	Dens  []*Dentry
	Valid bool
}

func (i *Inode) String() string {
	return fmt.Sprintf("Inode: %v, Type: %v, Size: %v, CreateTime: %s, AccessTime: %s, ModifyTime: %s, Nlink: %v, Valid: %v, Dens: %v",
		i.Inode, i.Type, i.Size, time.Unix(i.CreateTime, 0).Format(proto.TimeFormat), time.Unix(i.AccessTime, 0).Format(proto.TimeFormat),
		time.Unix(i.ModifyTime, 0).Format(proto.TimeFormat), i.NLink, i.Valid, i.Dens)
}

type Dentry struct {
	ParentId uint64	`json:"pid"`
	Name     string `json:"name"`
	Inode    uint64 `json:"ino"`
	Type     uint32 `json:"type"`

	Valid bool
}

func (t *FSCheckTask) analyzeOld(ifile, dfile, ddfile *os.File) (imap map[uint64]*Inode, dlist []*Dentry, err error) {
	imap = make(map[uint64]*Inode)
	dlist = make([]*Dentry, 0)

	var inodeCnt, dentryCnt, delDentryCnt uint64 = 0, 0, 0
	/*
	 * Walk through all the inodes to establish inode index
	 */
	err = walkRawFile(ifile, func(data []byte) error {
		if len(data) == 0 {
			return nil
		}
		inode := new(Inode)
		if errInfo := json.Unmarshal(data, inode); errInfo != nil {
			return fmt.Errorf("unmarshal [data:%s] failed:%v", string(data), errInfo)
		}
		imap[inode.Inode] = inode
		return nil
	})
	ifile.Seek(0, 0)
	if err != nil {
		err = fmt.Errorf("parse inode file failed:%v", err)
		return
	}

	/*
	 * Walk through all the dentries to establish inode relations.
	 */
	err = walkRawFile(dfile, func(data []byte) error {
		if len(data) == 0 {
			return nil
		}
		dentryCnt++
		dentry := new(Dentry)
		if errInfo := json.Unmarshal(data, dentry); errInfo != nil {
			return fmt.Errorf("unmarshal [data:%s] failed:%v", string(data), errInfo)
		}

		inode, ok := imap[dentry.ParentId]
		if !ok {
			dlist = append(dlist, dentry)
		} else {
			inode.Dens = append(inode.Dens, dentry)
		}
		return nil
	})
	dfile.Seek(0, 0)
	if err != nil {
		err = fmt.Errorf("parse dentry file failed:%v", err)
		return
	}

	err = walkRawFile(ddfile, func(data []byte) error {
		if len(data) == 0 {
			return nil
		}
		delDentryCnt++
		dentry := new(Dentry)
		if errInfo := json.Unmarshal(data, dentry); errInfo != nil {
			return fmt.Errorf("unmarshal [data:%s] failed:%v", string(data), errInfo)
		}

		inode, ok := imap[dentry.ParentId]
		if !ok {
			dlist = append(dlist, dentry)
		} else {
			inode.Dens = append(inode.Dens, dentry)
		}
		return nil
	})
	ddfile.Seek(0, 0)
	if err != nil {
		err = fmt.Errorf("parse del dentry file failed:%v", err)
		return
	}

	log.LogInfof("inode count:%v, dentry count:%v, delDentry count:%v", inodeCnt, dentryCnt, delDentryCnt)

	root, ok := imap[1]
	if !ok {
		err = fmt.Errorf("No root inode")
		return
	}

	/*
	 * Iterate all the path, and mark reachable inode and dentry.
	 */
	followPath(imap, root)
	return
}

func walkRawFile(fp *os.File, f func(data []byte) error) (err error) {
	var (
		buf    []byte
		respOK bool
	)
	defer func() {
		if !respOK {
			err = fmt.Errorf("response not ok")
			return
		}
	}()
	reader := bufio.NewReader(fp)
	for {
		var (
			line     []byte
			isPrefix bool
		)

		line, isPrefix, err = reader.ReadLine()
		buf = append(buf, line...)
		if isPrefix {
			continue
		}

		if err != nil {
			if err == io.EOF {
				if len(buf) == 0 {
					err = nil
				} else {
					buf, respOK = truncateBuffer(buf)
					err = f(buf)
				}
			}
			break
		}

		buf, respOK = truncateBuffer(buf)
		if err = f(buf); err != nil {
			break
		}
		buf = buf[:0]
	}
	return
}

func followPath(imap map[uint64]*Inode, inode *Inode) {
	inode.Valid = true
	// there is no down path for file inode
	if inode.Type == 0 || len(inode.Dens) == 0 {
		return
	}

	for _, den := range inode.Dens {
		childInode, ok := imap[den.Inode]
		if !ok {
			continue
		}
		den.Valid = true
		followPath(imap, childInode)
	}
}