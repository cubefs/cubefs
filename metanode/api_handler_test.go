package metanode

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	PROF_PORT                = 8220
	METAPARTITION_ID         = 1
	INVALID_METAPARTITION_ID = 1000
)

var server = createMetaNodeServerForTest()

func createMetaNodeServerForTest() (m *MetaNode) {
	var err error
	m = &MetaNode{}

	go func() {
		err = http.ListenAndServe(fmt.Sprintf(":%v", PROF_PORT), nil)
		if err != nil {
			panic(err)
		}
	}()

	if err = m.registerAPIHandler(); err != nil {
		return
	}

	return
}

func createMetaPartition(rootDir string, t *testing.T) (mp *metaPartition) {
	mpC := &MetaPartitionConfig{
		PartitionId:   METAPARTITION_ID,
		VolName:       "test_vol",
		Start:         0,
		End:           100,
		PartitionType: 1,
		Peers:         nil,
		RootDir:       rootDir,
	}
	metaM := &metadataManager{
		nodeId:     1,
		zoneName:   "test",
		raftStore:  nil,
		partitions: make(map[uint64]MetaPartition),
		metaNode:   &MetaNode{},
	}

	partition := NewMetaPartition(mpC, metaM)
	require.NotNil(t, partition)

	metaM.partitions[METAPARTITION_ID] = partition
	server.metadataManager = metaM

	mp, ok := partition.(*metaPartition)
	require.True(t, ok)
	msg := &storeMsg{
		command:        1,
		applyIndex:     0,
		inodeTree:      mp.inodeTree,
		dentryTree:     mp.dentryTree,
		extendTree:     mp.extendTree,
		multipartTree:  mp.multipartTree,
		txTree:         mp.txProcessor.txManager.txTree,
		txRbInodeTree:  mp.txProcessor.txResource.txRbInodeTree,
		txRbDentryTree: mp.txProcessor.txResource.txRbDentryTree,
	}
	mp.uidManager = NewUidMgr(mpC.VolName, mpC.PartitionId)
	mp.mqMgr = NewQuotaManager(mpC.VolName, mpC.PartitionId)

	ino := NewInode(1, 0)
	mp.inodeTree.ReplaceOrInsert(ino, true)
	dentry := &Dentry{ParentId: 0, Name: "/", Inode: 1}
	mp.dentryTree.ReplaceOrInsert(dentry, true)

	err := mp.store(msg)
	require.NoError(t, err)
	return
}

func httpReqHandle(url string, t *testing.T) (data []byte) {
	resp, err := http.Get(url)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}

	return body
}

func getSnapshot(t *testing.T, snapshotFile string, url string) {
	testPath := "/tmp/testMetaNodeApiHandler/"
	os.RemoveAll(testPath)
	defer os.RemoveAll(testPath)

	mp := createMetaPartition(testPath, t)
	require.NotNil(t, mp)

	file, err := os.Open(path.Join(mp.config.RootDir, snapshotDir, snapshotFile))
	require.NoError(t, err)
	defer file.Close()

	hash1 := md5.New()
	_, err = io.Copy(hash1, file)
	require.NoError(t, err)

	hash2 := md5.Sum(httpReqHandle(url, t))

	require.Equal(t, hex.EncodeToString(hash1.Sum(nil)[:16]), hex.EncodeToString(hash2[:]))
}

func TestGetInodeSnapshot(t *testing.T) {
	url := fmt.Sprintf("http://127.0.0.1:%v%v?pid=%v",
		PROF_PORT, "/getInodeSnapshot", METAPARTITION_ID)
	fmt.Printf(url)
	fmt.Printf("\n")
	getSnapshot(t, inodeFile, url)
}

func TestGetDentrySnapshot(t *testing.T) {
	url := fmt.Sprintf("http://127.0.0.1:%v%v?pid=%v",
		PROF_PORT, "/getDentrySnapshot", METAPARTITION_ID)
	fmt.Printf(url)
	getSnapshot(t, dentryFile, url)
}

func TestWithWrongMetaPartitionID(t *testing.T) {
	testPath := "/tmp/testMetaNodeApiHandler/"
	os.RemoveAll(testPath)
	defer os.RemoveAll(testPath)

	mp := createMetaPartition(testPath, t)
	require.NotNil(t, mp)

	url := fmt.Sprintf("http://127.0.0.1:%v%v?pid=%v",
		PROF_PORT, "/getInodeSnapshot", 2)

	data := httpReqHandle(url, t)
	require.Contains(t, string(data), "unknown meta partition")
}
