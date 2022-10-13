package meta

import (
	"github.com/chubaofs/chubaofs/proto"
	"golang.org/x/net/context"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestMetaAPI(t *testing.T) {
	ctx := context.Background()
	var (
		mw   *MetaWrapper
		err  error
		dir1 = "test1"
		dir2 = "test2"
		dir3 = "test.txt"
	)
	cfg := &MetaConfig{
		Volume:        "ltptest",
		Masters:       []string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"},
		ValidateOwner: true,
		Owner:         "ltptest",
	}
	mw, err = NewMetaWrapper(cfg)
	if err != nil {
		t.Fatalf("creat meta wrapper failed!")
	}
	// make directory: 1.not exit  2.exit
	dir1Inode, err := mw.MakeDirectory(proto.RootIno, dir1)
	if err != nil {
		t.Errorf("make directory(%v) failed", dir1)
	}
	dir2Inode, err := mw.MakeDirectory(dir1Inode, dir2)
	if err != nil {
		t.Errorf("make directory(%v) failed", dir2)
	}
	dir2Inode, err = mw.MakeDirectory(dir1Inode, dir2)
	if err != nil {
		t.Errorf("make directory(%v) failed", dir2)
	}
	// make directory at wrong rootIno
	if _, err = mw.MakeDirectory(0, dir3); err == nil {
		t.Errorf("make directory(%v) failed", dir3)
	}
	// look up path
	absPath := strings.Join([]string{dir1, dir2}, "/")
	inode, err := mw.LookupPath(ctx, absPath)
	if err != nil {
		t.Errorf("look up path err(%v)", err)
	}
	if inode != dir2Inode {
		t.Errorf("look up inode is not equal, get(%v), want(%v)", inode, dir2Inode)
	}
	// get root inode: 1.normal 2.has file
	rootInode, err := mw.GetRootIno(absPath, false)
	if err != nil {
		t.Errorf("get root inode failed: err(%v), dir(%v)", err, absPath)
	}
	if rootInode != dir2Inode {
		t.Errorf("get root inode is wrong")
	}
	filePath := strings.Join([]string{"/cfs/mnt", absPath, dir3}, "/")
	if _, err := os.Create(filePath); err != nil {
		t.Fatalf("create file(%v) failed err(%v)", err, filePath)
	}
	_, err = mw.GetRootIno(filePath, false)
	if err == nil {
		t.Errorf("get root inode failed: err(%v), dir(%v)", err, filePath)
	}
	// make directory at file
	if _, err = mw.MakeDirectory(dir2Inode, dir3); err == nil {
		t.Errorf("make directory(%v) failed", dir3)
	}
	// clean test file
	err = os.RemoveAll(strings.Join([]string{"/cfs/mnt",dir1}, "/"))
	if err != nil {
		t.Errorf("TestMetaAPI: clean test dir(%v) failed, err(%v)", dir1, err)
	}
}

func TestMetaWrapper_Link(t *testing.T) {
	var (
		testFile = "/cfs/mnt/hello.txt"
		linkFile = "/cfs/mnt/link_hello"
		word     = "hello.txt is writing"
	)
	if _, err := os.Create(testFile); err != nil {
		t.Fatalf("create link file failed err(%v)", err)
	}

	err := ioutil.WriteFile(testFile, []byte(word), 0755)
	if err != nil {
		t.Fatalf("write link file failed err(%v)", err)
	}

	err = os.Link(testFile, linkFile)
	if err != nil {
		t.Fatalf("link failed err(%v)", err)
	}
	data, _ := ioutil.ReadFile(linkFile)
	if strings.Compare(string(data), word) != 0 {
		t.Fatalf("link error")
	}
	// clean test file
	err = os.Remove(linkFile)
	if err != nil {
		t.Errorf("TestMetaWrapper_Link: remove link file(%s) failed\n", linkFile)
	}
	err = os.Remove(testFile)
	if err != nil {
		t.Errorf("TestMetaWrapper_Link: remove test file(%s) failed\n", testFile)
	}
}

func TestCreateFileAfterInodeLost(t *testing.T)  {
	ctx := context.Background()
	cfg := &MetaConfig{
		Volume:        "ltptest",
		Masters:       []string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"},
		ValidateOwner: true,
		Owner:         "ltptest",
	}
	mw, err := NewMetaWrapper(cfg)
	if err != nil {
		t.Fatalf("creat meta wrapper failed!")
	}
	tests := []struct {
		name string
		mode uint32
	} {
		{
			name: "test_file",
			mode: 0644,
		},
		{
			name: "test_dir",
			mode: uint32(os.ModeDir),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := mw.Create_ll(ctx, 1, tt.name, tt.mode, 0, 0, nil)
			if err != nil {
				t.Errorf("TestCreateFileAfterInodeLost: create err(%v) name(%v)", err, tt.name)
				return
			}
			if tt.mode != uint32(os.ModeDir) {
				if unlinkInfo, err := mw.InodeUnlink_ll(ctx, info.Inode); err != nil || unlinkInfo.Inode != info.Inode {
					t.Errorf("TestCreateFileAfterInodeLost: unlink err(%v) name(%v) unlinkInfo(%v) oldInfo(%v)",
						err, tt.name, unlinkInfo, info)
					return
				}
			}
			if err = mw.Evict(ctx, info.Inode, true); err != nil {
				t.Errorf("TestCreateFileAfterInodeLost: evict err(%v) name(%v)", err, tt.name)
				return
			}
			newInfo, newErr := mw.Create_ll(ctx, 1, tt.name, tt.mode, 0, 0, nil)
			if newErr != nil || newInfo.Inode == info.Inode {
				t.Errorf("TestCreateFileAfterInodeLost: create again err(%v) name(%v) newInode(%v) oldInode(%v)",
					newErr, tt.name, newInfo, info)
			}
			// clean test file
			if err = os.Remove(strings.Join([]string{"/cfs/mnt",tt.name}, "/")); err != nil {
				t.Errorf("TestCreateFileAfterInodeLost: clean test file(%v) failed, err(%v)", tt.name, err)
			}
		})
	}
}
