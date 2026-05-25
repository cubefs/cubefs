package meta

import (
	"encoding/json"
	"net"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/stretchr/testify/require"
)

func TestHashBucket(t *testing.T) {
	trash := &Trash{}
	b1 := trash.hashBucket("/a/b", "c")
	b2 := trash.hashBucket("/a/b", "c")
	if b1 != b2 {
		t.Fatalf("hashBucket not deterministic: %s vs %s", b1, b2)
	}
	if len(b1) != BucketHashWidth {
		t.Fatalf("bucket length want %d got %d", BucketHashWidth, len(b1))
	}
}

func TestRecoverPosixPathNamePlain(t *testing.T) {
	trash := &Trash{}
	encoded := "a|__|b|__|c"
	got := trash.recoverPosixPathName(encoded, 0)
	if got != "a/b/c" {
		t.Fatalf("recoverPosixPathName want a/b/c got %s", got)
	}
}

func TestGenerateTmpFileName(t *testing.T) {
	trash := &Trash{}

	if got := trash.generateTmpFileName(""); got != ParentDirPrefix {
		t.Fatalf("root tmp name want %s got %s", ParentDirPrefix, got)
	}

	// For nested path it should end with ParentDirPrefix and encode separators.
	got := trash.generateTmpFileName("a/b")
	if !strings.HasSuffix(got, ParentDirPrefix) {
		t.Fatalf("tmp name should end with ParentDirPrefix, got %s", got)
	}
	if !strings.Contains(got, ParentDirPrefix) {
		t.Fatalf("tmp name should contain encoded separator, got %s", got)
	}
}

func TestExtractTimeStampFromName(t *testing.T) {
	trash := &Trash{}
	now := time.Now().Unix()
	name := "Expired_" + time.Unix(now, 0).Format(ExpiredTimeFormat)
	ts, err := trash.extractTimeStampFromName(name)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ts != now {
		t.Fatalf("timestamp mismatch want %d got %d", now, ts)
	}

	if _, err := trash.extractTimeStampFromName("bad_name"); err == nil {
		t.Fatalf("expect error for bad name")
	}
}

func TestTransferLongFileName(t *testing.T) {
	base := strings.Repeat("x", FileNameLengthMax+10)
	filePath := path.Join("/tmp", base)
	newName, oldName := transferLongFileName(filePath)

	if oldName != base {
		t.Fatalf("old name want %s got %s", base, oldName)
	}
	if !strings.HasPrefix(newName, "/tmp/"+LongNamePrefix) {
		t.Fatalf("new name should start with long name prefix, got %s", newName)
	}
	if !strings.Contains(newName, ParentDirPrefix) {
		t.Fatalf("new name should contain ParentDirPrefix, got %s", newName)
	}
}

func newTrashUnitTestMetaWrapper(t *testing.T, addr string) *MetaWrapper {
	t.Helper()
	mw := newConnTestMetaWrapper()
	mw.EnableTransaction = 0
	mw.FollowerRead = false
	mw.NearRead = false
	mw.DirChildrenNumLimit = 1 << 20
	if mw.ranges == nil {
		mw.ranges = btree.New(32)
	}
	t.Cleanup(func() { mw.conns.Close() })
	addTrashTestPartition(mw, 1, 1, 1<<20, addr)
	return mw
}

func addTrashTestPartition(mw *MetaWrapper, id, start, end uint64, addr string) {
	mw.addPartition(&MetaPartition{
		PartitionID: id,
		Start:       start,
		End:         end,
		LeaderAddr:  addr,
		Members:     []string{addr},
	})
}

func newTrashUnitTestTrash(mw *MetaWrapper) *Trash {
	return &Trash{
		mw:          mw,
		mountPath:   "/",
		trashRoot:   "/.Trash",
		subDirCache: NewDirInodeCache(DefaultDirInodeExpiration, DefaultMaxDirInode),
	}
}

// mockTrashMetaHandler dispatches metanode responses by opcode for trash rename tests.
// lookupAlwaysNoent forces every lookup to return not-exist; otherwise lookupFirstNoent
// only affects the first lookup call.
func mockTrashMetaHandler(t *testing.T, lookupCalls *int32, lookupAlwaysNoent, lookupFirstNoent, igetOK bool) func(net.Conn) error {
	t.Helper()
	return func(conn net.Conn) error {
		for {
			pkt := proto.NewPacket()
			if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
				return err
			}

			resp := proto.NewPacketReqID()
			resp.ReqID = pkt.ReqID
			resp.Opcode = pkt.Opcode
			resp.PartitionID = pkt.PartitionID

			var body []byte
			switch pkt.Opcode {
			case proto.OpMetaInodeGet, proto.OpMetaAsyncInodeGet:
				if igetOK {
					resp.ResultCode = proto.OpOk
					body, _ = json.Marshal(&proto.InodeGetResponse{
						Info: &proto.InodeInfo{Inode: 200, Nlink: 1, Mode: uint32(os.ModeDir)},
					})
				} else {
					resp.ResultCode = proto.OpNotExistErr
				}
			case proto.OpMetaLookup, proto.OpMetaAsyncLookup:
				call := atomic.AddInt32(lookupCalls, 1)
				if lookupAlwaysNoent || (lookupFirstNoent && call == 1) {
					resp.ResultCode = proto.OpNotExistErr
				} else {
					resp.ResultCode = proto.OpOk
					body, _ = json.Marshal(&proto.LookupResponse{Inode: 4242, Mode: 0o644})
				}
			default:
				t.Errorf("unexpected opcode %v", pkt.Opcode)
				resp.ResultCode = proto.OpErr
			}

			if body != nil {
				resp.Data = body
				resp.Size = uint32(len(body))
			}
			if err := resp.WriteToConn(conn); err != nil {
				return err
			}
		}
	}
}

func TestRenameToTrashTempFile_SrcNotFound_Idempotent(t *testing.T) {
	var lookupCalls int32
	addr, cleanup := startMockMetaPacketListener(t, mockTrashMetaHandler(t, &lookupCalls, true, false, true))
	t.Cleanup(cleanup)

	mw := newTrashUnitTestMetaWrapper(t, addr)
	trash := newTrashUnitTestTrash(mw)

	const parentIno = uint64(150)
	const dstParentIno = uint64(180)
	shouldRetry, err := trash.renameToTrashTempFile(
		parentIno, dstParentIno,
		"/data/file.txt", "/.Trash/Current/bucket/tmp-file.txt",
		false,
	)
	require.NoError(t, err)
	require.False(t, shouldRetry)
	require.GreaterOrEqual(t, lookupCalls, int32(2))
}

func TestRenameToTrashTempFile_DstParentPartitionMissing_ShouldRetry(t *testing.T) {
	var lookupCalls int32
	addr, cleanup := startMockMetaPacketListener(t, mockTrashMetaHandler(t, &lookupCalls, false, false, true))
	t.Cleanup(cleanup)

	mw := newTrashUnitTestMetaWrapper(t, addr)
	trash := newTrashUnitTestTrash(mw)

	const parentIno = uint64(150)
	dstDir := "/.Trash/Current/bucket"
	trash.subDirCache.Put(dstDir, &proto.InodeInfo{Inode: 999})

	shouldRetry, err := trash.renameToTrashTempFile(
		parentIno, 2000000,
		"/data/file.txt", path.Join(dstDir, "tmp-file.txt"),
		false,
	)
	require.ErrorIs(t, err, syscall.ENOENT)
	require.True(t, shouldRetry)
	require.Nil(t, trash.subDirCache.Get(dstDir))
	require.Equal(t, int32(1), lookupCalls)
}

func TestRenameToTrashTempFile_DstInodeNotFound_ShouldRetry(t *testing.T) {
	var lookupCalls int32
	addr, cleanup := startMockMetaPacketListener(t, mockTrashMetaHandler(t, &lookupCalls, false, false, false))
	t.Cleanup(cleanup)

	mw := newTrashUnitTestMetaWrapper(t, addr)
	trash := newTrashUnitTestTrash(mw)

	const parentIno = uint64(150)
	const dstParentIno = uint64(180)
	dstDir := "/.Trash/Current/bucket"
	trash.subDirCache.Put(dstDir, &proto.InodeInfo{Inode: dstParentIno})

	shouldRetry, err := trash.renameToTrashTempFile(
		parentIno, dstParentIno,
		"/data/file.txt", path.Join(dstDir, "tmp-file.txt"),
		false,
	)
	require.ErrorIs(t, err, syscall.ENOENT)
	require.True(t, shouldRetry)
	require.Nil(t, trash.subDirCache.Get(dstDir))
	require.Equal(t, int32(1), lookupCalls)
}

func TestRenameToTrashTempFile_SrcParentPartitionMissing_NoRetry(t *testing.T) {
	mw := newConnTestMetaWrapper()
	mw.EnableTransaction = 0
	mw.FollowerRead = false
	mw.NearRead = false
	mw.DirChildrenNumLimit = 1 << 20
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	t.Cleanup(func() { mw.conns.Close() })
	addTrashTestPartition(mw, 1, 1000, 2000, "127.0.0.1:1")

	trash := newTrashUnitTestTrash(mw)

	shouldRetry, err := trash.renameToTrashTempFile(
		150, 180,
		"/data/file.txt", "/.Trash/Current/bucket/tmp-file.txt",
		false,
	)
	require.ErrorIs(t, err, syscall.ENOENT)
	require.False(t, shouldRetry)
}

func TestRenameToTrashTempFile_OtherENOENT_NoRetry(t *testing.T) {
	var lookupCalls int32
	addr, cleanup := startMockMetaPacketListener(t, mockTrashMetaHandler(t, &lookupCalls, false, true, true))
	t.Cleanup(cleanup)

	mw := newTrashUnitTestMetaWrapper(t, addr)
	trash := newTrashUnitTestTrash(mw)

	const parentIno = uint64(150)
	const dstParentIno = uint64(180)

	shouldRetry, err := trash.renameToTrashTempFile(
		parentIno, dstParentIno,
		"/data/file.txt", "/.Trash/Current/bucket/tmp-file.txt",
		false,
	)
	require.ErrorIs(t, err, syscall.ENOENT)
	require.False(t, shouldRetry)
	require.GreaterOrEqual(t, lookupCalls, int32(2))
}

func TestRenameRetryLimitPositive(t *testing.T) {
	require.Greater(t, renameRetryLimit, 0)
}

// mockLookupAlwaysOKHandler answers every lookup with OpOk (used for LookupPath / tx rename dst-exists).
func mockLookupAlwaysOKHandler(t *testing.T, lookupCalls *int32) func(net.Conn) error {
	t.Helper()
	return func(conn net.Conn) error {
		for {
			pkt := proto.NewPacket()
			if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
				return err
			}
			resp := proto.NewPacketReqID()
			resp.ReqID = pkt.ReqID
			resp.Opcode = pkt.Opcode
			resp.PartitionID = pkt.PartitionID

			var body []byte
			switch pkt.Opcode {
			case proto.OpMetaLookup, proto.OpMetaAsyncLookup:
				atomic.AddInt32(lookupCalls, 1)
				resp.ResultCode = proto.OpOk
				body, _ = json.Marshal(&proto.LookupResponse{Inode: 4242, Mode: 0o644})
			default:
				t.Errorf("unexpected opcode %v", pkt.Opcode)
				resp.ResultCode = proto.OpErr
			}

			if body != nil {
				resp.Data = body
				resp.Size = uint32(len(body))
			}
			if err := resp.WriteToConn(conn); err != nil {
				return err
			}
		}
	}
}

// TestPathIsExist_FullTrashBucketPath verifies commit 85c657c: existence check uses
// LookupPath on the full bucket path, not a single basename lookup under Current.
func TestPathIsExist_FullTrashBucketPath(t *testing.T) {
	var lookupCalls int32
	addr, cleanup := startMockMetaPacketListener(t, mockLookupAlwaysOKHandler(t, &lookupCalls))
	t.Cleanup(cleanup)

	mw := newTrashUnitTestMetaWrapper(t, addr)
	trash := newTrashUnitTestTrash(mw)

	fullPath := path.Join("/.Trash", CurrentName, BucketRootPrefix, "abc12345", "tmp-file.txt")
	exists, err := trash.pathIsExist(fullPath, false)
	require.NoError(t, err)
	require.True(t, exists)
	// .Trash, Current, .buckets, bucket hash, file name
	require.GreaterOrEqual(t, lookupCalls, int32(5))
}

func TestPathIsExist_CacheHit(t *testing.T) {
	mw := newConnTestMetaWrapper()
	t.Cleanup(func() { mw.conns.Close() })
	trash := newTrashUnitTestTrash(mw)

	fullPath := path.Join("/.Trash", CurrentName, BucketRootPrefix, "abc12345", "tmp-file.txt")
	trash.subDirCache.Put(fullPath, &proto.InodeInfo{Inode: 99})

	exists, err := trash.pathIsExist(fullPath, false)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestPathIsExist_NotFound(t *testing.T) {
	var lookupCalls int32
	addr, cleanup := startMockMetaPacketListener(t, mockTrashMetaHandler(t, &lookupCalls, true, false, false))
	t.Cleanup(cleanup)

	mw := newTrashUnitTestMetaWrapper(t, addr)
	trash := newTrashUnitTestTrash(mw)

	fullPath := path.Join("/.Trash", CurrentName, BucketRootPrefix, "abc12345", "tmp-file.txt")
	exists, err := trash.pathIsExist(fullPath, false)
	require.NoError(t, err)
	require.False(t, exists)
	require.GreaterOrEqual(t, lookupCalls, int32(1))
}

func TestRenameToTrashTempFile_DstExists_ReturnsShouldRetry(t *testing.T) {
	var lookupCalls int32
	addr, cleanup := startMockMetaPacketListener(t, mockLookupAlwaysOKHandler(t, &lookupCalls))
	t.Cleanup(cleanup)

	mw := newTrashUnitTestMetaWrapper(t, addr)
	mw.EnableTransaction = proto.TxOpMaskRename
	trash := newTrashUnitTestTrash(mw)

	const parentIno = uint64(150)
	const dstParentIno = uint64(180)
	dstDir := path.Join("/.Trash", CurrentName, "bucket")
	trash.subDirCache.Put(dstDir, &proto.InodeInfo{Inode: dstParentIno})

	shouldRetry, err := trash.renameToTrashTempFile(
		parentIno, dstParentIno,
		"/data/file.txt", path.Join(dstDir, "tmp-file.txt"),
		false,
	)
	require.ErrorIs(t, err, syscall.EEXIST)
	require.True(t, shouldRetry)
	require.Equal(t, int32(2), lookupCalls)
}
