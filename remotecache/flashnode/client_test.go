package flashnode

import (
	"io"
	"os"
	"time"

	"github.com/cubefs/cubefs/proto"
)

type MockMetaWrapper struct{}

func NewMockMetaWrapper() *MockMetaWrapper {
	return &MockMetaWrapper{}
}

func (mockMeta *MockMetaWrapper) Lookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error) {
	return
}

func (mockMeta *MockMetaWrapper) InodeGet_ll(inode uint64) (*proto.InodeInfo, error) {
	switch inode {
	case 1:
		return &proto.InodeInfo{
			Inode:      1,
			AccessTime: time.Now().AddDate(0, 0, -2),
			Size:       100,
		}, nil
	case 2:
		return &proto.InodeInfo{
			Inode:      2,
			AccessTime: time.Now().AddDate(0, 0, -3),
			Size:       200,
		}, nil
	case 3:
		return &proto.InodeInfo{
			Inode:      3,
			AccessTime: time.Now().AddDate(0, 0, -4),
		}, nil
	case 6:
		return &proto.InodeInfo{
			Inode:      6,
			AccessTime: time.Now().AddDate(0, 0, -4),
		}, nil
	}
	return nil, nil
}

func (mockMeta *MockMetaWrapper) ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error) {
	// for handleDirLimitDepthFirst
	if parentID == 4 {
		return []proto.Dentry{
			{
				Inode: 5,
				Type:  uint32(os.ModeDir),
			},
			{
				Inode: 6,
				Type:  uint32(420),
			},
		}, nil
	}
	// for handleDirLimitBreadthFirst
	if parentID == 5 {
		return nil, nil
	}
	return []proto.Dentry{
		{
			Inode: 1,
			Type:  uint32(420),
		},
		{
			Inode: 2,
			Type:  uint32(420),
		},
		{
			Inode: 3,
			Type:  uint32(420),
		},
		{
			Inode: 4,
			Type:  uint32(os.ModeDir),
		},
		{
			Inode: 5,
			Type:  uint32(os.ModeDir),
		},
	}, nil
}

func (mockMeta *MockMetaWrapper) GetExtents(inode uint64, isCache, openForWrite, isMigration bool) (gen uint64, size uint64, extents []proto.ExtentKey, err error) {
	var exts []proto.ExtentKey
	exts = append(exts, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	exts = append(exts, proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2})
	exts = append(exts, proto.ExtentKey{FileOffset: 4000, Size: 1000, ExtentId: 3})
	exts = append(exts, proto.ExtentKey{FileOffset: 3000, Size: 500, ExtentId: 4})
	return 0, 3500, exts, nil
}

func (mockMeta *MockMetaWrapper) Close() error {
	return nil
}

type MockExtentClient struct {
	data []byte
}

func NewMockExtentClient() *MockExtentClient {
	return &MockExtentClient{}
}

func (m *MockExtentClient) OpenStream(inode uint64, openForWrite bool, isCache bool, fullPath string) error {
	return nil
}

func (m *MockExtentClient) CloseStream(inode uint64) error {
	return nil
}

func (m *MockExtentClient) Read(inode uint64, data []byte, offset int, size int, storageClass uint32, isMigration bool) (read int, err error) {
	if isMigration {
		for i := 0; i < size; i++ {
			data[i] = m.data[i]
		}
		return len(data), io.EOF
	}
	for i := 0; i < size; i++ {
		data[i] = 'a'
	}
	return len(data), io.EOF
}

func (m *MockExtentClient) ForceRefreshExtentsCache(inode uint64) error { return nil }

func (m *MockExtentClient) Flush(inode uint64) error {
	return nil
}

func (m *MockExtentClient) Close() error {
	return nil
}
