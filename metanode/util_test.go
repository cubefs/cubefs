package metanode

import (
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

// initTestInodeStorage fills default StorageClass and PoolId for test inodes when unset.
func initTestInodeStorage(inode *Inode) {
	if inode == nil {
		return
	}
	if !proto.IsValidStorageClass(inode.StorageClass) {
		if proto.IsDir(inode.Type) {
			inode.StorageClass = proto.StorageClass_Replica_HDD
		} else {
			inode.StorageClass = proto.StorageClass_Replica_SSD
		}
	}
	if inode.PoolId == 0 {
		poolId, err := getDefaultPoolIdByStorageClass(inode.StorageClass)
		if err != nil {
			poolId = proto.DefaultSSDPoolId
		}
		inode.PoolId = poolId
	}
}

// mockFileInfo implements os.FileInfo for testing
type mockFileInfo struct {
	name  string
	isDir bool
}

func (m mockFileInfo) Name() string       { return m.name }
func (m mockFileInfo) IsDir() bool        { return m.isDir }
func (m mockFileInfo) Size() int64        { return 0 }
func (m mockFileInfo) Mode() os.FileMode  { return 0 }
func (m mockFileInfo) ModTime() time.Time { return time.Time{} }
func (m mockFileInfo) Sys() interface{}   { return nil }

func TestDelExtFile_GetIdx(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    int64
		expectPanic bool
	}{
		{
			name:        "valid file name",
			input:       "EXTENT_DEL_1",
			expected:    1,
			expectPanic: false,
		},
		{
			name:        "valid file name with multiple digits",
			input:       "EXTENT_DEL_12345",
			expected:    12345,
			expectPanic: false,
		},
		{
			name:        "file name without underscore - should panic",
			input:       "EXTENTDEL",
			expected:    0,
			expectPanic: true,
		},
		{
			name:        "file name with non-numeric index - should panic",
			input:       "EXTENT_DEL_abc",
			expected:    0,
			expectPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tt.expectPanic && r == nil {
					t.Errorf("getDelExtFileIdx(%q) expected panic, but didn't panic", tt.input)
				}
				if !tt.expectPanic && r != nil {
					t.Errorf("getDelExtFileIdx(%q) unexpected panic: %v", tt.input, r)
				}
			}()

			result := getDelExtFileIdx(tt.input)
			if !tt.expectPanic && result != tt.expected {
				t.Errorf("getDelExtFileIdx(%q) = %d, expected %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDelExtFile_Len(t *testing.T) {
	files := DelExtFile{
		mockFileInfo{name: "EXTENT_DEL_1", isDir: false},
		mockFileInfo{name: "EXTENT_DEL_2", isDir: false},
	}

	if files.Len() != 2 {
		t.Errorf("DelExtFile.Len() = %d, expected 2", files.Len())
	}
}

func TestDelExtFile_Swap(t *testing.T) {
	files := DelExtFile{
		mockFileInfo{name: "EXTENT_DEL_1", isDir: false},
		mockFileInfo{name: "EXTENT_DEL_2", isDir: false},
	}

	originalFirst := files[0]
	originalSecond := files[1]

	files.Swap(0, 1)

	if files[0] != originalSecond {
		t.Errorf("Swap(0, 1) failed: first element should be %v, got %v", originalSecond, files[0])
	}
	if files[1] != originalFirst {
		t.Errorf("Swap(0, 1) failed: second element should be %v, got %v", originalFirst, files[1])
	}
}

func TestDelExtFile_Less(t *testing.T) {
	files := DelExtFile{
		mockFileInfo{name: "EXTENT_DEL_1", isDir: false},
		mockFileInfo{name: "EXTENT_DEL_2", isDir: false},
	}

	if !files.Less(0, 1) {
		t.Errorf("DelExtFile.Less(0, 1) should return true")
	}

	if files.Less(1, 0) {
		t.Errorf("DelExtFile.Less(1, 0) should return false")
	}
}

func TestDelExtFile_Sort(t *testing.T) {
	// Test with valid files
	files := []os.FileInfo{
		mockFileInfo{name: "EXTENT_DEL_3", isDir: false},
		mockFileInfo{name: "EXTENT_DEL_1", isDir: false},
		mockFileInfo{name: "EXTENT_DEL_2", isDir: false},
	}

	result := sortDelExtFileInfo(files)

	if len(result) != 3 {
		t.Errorf("sortDelExtFileInfo() returned %d files, expected 3", len(result))
		return
	}

	expected := []string{"EXTENT_DEL_1", "EXTENT_DEL_2", "EXTENT_DEL_3"}
	for i, file := range result {
		if file.Name() != expected[i] {
			t.Errorf("sortDelExtFileInfo() result[%d] = %s, expected %s", i, file.Name(), expected[i])
		}
	}
}

func TestDelExtFile_SortWithDirectories(t *testing.T) {
	// Test filtering out directories
	files := []os.FileInfo{
		mockFileInfo{name: "EXTENT_DEL_2", isDir: false},
		mockFileInfo{name: "some_directory", isDir: true},
		mockFileInfo{name: "EXTENT_DEL_1", isDir: false},
	}

	result := sortDelExtFileInfo(files)

	if len(result) != 2 {
		t.Errorf("sortDelExtFileInfo() returned %d files, expected 2", len(result))
		return
	}

	expected := []string{"EXTENT_DEL_1", "EXTENT_DEL_2"}
	for i, file := range result {
		if file.Name() != expected[i] {
			t.Errorf("sortDelExtFileInfo() result[%d] = %s, expected %s", i, file.Name(), expected[i])
		}
	}
}

func TestDelExtFile_SortEmptyInput(t *testing.T) {
	// Test with empty input
	result := sortDelExtFileInfo([]os.FileInfo{})
	if len(result) != 0 {
		t.Errorf("sortDelExtFileInfo() with empty input returned %d files, expected 0", len(result))
	}

	// Test with nil input
	result = sortDelExtFileInfo(nil)
	if len(result) != 0 {
		t.Errorf("sortDelExtFileInfo() with nil input returned %d files, expected 0", len(result))
	}
}
