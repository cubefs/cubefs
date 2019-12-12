package objectnode

import (
	"io"
	"os"
	"sort"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

type VolumeManager interface {
	Volume(volName string) (Volume, error)
	Release(volName string)
	GetStore() (Store, error)
	InitStore(s Store)
	Close()
}

type FSFileInfo struct {
	Path       string
	Size       int64
	Mode       os.FileMode
	ModifyTime time.Time
	ETag       string
	Inode      uint64
}

type Prefixes []string

type PrefixMap map[string]struct{}

func (m PrefixMap) AddPrefix(prefix string) {
	m[prefix] = struct{}{}
}

func (m PrefixMap) Prefixes() Prefixes {
	s := make([]string, 0, len(m))
	for prefix := range m {
		s = append(s, prefix)
	}
	sort.Strings(s)
	return s
}

type FSUpload struct {
	Key          string
	UploadId     string
	StorageClass string
	Initiated    string
}

type FSPart struct {
	PartNumber   int
	LastModified string
	ETag         string
	Size         int
}

type Volume interface {
	OSSSecure() (accessKey, secretKey string)
	OSSMeta() *OSSMeta

	// ListFiles return an FileInfo slice of specified volume, like read dir for hole volume.
	// The result will be ordered by full path.
	ListFilesV1(request *ListBucketRequestV1) ([]*FSFileInfo, string, bool, []string, error)

	ListFilesV2(request *ListBucketRequestV2) ([]*FSFileInfo, uint64, string, bool, []string, error)

	// PutObject create file in specified volume with specified path.
	WriteFile(path string, reader io.Reader) (*FSFileInfo, error)

	// DeleteFile delete specified file from specified volume. If target is not exists then returns error.
	DeleteFile(path string) error

	FileInfo(path string) (*FSFileInfo, error)

	// operation about multipart uploads
	InitMultipart(path string) (multipartID string, err error)
	WritePart(path, multipartID string, partId uint16, reader io.Reader) (*FSFileInfo, error)
	ListParts(path, multipartID string, maxParts, partNumberMarker uint64) ([]*FSPart, uint64, bool, error)
	CompleteMultipart(path, multipartID string) (*FSFileInfo, error)
	AbortMultipart(path, multipartID string) error
	ListMultipartUploads(prefix, delimiter, keyMarker, uploadIdMarker string, maxUploads uint64) ([]*FSUpload, string, string, bool, []string, error)

	ReadFile(path string, writer io.Writer, offset, size uint64) error

	CopyFile(path, sourcePath string) (*FSFileInfo, error)

	SetXAttr(path string, key string, data []byte) error
	GetXAttr(path string, key string) (*proto.XAttrInfo, error)
	DeleteXAttr(path string, key string) error

	Close() error
}
