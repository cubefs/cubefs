package proto

type MPCompactState uint8
type InodeCompactState uint32

const (
	MPCmpGetMPInfo MPCompactState = iota
	MPCmpGetProfPort
	MPCmpListAllIno
	MPCmpGetCmpInodes
	MPCmpWaitSubTask
	MPCmpStopped
)

const (
	InodeCmpInit InodeCompactState = iota
	InodeCmpOpenFile
	InodeCmpCalcCmpEKS
	InodeCmpWaitRead
	InodeCmpWrite
	InodeCmpMetaMerge
	InodeCmpStopped
)

type CompactVolume struct {
	Name       string
	Owner      string
	Status     uint8
	TotalSize  uint64
	UsedSize   uint64
	CreateTime int64
	ForceROW   bool
	CompactTag CompactTag
}

func (cv CompactVolume) String() string {
	return cv.Name
}

type CompactVolumeView struct {
	Cluster        string
	CompactVolumes []*CompactVolume
}

func NewCompactVolumeView(cluster string, compactVolumes []*CompactVolume) *CompactVolumeView {
	return &CompactVolumeView{
		Cluster:        cluster,
		CompactVolumes: compactVolumes,
	}
}

type VolumeCompactView struct {
	ClusterName   string   `json:"clusterName"`
	Name          string   `json:"volName"`
	State         uint32   `json:"state"`
	LastUpdate    int64    `json:"lastUpdate"`
	RunningMPCnt  uint32   `json:"runningMPCnt"`
	RunningMpIds  []uint64 `json:"runningMpIds"`
	RunningInoCnt uint32   `json:"runningInoCnt"`
}

type ClusterCompactView struct {
	ClusterName string               `json:"clusterName"`
	Nodes       []string             `json:"nodes"`
	VolumeInfo  []*VolumeCompactView `json:"volumeInfo"`
}

type CompactWorkerViewInfo struct {
	Port     string                `json:"port"`
	Clusters []*ClusterCompactView `json:"cluster"`
}

type CompactMpTaskViewInfo struct {
	Pid      uint64
	Name     string
	State    uint8
	DealDate string
	DealCnt  int
	Leader   string
	inodes   []uint64
	Last     int
	ErrMsg   string
	ErrCnt   uint32
}

type QueryHTTPResult struct {
	Code int32  `json:"code"`
	Msg  string `json:"msg"`
}
