package cluster
//auto generral by sdk/graphql general.go

import "context"
import "github.com/cubefs/cubefs/sdk/graphql/client"
import "time"

type ClusterClient struct {
	*client.MasterGClient
}

func NewClusterClient(c *client.MasterGClient) *ClusterClient {
	return &ClusterClient{c}
}

//struct begin .....
type GeneralResp struct {
	Code	int32
	Message	string
}

type NodeView struct {
	Addr	string
	ID	uint64
	IsWritable	bool
	Status	bool
	ToDataNode	*DataNode
	ToMetaNode	*metaNode
}

type PartitionReport struct {
	DiskPath	string
	ExtentCount	int
	IsLeader	bool
	NeedCompare	bool
	PartitionID	uint64
	PartitionStatus	int
	Total	uint64
	Used	uint64
	VolName	string
}

type metaNode struct {
	Addr	string
	Carry	float64
	ID	uint64
	IsActive	bool
	MaxMemAvailWeight	uint64
	MetaPartitionCount	int
	MetaPartitionInfos	[]MetaPartitionReport
	NodeSetID	uint64
	PersistenceMetaPartitions	[]uint64
	Ratio	float64
	ReportTime	time.Time
	SelectCount	uint64
	Threshold	float32
	Total	uint64
	Used	uint64
	ZoneName	string
}

type ClusterView struct {
	Applied	uint64
	BadMetaPartitionIDs	[]BadPartitionView
	BadPartitionIDs	[]BadPartitionView
	DataNodeCount	int32
	DataNodeStatInfo	*NodeStatInfo
	DataNodes	[]NodeView
	DataPartitionCount	int32
	DisableAutoAlloc	bool
	LeaderAddr	string
	MasterCount	int32
	MaxDataPartitionID	uint64
	MaxMetaNodeID	uint64
	MaxMetaPartitionID	uint64
	MetaNodeCount	int32
	MetaNodeStatInfo	*NodeStatInfo
	MetaNodeThreshold	float32
	MetaNodes	[]NodeView
	MetaPartitionCount	int32
	Name	string
	ServerCount	int32
	VolStatInfo	[]VolStatInfo
	VolumeCount	int32
}

type DataNode struct {
	Addr	string
	AvailableSpace	uint64
	BadDisks	[]string
	Carry	float64
	DataPartitionCount	uint32
	DataPartitionReports	[]PartitionReport
	ID	uint64
	IsActive	bool
	NodeSetID	uint64
	PersistenceDataPartitions	[]uint64
	ReportTime	time.Time
	SelectedTimes	uint64
	Total	uint64
	UsageRatio	float64
	Used	uint64
	ZoneName	string
}

type MasterInfo struct {
	Addr	string
	Index	string
	IsLeader	bool
}

type MetaPartitionReport struct {
	End	uint64
	IsLeader	bool
	MaxInodeID	uint64
	PartitionID	uint64
	Start	uint64
	Status	int
	VolName	string
}

type NodeStatInfo struct {
	IncreasedGB	int64
	TotalGB	uint64
	UsedGB	uint64
	UsedRatio	string
}

type RWMutex struct {
}

type Vol struct {
	Capacity	uint64
	FollowerRead	bool
	ID	uint64
	Name	string
	NeedToLowerReplica	bool
	OSSAccessKey	string
	OSSSecretKey	string
	Owner	string
	RWMutex	RWMutex
	Status	uint8
}

type VolStatInfo struct {
	EnableToken	bool
	Name	string
	ToVolume	*Vol
	TotalSize	uint64
	UsedRatio	string
	UsedSize	uint64
}

type BadPartitionView struct {
	PartitionIDs	[]uint64
	Path	string
}

//function begin .....
func (c *ClusterClient) DecommissionDataNode (ctx context.Context, offLineAddr string) (*GeneralResp, error){

		req := client.NewRequest(ctx, `mutation($offLineAddr: string){
			decommissionDataNode(offLineAddr: $offLineAddr){
				code
				message
			}

		}`)
	
		req.Var("offLineAddr", offLineAddr)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "decommissionDataNode"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) ClusterView (ctx context.Context) (*ClusterView, error){

		req := client.NewRequest(ctx, `query(){
			clusterView{
				applied
				badMetaPartitionIDs{
					partitionIDs
					path
				}
				badPartitionIDs{
					partitionIDs
					path
				}
				dataNodeCount
				dataNodeStatInfo{
					increasedGB
					totalGB
					usedGB
					usedRatio
				}
				dataNodes{
					addr
					iD
					isWritable
					status
					toDataNode{
						addr
						availableSpace
						badDisks
						carry
						dataPartitionCount
						dataPartitionReports{
							diskPath
							extentCount
							isLeader
							needCompare
							partitionID
							partitionStatus
							total
							used
							volName
						}
						iD
						isActive
						nodeSetID
						persistenceDataPartitions
						reportTime
						selectedTimes
						total
						usageRatio
						used
						zoneName
					}
					toMetaNode{
						addr
						carry
						iD
						isActive
						maxMemAvailWeight
						metaPartitionCount
						metaPartitionInfos{
							end
							isLeader
							maxInodeID
							partitionID
							start
							status
							volName
						}
						nodeSetID
						persistenceMetaPartitions
						ratio
						reportTime
						selectCount
						threshold
						total
						used
						zoneName
					}
				}
				dataPartitionCount
				disableAutoAlloc
				leaderAddr
				masterCount
				maxDataPartitionID
				maxMetaNodeID
				maxMetaPartitionID
				metaNodeCount
				metaNodeStatInfo{
					increasedGB
					totalGB
					usedGB
					usedRatio
				}
				metaNodeThreshold
				metaNodes{
					addr
					iD
					isWritable
					status
					toDataNode{
						addr
						availableSpace
						badDisks
						carry
						dataPartitionCount
						dataPartitionReports{
							diskPath
							extentCount
							isLeader
							needCompare
							partitionID
							partitionStatus
							total
							used
							volName
						}
						iD
						isActive
						nodeSetID
						persistenceDataPartitions
						reportTime
						selectedTimes
						total
						usageRatio
						used
						zoneName
					}
					toMetaNode{
						addr
						carry
						iD
						isActive
						maxMemAvailWeight
						metaPartitionCount
						metaPartitionInfos{
							end
							isLeader
							maxInodeID
							partitionID
							start
							status
							volName
						}
						nodeSetID
						persistenceMetaPartitions
						ratio
						reportTime
						selectCount
						threshold
						total
						used
						zoneName
					}
				}
				metaPartitionCount
				name
				serverCount
				volStatInfo{
					enableToken
					name
					toVolume{
						capacity
						followerRead
						iD
						name
						needToLowerReplica
						oSSAccessKey
						oSSSecretKey
						owner
						rWMutex{
						}
						status
					}
					totalSize
					usedRatio
					usedSize
				}
				volumeCount
			}

		}`)
	
	
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := ClusterView{}
	
		if err := rep.GetValueByType(&result, "clusterView"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) DataNodeList (ctx context.Context) ([]DataNode, error){

		req := client.NewRequest(ctx, `query(){
			dataNodeList{
				addr
				availableSpace
				badDisks
				carry
				dataPartitionCount
				dataPartitionReports{
					diskPath
					extentCount
					isLeader
					needCompare
					partitionID
					partitionStatus
					total
					used
					volName
				}
				iD
				isActive
				nodeSetID
				persistenceDataPartitions
				reportTime
				selectedTimes
				total
				usageRatio
				used
				zoneName
			}

		}`)
	
	
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := make([]DataNode,0)
	
		if err := rep.GetValueByType(&result, "dataNodeList"); err != nil {
			return nil, err
		}
	
		return result, nil
	
}

func (c *ClusterClient) DataNodeListTest (ctx context.Context, num int64) ([]DataNode, error){

		req := client.NewRequest(ctx, `query($num: int64){
			dataNodeListTest(num: $num){
				addr
				availableSpace
				badDisks
				carry
				dataPartitionCount
				dataPartitionReports{
					diskPath
					extentCount
					isLeader
					needCompare
					partitionID
					partitionStatus
					total
					used
					volName
				}
				iD
				isActive
				nodeSetID
				persistenceDataPartitions
				reportTime
				selectedTimes
				total
				usageRatio
				used
				zoneName
			}

		}`)
	
		req.Var("num", num)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := make([]DataNode,0)
	
		if err := rep.GetValueByType(&result, "dataNodeListTest"); err != nil {
			return nil, err
		}
	
		return result, nil
	
}

func (c *ClusterClient) MasterList (ctx context.Context) ([]MasterInfo, error){

		req := client.NewRequest(ctx, `query(){
			masterList{
				addr
				index
				isLeader
			}

		}`)
	
	
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := make([]MasterInfo,0)
	
		if err := rep.GetValueByType(&result, "masterList"); err != nil {
			return nil, err
		}
	
		return result, nil
	
}

func (c *ClusterClient) AddMetaNode (ctx context.Context, addr string, id uint64) (*GeneralResp, error){

		req := client.NewRequest(ctx, `mutation($addr: string, $id: uint64){
			addMetaNode(addr: $addr, id: $id){
				code
				message
			}

		}`)
	
		req.Var("addr", addr)
		req.Var("id", id)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "addMetaNode"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) AddRaftNode (ctx context.Context, addr string, id uint64) (*GeneralResp, error){

		req := client.NewRequest(ctx, `mutation($addr: string, $id: uint64){
			addRaftNode(addr: $addr, id: $id){
				code
				message
			}

		}`)
	
		req.Var("addr", addr)
		req.Var("id", id)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "addRaftNode"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) DecommissionMetaPartition (ctx context.Context, nodeAddr string, partitionID uint64) (*GeneralResp, error){

		req := client.NewRequest(ctx, `mutation($nodeAddr: string, $partitionID: uint64){
			decommissionMetaPartition(nodeAddr: $nodeAddr, partitionID: $partitionID){
				code
				message
			}

		}`)
	
		req.Var("nodeAddr", nodeAddr)
		req.Var("partitionID", partitionID)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "decommissionMetaPartition"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) RemoveRaftNode (ctx context.Context, addr string, id uint64) (*GeneralResp, error){

		req := client.NewRequest(ctx, `mutation($addr: string, $id: uint64){
			removeRaftNode(addr: $addr, id: $id){
				code
				message
			}

		}`)
	
		req.Var("addr", addr)
		req.Var("id", id)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "removeRaftNode"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) DataNodeGet (ctx context.Context, addr string) (*DataNode, error){

		req := client.NewRequest(ctx, `query($addr: string){
			dataNodeGet(addr: $addr){
				addr
				availableSpace
				badDisks
				carry
				dataPartitionCount
				dataPartitionReports{
					diskPath
					extentCount
					isLeader
					needCompare
					partitionID
					partitionStatus
					total
					used
					volName
				}
				iD
				isActive
				nodeSetID
				persistenceDataPartitions
				reportTime
				selectedTimes
				total
				usageRatio
				used
				zoneName
			}

		}`)
	
		req.Var("addr", addr)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := DataNode{}
	
		if err := rep.GetValueByType(&result, "dataNodeGet"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) GetTopology (ctx context.Context) (*GeneralResp, error){

		req := client.NewRequest(ctx, `query(){
			getTopology{
				code
				message
			}

		}`)
	
	
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "getTopology"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) MetaNodeGet (ctx context.Context, addr string) (*metaNode, error){

		req := client.NewRequest(ctx, `query($addr: string){
			metaNodeGet(addr: $addr){
				addr
				carry
				iD
				isActive
				maxMemAvailWeight
				metaPartitionCount
				metaPartitionInfos{
					end
					isLeader
					maxInodeID
					partitionID
					start
					status
					volName
				}
				nodeSetID
				persistenceMetaPartitions
				ratio
				reportTime
				selectCount
				threshold
				total
				used
				zoneName
			}

		}`)
	
		req.Var("addr", addr)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := metaNode{}
	
		if err := rep.GetValueByType(&result, "metaNodeGet"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) DecommissionDisk (ctx context.Context, diskPath string, offLineAddr string) (*GeneralResp, error){

		req := client.NewRequest(ctx, `mutation($diskPath: string, $offLineAddr: string){
			decommissionDisk(diskPath: $diskPath, offLineAddr: $offLineAddr){
				code
				message
			}

		}`)
	
		req.Var("diskPath", diskPath)
		req.Var("offLineAddr", offLineAddr)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "decommissionDisk"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) DecommissionMetaNode (ctx context.Context, offLineAddr string) (*GeneralResp, error){

		req := client.NewRequest(ctx, `mutation($offLineAddr: string){
			decommissionMetaNode(offLineAddr: $offLineAddr){
				code
				message
			}

		}`)
	
		req.Var("offLineAddr", offLineAddr)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "decommissionMetaNode"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) LoadMetaPartition (ctx context.Context, partitionID uint64) (*GeneralResp, error){

		req := client.NewRequest(ctx, `mutation($partitionID: uint64){
			loadMetaPartition(partitionID: $partitionID){
				code
				message
			}

		}`)
	
		req.Var("partitionID", partitionID)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "loadMetaPartition"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) ClusterFreeze (ctx context.Context, status bool) (*GeneralResp, error){

		req := client.NewRequest(ctx, `mutation($status: bool){
			clusterFreeze(status: $status){
				code
				message
			}

		}`)
	
		req.Var("status", status)
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := GeneralResp{}
	
		if err := rep.GetValueByType(&result, "clusterFreeze"); err != nil {
			return nil, err
		}
	
		return &result, nil
	
}

func (c *ClusterClient) MetaNodeList (ctx context.Context) ([]metaNode, error){

		req := client.NewRequest(ctx, `query(){
			metaNodeList{
				addr
				carry
				iD
				isActive
				maxMemAvailWeight
				metaPartitionCount
				metaPartitionInfos{
					end
					isLeader
					maxInodeID
					partitionID
					start
					status
					volName
				}
				nodeSetID
				persistenceMetaPartitions
				ratio
				reportTime
				selectCount
				threshold
				total
				used
				zoneName
			}

		}`)
	
	
	
		rep, err := c.Query(ctx, "/api/cluster", req)
		if err != nil {
			return nil, err
		}
		result := make([]metaNode,0)
	
		if err := rep.GetValueByType(&result, "metaNodeList"); err != nil {
			return nil, err
		}
	
		return result, nil
	
}

