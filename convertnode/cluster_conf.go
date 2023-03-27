package convertnode

import (
	"container/list"
	"github.com/cubefs/cubefs/proto"
	sdk "github.com/cubefs/cubefs/sdk/master"
	"sync"
)

type ClusterInfo struct {
	sync.RWMutex
	mc  			*sdk.MasterClient
	name			string
	taskList		*list.List
}

func NewClusterInfo(clusterName string, nodes []string) *ClusterInfo {
	clusterInfo := new(ClusterInfo)
	clusterInfo.name = clusterName
	clusterInfo.mc = sdk.NewMasterClient(nodes, false)
	clusterInfo.taskList = list.New()

	return clusterInfo
}


func (cluster *ClusterInfo) Nodes() (nodes []string){
	cluster.RLock()
	defer cluster.RUnlock()

	return cluster.mc.Nodes()
}

func (cluster *ClusterInfo) AddNode(node string) {
	cluster.Lock()
	defer cluster.Unlock()

	cluster.mc.AddNode(node)
	return
}

func (cluster *ClusterInfo) UpdateNodes(nodes []string) {
	cluster.Lock()
	defer cluster.Unlock()

	cluster.mc = sdk.NewMasterClient(nodes, false)
	return
}

func (cluster *ClusterInfo) AddTask(task *ConvertTask){
	cluster.Lock()
	defer cluster.Unlock()

	task.elemC = cluster.taskList.PushBack(task)
	return
}

func (cluster *ClusterInfo) DelTask(task *ConvertTask){
	cluster.Lock()
	defer cluster.Unlock()

	if task.elemC != nil {
		cluster.taskList.Remove(task.elemC)
	}
	task.elemC = nil
	return
}

func (cluster *ClusterInfo) TaskCount() int{
	cluster.RLock()
	defer cluster.RUnlock()

	return cluster.taskList.Len()
}

func (cluster *ClusterInfo) GenClusterDetailView() (view *proto.ConvertClusterDetailInfo){
	cluster.RLock()
	defer cluster.RUnlock()

	view = &proto.ConvertClusterDetailInfo{}
	view.Cluster = &proto.ConvertClusterInfo{ClusterName: cluster.name, Nodes: cluster.mc.Nodes()}
	view.Tasks = make([]*proto.ConvertTaskInfo, 0, cluster.taskList.Len())
	for elem := cluster.taskList.Front(); elem != nil; elem = elem.Next() {
		task := elem.Value.(*ConvertTask)
		view.Tasks = append(view.Tasks, task.info)
	}
	return
}

func (cluster *ClusterInfo) adminAPI() (*sdk.AdminAPI) {
	return cluster.mc.AdminAPI()
}

func (cluster *ClusterInfo) clientAPI() (*sdk.ClientAPI) {
	return cluster.mc.ClientAPI()
}

func (cluster *ClusterInfo) nodeAPI() (*sdk.NodeAPI) {
	return cluster.mc.NodeAPI()
}