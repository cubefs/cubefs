package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"testing"
	"time"
)

func createDataNodeForTopo(addr, cellName string, ns *nodeSet) (dn *DataNode) {
	dn = newDataNode(addr, "test")
	dn.CellName = cellName
	dn.Total = 1024 * util.GB
	dn.Used = 10 * util.GB
	dn.AvailableSpace = 1024 * util.GB
	dn.Carry = 0.9
	dn.ReportTime = time.Now()
	dn.isActive = true
	dn.NodeSetID = ns.ID
	return
}

func TestSingleCell(t *testing.T) {
	//cell name must be DefaultCellName
	dataNode, err := server.cluster.dataNode(mds1Addr)
	if err != nil {
		t.Error(err)
		return
	}
	if dataNode.CellName != DefaultCellName {
		t.Errorf("cell name should be [%v],but now it's [%v]", DefaultCellName, dataNode.CellName)
	}
	topo := newTopology()
	nodeSet := newNodeSet(1, 6)
	topo.putNodeSet(nodeSet)
	cellName := "test"
	topo.putDataNode(createDataNodeForTopo(mds1Addr, cellName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, cellName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds3Addr, cellName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, cellName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds5Addr, cellName, nodeSet))
	if !nodeSet.isSingleCell() {
		cells := nodeSet.getAllCells()
		t.Errorf("topo should be single cell,cell num [%v]", len(cells))
		return
	}
	replicaNum := 2
	//single cell exclude,if it is a single cell excludeCells don't take effect
	excludeCells := make([]string, 0)
	excludeCells = append(excludeCells, cellName)
	cells, err := nodeSet.allocCells(replicaNum, excludeCells)
	if err != nil {
		t.Error(err)
		return
	}
	if len(cells) != 1 {
		t.Errorf("expect cell num [%v],len(cells) is %v", 0, len(cells))
		fmt.Println(cells)
		return
	}

	//single cell normal
	cells, err = nodeSet.allocCells(replicaNum, nil)
	if err != nil {
		t.Error(err)
		return
	}
	newHosts, _, err := cells[0].getAvailDataNodeHosts(nil, replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(newHosts)
	cells[0].removeDataNode(mds1Addr)
	nodeSet.removeCell(cellName)
}

func TestAllocCells(t *testing.T) {
	topo := newTopology()
	nodeSet := newNodeSet(1, 6)
	topo.putNodeSet(nodeSet)
	cellCount := 3
	//add three cells
	cellName1 := "cell1"
	topo.putDataNode(createDataNodeForTopo(mds1Addr, cellName1, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, cellName1, nodeSet))
	cellName2 := "cell2"
	topo.putDataNode(createDataNodeForTopo(mds3Addr, cellName2, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, cellName2, nodeSet))
	cellName3 := "cell3"
	topo.putDataNode(createDataNodeForTopo(mds5Addr, cellName3, nodeSet))
	nodeSet.dataNodeLen = 5
	cells := nodeSet.getAllCells()
	if len(cells) != cellCount {
		t.Errorf("expect cells num[%v],len(cells) is %v", cellCount, len(cells))
		return
	}
	//only pass replica num
	replicaNum := 2
	cells, err := nodeSet.allocCells(replicaNum, nil)
	if err != nil {
		t.Error(err)
		return
	}
	if len(cells) != replicaNum {
		t.Errorf("expect cells num[%v],len(cells) is %v", replicaNum, len(cells))
		return
	}
	cluster := new(Cluster)
	cluster.t = topo
	hosts, _, err := cluster.chooseTargetDataNodes(nil, nil, nil, replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("ChooseTargetDataHosts in multi cells,hosts[%v]", hosts)
	//test exclude cell
	excludeCells := make([]string, 0)
	excludeCells = append(excludeCells, cellName1)
	cells, err = nodeSet.allocCells(replicaNum, excludeCells)
	if err != nil {
		t.Error(err)
		return
	}
	for _, cell := range cells {
		if cell.name == cellName1 {
			t.Errorf("cell [%v] should be exclued", cellName1)
			return
		}
	}
	nodeSet.removeCell(cellName1)
	nodeSet.removeCell(cellName2)
	nodeSet.removeCell(cellName3)
}
