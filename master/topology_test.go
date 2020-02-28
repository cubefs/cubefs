package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"testing"
	"time"
)

func createDataNodeForTopo(addr, cellName string, ns *nodeSet) (dn *DataNode) {
	dn = newDataNode(addr, cellName, "test")
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
	topo := newTopology()
	cellName := "test"
	cell := newCell(cellName)
	topo.putCell(cell)
	nodeSet := newNodeSet(1, 6, cellName)
	cell.putNodeSet(nodeSet)
	topo.putDataNode(createDataNodeForTopo(mds1Addr, cellName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, cellName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds3Addr, cellName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, cellName, nodeSet))
	topo.putDataNode(createDataNodeForTopo(mds5Addr, cellName, nodeSet))
	nodeSet.dataNodeLen = 5
	if !topo.isSingleCell() {
		cells := topo.getAllCells()
		t.Errorf("topo should be single cell,cell num [%v]", len(cells))
		return
	}
	replicaNum := 2
	//single cell exclude,if it is a single cell excludeCells don't take effect
	excludeCells := make([]string, 0)
	excludeCells = append(excludeCells, cellName)
	cells, err := topo.allocCellsForDataNode(replicaNum, replicaNum, excludeCells)
	if err != nil {
		t.Error(err)
		return
	}
	if len(cells) != 1 {
		t.Errorf("expect cell num [%v],len(cells) is %v", 0, len(cells))
		return
	}

	//single cell normal
	cells, err = topo.allocCellsForDataNode(replicaNum, replicaNum, nil)
	if err != nil {
		t.Error(err)
		return
	}
	newHosts, _, err := cells[0].getAvailDataNodeHosts(nil, nil, replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(newHosts)
	topo.deleteDataNode(createDataNodeForTopo(mds1Addr, cellName, nodeSet))
}

func TestAllocCells(t *testing.T) {
	topo := newTopology()

	cellCount := 3
	//add three cells
	cellName1 := "cell1"
	cell1 := newCell(cellName1)
	nodeSet1 := newNodeSet(1, 6, cellName1)
	cell1.putNodeSet(nodeSet1)
	topo.putCell(cell1)
	topo.putDataNode(createDataNodeForTopo(mds1Addr, cellName1, nodeSet1))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, cellName1, nodeSet1))
	nodeSet1.dataNodeLen = 2
	cellName2 := "cell2"
	cell2 := newCell(cellName2)
	nodeSet2 := newNodeSet(2, 6, cellName2)
	cell2.putNodeSet(nodeSet2)
	topo.putCell(cell2)
	topo.putDataNode(createDataNodeForTopo(mds3Addr, cellName2, nodeSet2))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, cellName2, nodeSet2))
	nodeSet2.dataNodeLen = 2
	cellName3 := "cell3"
	cell3 := newCell(cellName3)
	nodeSet3 := newNodeSet(3, 6, cellName3)
	cell3.putNodeSet(nodeSet3)
	topo.putCell(cell3)
	topo.putDataNode(createDataNodeForTopo(mds5Addr, cellName3, nodeSet3))
	nodeSet3.dataNodeLen = 1
	cells := topo.getAllCells()
	if len(cells) != cellCount {
		t.Errorf("expect cells num[%v],len(cells) is %v", cellCount, len(cells))
		return
	}
	//only pass replica num
	replicaNum := 2
	cells, err := topo.allocCellsForDataNode(replicaNum, replicaNum, nil)
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
	cluster.cfg = newClusterConfig()
	//don't cross cell
	cluster.cfg.crossCellNum = 1
	hosts, _, err := cluster.chooseTargetDataNodes(nil, nil, nil, replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	//cross cell
	cluster.cfg.crossCellNum = 2
	hosts, _, err = cluster.chooseTargetDataNodes(nil, nil, nil, replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("ChooseTargetDataHosts in multi cells,hosts[%v]", hosts)
	//after excluding cell1, alloc cells will be failed
	excludeCells := make([]string, 0)
	excludeCells = append(excludeCells, cellName1)
	cells, err = topo.allocCellsForDataNode(cluster.cfg.crossCellNum, replicaNum, excludeCells)
	if err == nil {
		t.Errorf("allocCellsForDataNode should be failed,len(%v)", len(cells))
	}
	// after excluding cell3, alloc cells will be success
	excludeCells = make([]string, 0)
	excludeCells = append(excludeCells, cellName3)
	cells, err = topo.allocCellsForDataNode(cluster.cfg.crossCellNum, replicaNum, excludeCells)
	if err != nil {
		t.Logf("allocCellsForDataNode failed,err[%v]", err)
	}
	for _, cell := range cells {
		if cell.name == cellName3 {
			t.Errorf("cell [%v] should be exclued", cellName3)
			return
		}
	}
}
