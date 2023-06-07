package datanode

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

// MasterClientInterface for mocking
type MasterClientInterface interface {
	AdminAPI() AdminAPIInterface
}

// AdminAPIInterface for mocking
type AdminAPIInterface interface {
	GetClusterInfo() (*ClusterInfo, error)
}

// Manual mock
type ManualMockMasterClient struct {
	adminAPI AdminAPIInterface
}

func (m *ManualMockMasterClient) AdminAPI() AdminAPIInterface {
	return m.adminAPI
}

type ManualMockAdminAPI struct {
	clusterInfo *ClusterInfo
	err         error
}

func (m *ManualMockAdminAPI) GetClusterInfo() (*ClusterInfo, error) {
	return m.clusterInfo, m.err
}

func TestStartStopUpdateNodeInfo(t *testing.T) {
	log.SetLevel(log.LogLevelNone) // disable log

	// Create a DataNode
	dn := &DataNode{}

	// Create a ManualMockMasterClient and ManualMockAdminAPI
	manualMockAdminAPI := &ManualMockAdminAPI{
		clusterInfo: &ClusterInfo{
			DataNodeDeleteLimitRate:     defaultMarkDeleteLimitRate,
			DataNodeAutoRepairLimitRate: defaultIOLimitBurst,
		},
		err: nil,
	}

	manualMockMasterClient := &ManualMockMasterClient{
		adminAPI: manualMockAdminAPI,
	}

	MasterClient = manualMockMasterClient

	stoppedC := make(chan struct{})

	// Run startUpdateNodeInfo in another goroutine
	go func() {
		dn.startUpdateNodeInfo()
		close(stoppedC)
	}()

	// Let startUpdateNodeInfo run for a while
	time.Sleep(100 * time.Millisecond)

	// Call stopUpdateNodeInfo to stop startUpdateNodeInfo
	dn.stopUpdateNodeInfo()

	// Make sure startUpdateNodeInfo is stopped
	select {
	case <-stoppedC:
	case <-time.After(time.Second):
		t.Fatal("startUpdateNodeInfo did not stop after 1 second")
	}

}
