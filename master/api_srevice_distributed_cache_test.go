package master

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"io/ioutil"
	"net/http"
	"testing"
	"github.com/stretchr/testify/assert"
)

var flashGroupIDs []uint64

func TestCreateFlashGroup(t *testing.T) {
	count := 10
	for i := 0; i < count; i++ {
		fgView, err := mc.AdminAPI().CreateFlashGroup("")
		if !assert.NoError(t, err) {
			break
		}
		flashGroupIDs = append(flashGroupIDs, fgView.ID)
	}
	if !assert.Equal(t,count, len(flashGroupIDs), "expect count:%v but get:%v", count, len(flashGroupIDs)) {
		return
	}
	err := checkFlashGroupsStatus(flashGroupIDs, proto.FlashGroupStatus_Inactive, false)
	assert.NoError(t, err)
}

func checkFlashGroupsStatus(fgIDs []uint64, expectStatus proto.FlashGroupStatus, checkFGViews bool) (err error) {
	for _, id := range fgIDs {
		if err = checkFlashGroupStatus(id, expectStatus); err != nil {
			return
		}
	}
	flashGroupsAdminView, err := mc.AdminAPI().ListFlashGroups(expectStatus.IsActive(), false)
	if err != nil {
		return
	}
	allExpectStatusFgIDs := make([]uint64, 0)
	for _, flashGroup := range flashGroupsAdminView.FlashGroups {
		if flashGroup.Status == expectStatus {
			allExpectStatusFgIDs = append(allExpectStatusFgIDs, flashGroup.ID)
		}
	}
	for _, id := range fgIDs {
		flag := false
		for _, expectStatusFgID := range allExpectStatusFgIDs {
			if id == expectStatusFgID {
				flag = true
				break
			}
		}
		if !flag {
			err = fmt.Errorf("id:%v expectStatus:%v should be in list flash groups api", id, expectStatus)
			return
		}
	}

	if !checkFGViews {
		return
	}
	server.cluster.updateFlashGroupResponseCache()
	clientFlashGroups, err := mc.AdminAPI().ClientFlashGroups()
	if err != nil {
		return
	}
	if expectStatus.IsActive() == true {
		for _, id := range fgIDs {
			flag := false
			for _, flashGroup := range clientFlashGroups.FlashGroups {
				if id == flashGroup.ID {
					flag = true
					break
				}
			}
			if !flag {
				err = fmt.Errorf("id:%v expectStatus:%v should be in ClientFlashGroups api", id, expectStatus)
				return
			}
		}
	}
	return
}

func checkFlashGroupStatus(fgID uint64, expectStatus proto.FlashGroupStatus) (err error) {
	fgView, err := mc.AdminAPI().GetFlashGroup(fgID)
	if err != nil {
		return
	}
	if fgView.Status != expectStatus {
		err = fmt.Errorf("fgID:%v expect status:%v but get:%v", fgID, expectStatus, fgView.Status)
	}
	return
}

func TestSetFlashGroup(t *testing.T) {
	if !assert.NotZero(t, len(flashGroupIDs), "flashGroupIDs count is 0") {
		return
	}
	var err error
	fgIDs := flashGroupIDs
	expectStatus := proto.FlashGroupStatus_Active
	err = setFlashGroup(fgIDs, expectStatus, false)
	if !assert.NoErrorf(t, err, "setFlashGroup fgIDs:%v setStatus:%v, err:%v", fgIDs, expectStatus, err){
		return
	}
	expectStatus = proto.FlashGroupStatus_Inactive
	err = setFlashGroup(fgIDs, expectStatus, false)
	if !assert.NoErrorf(t, err, "setFlashGroup fgIDs:%v setStatus:%v, err:%v", fgIDs, expectStatus, err) {
		return
	}
	expectStatus = proto.FlashGroupStatus_Active
	fgID := fgIDs[0]
	err = setFlashGroup([]uint64{fgID}, expectStatus, false)
	assert.NoErrorf(t, err, "setFlashGroup fgID:%v setStatus:%v, err:%v", fgID, expectStatus, err)
}

func setFlashGroup(fgIDs []uint64, setStatus proto.FlashGroupStatus, checkFGViews bool) (err error) {
	for _, id := range fgIDs {
		_, err = mc.AdminAPI().SetFlashGroup(id, setStatus.IsActive())
		if err != nil {
			return
		}
	}
	err = checkFlashGroupsStatus(fgIDs, setStatus, checkFGViews)
	return
}

func TestFlashNodeDecommission_unused(t *testing.T) {
	flashNodeAddr := mfs7Addr
	nodeInfo, err := mc.NodeAPI().GetFlashNode(flashNodeAddr)
	if !assert.NoError(t, err) {
		return
	}
	msg := fmt.Sprintf("expect flashnodeAddr:%v, but get:%v", flashNodeAddr, *nodeInfo)
	if !assert.Equalf(t, flashNodeAddr, nodeInfo.Addr, msg) || !assert.Zerof(t, nodeInfo.FlashGroupID, msg) {
		return
	}
	result, err := mc.NodeAPI().FlashNodeDecommission(flashNodeAddr)
	if !assert.NoError(t, err) {
		return
	}
	t.Log(result)
	allFlashNodes, err := mc.AdminAPI().GetAllFlashNodes(true)
	if !assert.NoError(t, err) {
		return
	}

	for _, flashNodeViewInfos := range allFlashNodes {
		for _, flashNodeViewInfo := range flashNodeViewInfos {
			assert.NotEqualf(t, flashNodeAddr, flashNodeViewInfo.Addr, "flashNodeAddr:%v should not be in flash nodes list", flashNodeAddr)
		}
	}
}

func TestFlashGroupAddFlashNode_givenAddr(t *testing.T) {
	if !assert.NotZerof(t, len(flashGroupIDs), "flashGroupIDs count is 0") {
		return
	}
	fgID := flashGroupIDs[0]
	err := checkFlashGroupAddGivenFlashNode(1, fgID, mfs1Addr)
	if !assert.NoError(t, err) {
		return
	}
	err = checkFlashGroupAddGivenFlashNode(2, fgID, mfs2Addr)
	assert.NoError(t, err)
}

func checkFlashGroupAddGivenFlashNode(expectCount int, flashGroupID uint64, addr string) (err error) {
	fgView, err := mc.AdminAPI().FlashGroupAddFlashNode(flashGroupID, 1, "zoneName", addr)
	if err != nil {
		return
	}
	if fgView.FlashNodeCount != expectCount {
		return fmt.Errorf("expectCount:%v but get:%v", expectCount, fgView.FlashNodeCount)
	}
	nodeInfo, err := mc.NodeAPI().GetFlashNode(addr)
	if err != nil {
		return
	}
	if nodeInfo.FlashGroupID != flashGroupID {
		return fmt.Errorf("expect flashGroupID:%v, but get:%v", flashGroupID, nodeInfo.FlashGroupID)
	}

	count := 0
	for _, flashNodeInfos := range fgView.ZoneFlashNodes {
		for _, flashNodeInfo := range flashNodeInfos {
			if flashNodeInfo.Addr == addr {
				count++
				break
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("addr:%v not in flash group view or too many(count:%v)", addr, count)
	}
	server.cluster.updateFlashGroupResponseCache()
	clientFlashGroups, err := mc.AdminAPI().ClientFlashGroups()
	if err != nil {
		return
	}
	for _, flashGroup := range clientFlashGroups.FlashGroups {
		if flashGroupID == flashGroup.ID {
			if len(flashGroup.Hosts) != expectCount {
				return fmt.Errorf("expectCount:%v but get:%v", expectCount, len(flashGroup.Hosts))
			}
			if !contains(flashGroup.Hosts, addr) {
				return fmt.Errorf("addr:%v should be in ClientFlashGroups api, detail:%v", addr, flashGroup)
			}
		}
	}
	return
}

func TestClientFlashGroups(t *testing.T) {
	server.cluster.flashGroupRespCache = nil
	_, err := mc.AdminAPI().ClientFlashGroups()
	assert.NoError(t, err)
}

func TestAdminListFlashGroups(t *testing.T) {
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminListFlashGroups)
	fmt.Println(reqURL)
	process(reqURL, t)
}

func TestFlashGroup(t *testing.T) {
	reqURL := fmt.Sprintf("%v/flashGroup/set?id=wrong", hostAddr)
	reply, err := processReqURL(reqURL, t)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.NotZerof(t, reply.Code, "reqURL:%v exptct 0 but get reply.Code:%v ", reqURL, reply.Code){
		return
	}


	reqURL = fmt.Sprintf("%v/flashGroup/get?id=wrong", hostAddr)
	reply, err = processReqURL(reqURL, t)
	if !assert.NoError(t, err) {
		return
	}
	assert.NotZerof(t, reply.Code, "reqURL:%v exptct 0 but get reply.Code:%v ", reqURL, reply.Code)
}

func processReqURL(reqURL string, t *testing.T) (reply *proto.HTTPReply, err error) {
	fmt.Println(reqURL)
	var resp *http.Response
	resp, err = http.Get(reqURL)
	if !assert.NoError(t, err) {
		return
	}
	fmt.Println(resp.StatusCode)
	defer resp.Body.Close()
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if !assert.NoError(t, err) {
		return
	}
	t.Log(string(body))
	if !assert.Equalf(t, http.StatusOK, resp.StatusCode, "status code[%v]", resp.StatusCode) {
		return
	}
	reply = &proto.HTTPReply{}
	err = json.Unmarshal(body, reply)
	assert.NoError(t, err)
	return
}

func TestFlashNodeDecommission_used(t *testing.T) {
	fgID := flashGroupIDs[0]
	oldFgView, err := mc.AdminAPI().GetFlashGroup(fgID)
	if !assert.NoError(t, err) {
		return
	}
	flashNodeAddr := mfs1Addr
	nodeInfo, err := mc.NodeAPI().GetFlashNode(flashNodeAddr)
	if !assert.NoError(t, err) {
		return
	}
	msg := fmt.Sprintf("expect flashnodeAddr:%v, but get:%v", flashNodeAddr, *nodeInfo)
	if !assert.Equal(t, flashNodeAddr, nodeInfo.Addr, msg) || !assert.Equal(t, fgID, nodeInfo.FlashGroupID, msg) {
		return
	}
	result, err := mc.NodeAPI().FlashNodeDecommission(flashNodeAddr)
	if !assert.NoError(t, err) {
		return
	}
	t.Log(result)
	allFlashNodes, err := mc.AdminAPI().GetAllFlashNodes(true)
	if !assert.NoError(t, err) {
		return
	}
	for _, flashNodeViewInfos := range allFlashNodes {
		for _, flashNodeViewInfo := range flashNodeViewInfos {
			assert.NotEqualf(t, flashNodeAddr, flashNodeViewInfo.Addr, "flashNodeAddr:%v should not be in flash nodes list", flashNodeAddr)
		}
	}
	//需要选一个同zone新节点
	newFgView, err := mc.AdminAPI().GetFlashGroup(fgID)
	if !assert.NoError(t, err) {
		return
	}
	msg = fmt.Sprintf("expect FlashNodeCount:%v but get:%v", oldFgView.FlashNodeCount, newFgView.FlashNodeCount)
	if !assert.Equalf(t, oldFgView.FlashNodeCount, newFgView.FlashNodeCount, msg) {
		return
	}

	newFgZoneNodes := newFgView.ZoneFlashNodes[nodeInfo.ZoneName]
	oldFgZoneNodes := oldFgView.ZoneFlashNodes[nodeInfo.ZoneName]
	msg = fmt.Sprintf("expect zone:%v FlashNodeCount:%v but get:%v", nodeInfo.ZoneName, len(oldFgZoneNodes), len(newFgZoneNodes))
	assert.Equalf(t, len(oldFgZoneNodes), len(newFgZoneNodes), msg)
}

func TestFlashGroupAddFlashNode_givenZone(t *testing.T) {
	if !assert.NotZerof(t, len(flashGroupIDs), "flashGroupIDs count is 0") {
		return
	}
	fgID := flashGroupIDs[0]
	err := checkFlashGroupAddGivenZoneFlashNode(fgID, 2, testZone2)
	assert.NoError(t, err)
}

func checkFlashGroupAddGivenZoneFlashNode(fgID uint64, count int, zoneName string) (err error) {
	oldFgView, err := mc.AdminAPI().GetFlashGroup(fgID)
	if err != nil {
		return
	}
	newFgView, err := mc.AdminAPI().FlashGroupAddFlashNode(fgID, count, zoneName, "")
	if err != nil {
		return
	}
	if len(newFgView.ZoneFlashNodes[zoneName]) != len(oldFgView.ZoneFlashNodes[zoneName])+count {
		return fmt.Errorf("zone:%v expectCount:%v but get:%v", zoneName, len(oldFgView.ZoneFlashNodes[zoneName])+count, len(newFgView.ZoneFlashNodes[zoneName]))
	}
	for _, flashNodeViewInfo := range newFgView.ZoneFlashNodes[zoneName] {
		if flashNodeViewInfo.FlashGroupID != fgID {
			return fmt.Errorf("flashNode:%v expect fgID:%v but get:%v", flashNodeViewInfo.Addr, fgID, flashNodeViewInfo.FlashGroupID)
		}
	}
	return
}

func TestFlashGroupRemoveFlashNode(t *testing.T) {
	if !assert.NotZerof(t, len(flashGroupIDs), "flashGroupIDs count is 0") {
		return
	}
	fgID := flashGroupIDs[0]
	err := checkFlashGroupRemoveFlashNode(fgID)
	if !assert.NoError(t, err) {
		return
	}
	err = checkFlashGroupRemoveFlashNodeOfGivenZone(fgID, testZone1)
	assert.NoError(t, err)
}

func checkFlashGroupRemoveFlashNode(fgID uint64) (err error) {
	oldFgView, err := mc.AdminAPI().GetFlashGroup(fgID)
	if err != nil {
		return
	}
	zoneName := testZone2
	if len(oldFgView.ZoneFlashNodes[zoneName]) == 0 {
		return fmt.Errorf("zone:%v flash node is 0", zoneName)
	}
	addr := oldFgView.ZoneFlashNodes[zoneName][0].Addr
	newFgView, err := mc.AdminAPI().FlashGroupRemoveFlashNode(fgID, 1, "zoneName", addr)
	if err != nil {
		return
	}
	if newFgView.FlashNodeCount != oldFgView.FlashNodeCount-1 {
		return fmt.Errorf("expect FlashNodeCount:%v, but get:%v", oldFgView.FlashNodeCount-1, newFgView.FlashNodeCount)
	}
	if len(newFgView.ZoneFlashNodes[zoneName]) != len(oldFgView.ZoneFlashNodes[zoneName])-1 {
		return fmt.Errorf("zone:%v expectCount:%v but get:%v", zoneName, len(oldFgView.ZoneFlashNodes[zoneName])-1, len(newFgView.ZoneFlashNodes[zoneName]))
	}
	flashNode, err := mc.NodeAPI().GetFlashNode(addr)
	if err != nil {
		return
	}
	if flashNode.FlashGroupID != 0 {
		return fmt.Errorf("node:%v FlashGroupID should be 0,but get:%v ", flashNode.Addr, flashNode.FlashGroupID)
	}
	return
}

func checkFlashGroupRemoveFlashNodeOfGivenZone(fgID uint64, zoneName string) (err error) {
	oldFgView, err := mc.AdminAPI().GetFlashGroup(fgID)
	if err != nil {
		return
	}
	count := len(oldFgView.ZoneFlashNodes[zoneName])
	if count == 0 {
		return fmt.Errorf("zone:%v node count is 0", zoneName)
	}
	newFgView, err := mc.AdminAPI().FlashGroupRemoveFlashNode(fgID, count, zoneName, "")
	if err != nil {
		return
	}
	if newFgView.FlashNodeCount != oldFgView.FlashNodeCount-count {
		return fmt.Errorf("expect FlashNodeCount:%v, but get:%v", oldFgView.FlashNodeCount-count, newFgView.FlashNodeCount)
	}
	if len(newFgView.ZoneFlashNodes[zoneName]) != len(oldFgView.ZoneFlashNodes[zoneName])-count {
		return fmt.Errorf("zone:%v expectCount:%v but get:%v", zoneName, len(oldFgView.ZoneFlashNodes[zoneName])-count, len(newFgView.ZoneFlashNodes[zoneName]))
	}

	for _, flashNodeViewInfo := range oldFgView.ZoneFlashNodes[zoneName] {
		var flashNode *proto.FlashNodeViewInfo
		flashNode, err = mc.NodeAPI().GetFlashNode(flashNodeViewInfo.Addr)
		if err != nil {
			return
		}
		if flashNode.FlashGroupID != 0 {
			return fmt.Errorf("node:%v FlashGroupID should be 0,but get:%v ", flashNode.Addr, flashNode.FlashGroupID)
		}
	}
	return
}

func TestRemoveFlashGroup(t *testing.T) {
	if !assert.NotZerof(t, len(flashGroupIDs), "flashGroupIDs count is 0") {
		return
	}
	fgID := flashGroupIDs[0]
	flashGroupAdminView, err := mc.AdminAPI().GetFlashGroup(fgID)
	if !assert.NoError(t, err) {
		return
	}
	result, err := mc.AdminAPI().RemoveFlashGroup(fgID)
	if !assert.NoError(t, err) {
		return
	}
	t.Log(result)
	//检查FN的释放情况
	for _, flashNodeInfos := range flashGroupAdminView.ZoneFlashNodes {
		for _, flashNodeInfo := range flashNodeInfos {
			var nodeInfo *proto.FlashNodeViewInfo
			nodeInfo, err = mc.NodeAPI().GetFlashNode(flashNodeInfo.Addr)
			if !assert.NoError(t, err) {
				continue
			}
			assert.Zerof(t, nodeInfo.FlashGroupID, "flash node FlashGroupID should be 0,nodeInfo:%v", *nodeInfo)
		}
	}
}
