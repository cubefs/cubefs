package img

import (
	"github.com/cubefs/cubefs/util/checktool"
	"testing"
)

func TestName(t *testing.T) {
	s := NewVolStoreMonitor()
	if err := s.extractLbDownloadInfo(checktool.ReDirPath("imglb.json")); err != nil {
		t.Error(err)
	}
	comGroups, localGroups, err := s.getAllGroupsFromApp()
	if err != nil {
		t.Error(err)
	}
	t.Logf("comGroups count:%v,detail:\n", len(comGroups))
	for _, group := range comGroups {
		t.Logf("systemApp:%v groupNames:%v\n", group.systemApp, group.groupNames)
	}
	t.Logf("localGroups count:%v,detail:\n", len(localGroups))
	for _, group := range localGroups {
		t.Logf("systemApp:%v groupNames:%v\n", group.systemApp, group.groupNames)
	}
	comPods, err := s.getAllPodsFromAppGroups(comGroups, LbTypeCom)
	if err != nil {
		t.Errorf("action[checkDownloadLbConfToPodIP] err:%v", err)
	}
	var comPodIps []string
	comPods.Range(func(key, value interface{}) bool {
		comPodIps = append(comPodIps, key.(string))
		return true
	})
	t.Logf("com pod ip count:%v", len(comPodIps))

	var localPodIps []string
	localPods, err := s.getAllPodsFromAppGroups(localGroups, LbTypeLocal)
	if err != nil {
		t.Errorf("action[checkDownloadLbConfToPodIP] err:%v", err)
	}
	localPods.Range(func(key, value interface{}) bool {
		localPodIps = append(localPodIps, key.(string))
		return true
	})
	t.Logf("local pod ip count:%v", len(localPodIps))
}
