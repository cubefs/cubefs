package jdos

import (
	"fmt"
	"testing"
)

var openAPIV2 = NewJDOSOpenApi("jdimage", "imagedownload", OnlineSite, Erp, OnlineToken)

func TestApiV2GetAllGroupsDetails(t *testing.T) {
	jGroups, err := openAPIV2.GetAllGroupsDetails()
	if err != nil {
		t.Errorf("TestApiV2GetAllGroupsDetails err:%v", err)
		return
	}
	for _, jGroup := range jGroups {
		fmt.Println(jGroup)
	}
}

func TestApiV2GetAllGroupsNames(t *testing.T) {
	jGroups, err := openAPIV2.GetAllGroupsNames()
	if err != nil {
		t.Errorf("TestApiV2GetAllGroupsNames err:%v", err)
		return
	}
	for _, jGroup := range jGroups {
		fmt.Println(jGroup)
	}
}

func TestApiV2GetGroupDetails(t *testing.T) {
	jGroup, err := openAPIV2.GetGroupDetails("testtest")
	if err != nil {
		t.Errorf("TestApiV2GetGroupDetails err:%v", err)
		return
	}
	fmt.Println(jGroup)
}

func TestApiV2GetGroupAllPods(t *testing.T) {
	jDOSPods, err := openAPIV2.GetGroupAllPods("testtest")
	if err != nil {
		t.Errorf("TestApiV2GetGroupAllPods err:%v", err)
		return
	}
	for _, jDOSPod := range jDOSPods {
		fmt.Println(jDOSPod)
	}
}

func TestApiV2GetAllLbs(t *testing.T) {
	jDOSLbs, err := openAPIV2.GetAllLbs()
	if err != nil {
		t.Errorf("TestApiV2GetAllLbs err:%v", err)
		return
	}
	for _, jDOSLb := range jDOSLbs {
		fmt.Println(jDOSLb)
	}
}
