package xbp

import "testing"

const (
	TestAPIUser         = "cfs_test" //API用户 需要申请 + 添加到对应的流程中
	TestAPISign         = "6f5e35bf05"
	TestDomain          = "xbp-api-pre.jd.com" //预发环境域名
	TestTicketProcessId = 1435
)

func TestCreateTicket(t *testing.T) {
	m := map[string]string{
		"集群名称":  "CFS国内",
		"角色":    "DN",
		"节点信息":  "11.1.1.1",
		"执行URL": "url"}
	ticketId, err := CreateTicket(TestTicketProcessId, TestDomain, "yangqingyuan8", TestAPISign, TestAPIUser, m)
	if err != nil {
		t.Error(err)
	} else {
		t.Log(ticketId)
	}
}

func TestGetTicketsStatus(t *testing.T) {
	ticketIDs := []int{40247, 40248, 40259, 40260}
	ticketStatus, err := GetTicketsStatus(TestDomain, TestAPISign, TestAPIUser, ticketIDs)
	if err != nil {
		t.Error(err)
	} else {
		t.Log(ticketStatus)
	}
}
