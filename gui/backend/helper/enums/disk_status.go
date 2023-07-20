package enums

type DiskStatus int

const (
	DiskStatusNormal = DiskStatus(iota + 1)
	DiskStatusBroken
	DiskStatusRepairing
	DiskStatusRepaired
	DiskStatusDropped
	DiskStatusDropping //协商内容dropped后与原服务器状态码一致
	DiskStatusMax
)
