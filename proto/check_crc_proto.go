package proto

type RepairType uint8

const (
	RepairDataNode RepairType = iota
	RepairVolume
)

type CheckCrcTaskInfo struct {
	Concurrency   uint32     `json:"limit_level"`
	Filter        Filter     `json:"filter"`
	ModifyTimeMin string     `json:"modify_time_min"`
	ModifyTimeMax string     `json:"modify_time_max"`
	RepairType    RepairType `json:"repair_type"`
	CheckTiny     bool       `json:"check_tiny"`
	NodeAddress   string     `json:"node_address"`
}

type Filter struct {
	VolFilter         []string `json:"vol_filter"`
	VolExcludeFilter  []string `json:"vol_exclude_filter"`
	ZoneFilter        []string `json:"zone_filter"`
	ZoneExcludeFilter []string `json:"zone_exclude_filter"`
}
type Frequency struct {
	Interval     uint32 `json:"interval"`
	ExecuteCount uint32 `json:"execute_count"`
}
