package enums

type VolType int

const (
	VolTypeStandard VolType = iota
	VolTypeLowFrequency
)

var VolTypeMsgMap = map[VolType]string{
	VolTypeStandard:     "标准卷",
	VolTypeLowFrequency: "低频卷",
}