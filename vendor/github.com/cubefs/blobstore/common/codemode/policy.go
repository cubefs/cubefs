package codemode

// Policy will be used to adjust code mode's upload range or code mode's volume ratio and so on
type Policy struct {
	CodeMode CodeMode `json:"code_mode"`
	// min size of object, access use this to put object
	MinSize int64 `json:"min_size"`
	// max size of object. access use this to put object
	MaxSize int64 `json:"max_size"`
	// code mode's cluster space size ratio, clusterMgr should use this to create specified num of volume
	SizeRatio float64 `json:"size_ratio"`
	// enable means this kind of code mode enable or not
	// access/allocator will ignore this kind of code mode's allocation when enable is false
	// clustermgr will ignore this kind of code mode's creation when enable is false
	Enable bool `json:"enable"`
}
