package proto

type SpaceStatus uint8

const (
	SpaceStatusInit = SpaceStatus(iota + 1)
	SpaceStatusNormal
)

type ShardUpdateType uint8

const (
	ShardUpdateTypeAddMember    ShardUpdateType = 0
	ShardUpdateTypeRemoveMember ShardUpdateType = 1
	ShardUpdateTypeSetNormal    ShardUpdateType = 2
)

type FieldType uint8

const (
	FieldTypeBool   = 1
	FieldTypeInt    = 2
	FieldTypeFloat  = 3
	FieldTypeString = 4
	FieldTypeBytes  = 5
)

type IndexOption uint8

const (
	IndexOptionNull     = 0
	IndexOptionIndexed  = 1
	IndexOptionFulltext = 2
	IndexOptionUnique   = 3
)

type FieldID uint32

type ShardTaskType uint8

const (
	ShardTaskTypeClearShard = 1
	ShardTaskTypeCheckpoint = 2
)
