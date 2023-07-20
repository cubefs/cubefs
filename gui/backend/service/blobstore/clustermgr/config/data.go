package config

type ListOutput struct {
	Configs Config `json:"configs"`
}

type Config struct {
	Balance           string `json:"balance"`
	BlobDelete        string `json:"blob_delete"`
	CodeMode          string `json:"code_mode"`
	DiskDrop          string `json:"disk_drop"`
	DiskRepair        string `json:"disk_repair"`
	DiskRepire        string `json:"disk_repire"`
	ShardRepair       string `json:"shard_repair"`
	VolInspect        string `json:"vol_inspect"`
	VolumeChunkSize   string `json:"volume_chunk_size"`
	VolumeInspect     string `json:"volume_inspect"`
	VolumeReserveSize string `json:"volume_reserve_size"`
}