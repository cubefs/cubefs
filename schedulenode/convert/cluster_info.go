package convert

type ClusterInfo struct {
	ClusterID   string
	ConvertVols map[string]*ConvertVolumeInfo
}

func (c *ClusterInfo) GetVolumeConvertStatus(volName string) (status ConvertStatus) {
	status = ConvertClosing
	if vol, ok := c.ConvertVols[volName]; ok {
		status = vol.Status
	}
	return
}