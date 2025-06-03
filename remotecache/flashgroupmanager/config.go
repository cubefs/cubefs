package flashgroupmanager

const (
	defaultFlashNodeHandleReadTimeout   = 1000
	defaultFlashNodeReadDataNodeTimeout = 3000
)

type clusterConfig struct {
	flashNodeHandleReadTimeout   int
	flashNodeReadDataNodeTimeout int
}

func newClusterConfig() (cfg *clusterConfig) {
	cfg = new(clusterConfig)

	cfg.flashNodeHandleReadTimeout = defaultFlashNodeHandleReadTimeout
	cfg.flashNodeReadDataNodeTimeout = defaultFlashNodeReadDataNodeTimeout
	return
}
