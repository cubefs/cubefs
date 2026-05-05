package storage

// CFSConfig holds connection parameters for the CubeFS storage backend.
type CFSConfig struct {
	Masters  []string
	Vol      string
	LogDir   string
	LogLevel string
}
