package storage

// CFSConfig holds connection parameters for the CubeFS storage backend.
type CFSConfig struct {
	Masters  []string
	Vol      string
	LogDir   string
	LogLevel string

	// ReadChunkSize is the bytes-per-chunk the prefetch reader pulls from
	// the SDK in each call. Larger values amortise SDK call overhead but
	// raise per-file memory (= ReadChunkSize × ReadPrefetch). Zero =
	// default (4 MiB).
	ReadChunkSize int
	// ReadPrefetch is the number of chunks held in flight while the caller
	// drains the reader. Each in-flight chunk uses a separate goroutine
	// and a TCP conn from the SDK pool; with too few, single-file
	// throughput stays at the single-stream cap (~330 MB/s). Zero =
	// default (4).
	ReadPrefetch int
}

const (
	defaultReadChunkSize = 4 * 1024 * 1024
	defaultReadPrefetch  = 4
)

// resolvedChunkSize returns ReadChunkSize if set, else the default.
func (c CFSConfig) resolvedChunkSize() int {
	if c.ReadChunkSize > 0 {
		return c.ReadChunkSize
	}
	return defaultReadChunkSize
}

// resolvedPrefetch returns ReadPrefetch if set, else the default.
func (c CFSConfig) resolvedPrefetch() int {
	if c.ReadPrefetch > 0 {
		return c.ReadPrefetch
	}
	return defaultReadPrefetch
}
