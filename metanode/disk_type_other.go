//go:build !linux

package metanode

// Non-Linux platforms: skip NVMe detection to avoid false alarms / build issues.
func isNvmeDisk(dirPath string) (bool, string, error) {
	return true, "", nil
}
