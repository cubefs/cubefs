package fileutil

import (
	"io/fs"
	"os"
)

// os.WriteFile() with fsync implementation
func WriteFileWithFsync(filename string, data []byte, perm fs.FileMode) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}
