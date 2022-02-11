package fuse

import (
	"bytes"
	"fmt"
	"os/exec"
)

func unmount(dir string) (err error) {
	// Call fusermount.
	cmd := exec.Command("fusermount", "-u", dir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if len(output) > 0 {
			output = bytes.TrimRight(output, "\n")
			err = fmt.Errorf("%v: %s", err, output)
		}

		return
	}

	return
}
