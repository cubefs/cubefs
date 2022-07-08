// Package fstab parses and serializes linux filesystem mounts information
package fstab

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type Mounts []*Mount

// String serializes a list of mounts to the fstab format
func (mounts Mounts) String() (output string) {
	for i, mount := range mounts {
		if i > 0 {
			output += "\n"
		}
		output += mount.String()
	}

	return
}

// PaddedString serializes a list of mounts to the fstab format with padding.
func (mounts Mounts) PaddedString(paddings ...int) (output string) {
	for i, mount := range mounts {
		if i > 0 {
			output += "\n"
		}
		output += mount.PaddedString(paddings...)
	}

	return
}

// ParseSystem parses your system fstab ("/etc/fstab")
func ParseSystem() (mounts Mounts, err error) {
	return ParseFile("/etc/fstab")
}

// ParseProc parses procfs information
func ParseProc() (mounts Mounts, err error) {
	return ParseFile("/proc/mounts")
}

// ParseFile parses the given file
func ParseFile(filename string) (mounts Mounts, err error) {
	file, err := os.Open(filename)
	if nil != err {
		return nil, err
	} else {
		defer file.Close()
		return Parse(file)
	}
}

func Parse(source io.Reader) (mounts Mounts, err error) {
	mounts = make([]*Mount, 0, 10)

	scanner := bufio.NewScanner(source)
	lineNo := 0

	for scanner.Scan() {
		lineNo++
		mount, err := ParseLine(scanner.Text())
		if nil != err {
			return nil, fmt.Errorf("Syntax error at line %d: %s", lineNo, err)
		}

		if nil != mount {
			mounts = append(mounts, mount)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return mounts, nil
}
