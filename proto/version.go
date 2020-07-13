package proto

import (
	"fmt"
	"runtime"
)

const (
	BaseVersion="2.1.0"
)


func DumpVersion(role,branchName,commitID,buildTime string) string {
	return fmt.Sprintf("ChubaoFS %s\nBranch: %s\nVersion: %s\nCommit: %s\nBuild: %s %s %s %s\n", role,branchName, BaseVersion,commitID,runtime.Version(), runtime.GOOS, runtime.GOARCH, buildTime)
}