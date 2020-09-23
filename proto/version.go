package proto

import (
	"fmt"
	"runtime"
)

var (
	Version    string
	CommitID   string
	BranchName string
	BuildTime  string
)

func DumpVersion(role string) string {
	return fmt.Sprintf("ChubaoFS %s\n"+
		"Version : %s\n"+
		"Branch  : %s\n"+
		"Commit  : %s\n"+
		"Build   : %s %s %s %s\n",
		role,
		Version,
		BranchName,
		CommitID,
		runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime)
}

type VersionInfo struct {
	Model      string
	Version    string
	CommitID   string
	BranchName string
	BuildTime  string
}

func MakeVersion(model string) VersionInfo {
	return VersionInfo{
		Model:      model,
		Version:    Version,
		CommitID:   CommitID,
		BranchName: BranchName,
		BuildTime:  fmt.Sprintf("%s %s %s %s", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime),
	}
}
