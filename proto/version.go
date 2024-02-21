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
	return fmt.Sprintf("CubeFS %s\n"+
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
	Role    string
	Version string
	Branch  string
	Commit  string
	Build   string
}

func (v VersionInfo) ToMap() map[string]string {
	return map[string]string{
		"role":    v.Role,
		"version": v.Version,
		"branch":  v.Branch,
		"commit":  v.Commit,
		"build":   v.Build,
	}
}

func GetVersion(role string) VersionInfo {
	return VersionInfo{
		Role:    role,
		Version: Version,
		Branch:  BranchName,
		Commit:  CommitID,
		Build:   fmt.Sprintf("%s %s %s %s", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime),
	}
}
