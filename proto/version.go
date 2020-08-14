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
