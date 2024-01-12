package proto

import (
	"fmt"
	"runtime"
)

//TODO: remove this later.
//go:generate golangci-lint run --issues-exit-code=1 -D errcheck -E bodyclose .

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
