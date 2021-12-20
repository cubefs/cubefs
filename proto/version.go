package proto

import (
	"fmt"
	"runtime"
)

const (
	BaseVersion = "2.5.2"
)

var (
	Version    = BaseVersion
	CommitID   string
	BranchName string
	BuildTime  string
)

type VersionInfo struct {
	ClientId         string
	Version          string
	FilesRead        int64
	ReadByte         int64
	TotalReadTime    int64
	ErrorRead        int64
	FinalReadError   int64
	FilesWrite       int64
	WriteByte        int64
	TotalWriteTime   int64
	ErrorWrite       int64
	FinalWriteError  int64
	TotalConnections int64
	ZkAddr           string `json:"zkAddr"`
}

func DumpVersion(role, branchName, commitID, buildTime string) string {
	CommitID = commitID
	BranchName = branchName
	BuildTime = buildTime

	return fmt.Sprintf("ChubaoFS %s\nBranch: %s\nVersion: %s\nCommit: %s\nBuild: %s %s %s %s\n", role, branchName, BaseVersion, commitID, runtime.Version(), runtime.GOOS, runtime.GOARCH, buildTime)
}

type VersionValue struct {
	Model      string
	Version    string
	CommitID   string
	BranchName string
	BuildTime  string
}

func MakeVersion(model string) VersionValue {
	return VersionValue{
		Model:      model,
		Version:    Version,
		CommitID:   CommitID,
		BranchName: BranchName,
		BuildTime:  fmt.Sprintf("%s %s %s %s", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime),
	}
}
