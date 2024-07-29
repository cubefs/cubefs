package raft

//go:generate mockgen -destination=group_mock.go -package=raft -mock_names Group=MockGroup github.com/cubefs/cubefs/blobstore/common/raft Group
