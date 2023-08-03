package taskpool

//go:generate mockgen -destination=./iopool_mock.go -package=taskpool -mock_names IoPool=MockIoPool github.com/cubefs/cubefs/blobstore/util/taskpool IoPool
