package master

type IReplica interface {
	setAlive()
	isMissing(interval int64) (isMissing bool)
	isLive(timeOutSec int64) (isAvailable bool)
	isActive(timeOutSec int64) bool
	isLocationAvailable() (isAvailable bool)
}

type IPartition interface {
	afterCreation(nodeAddr, diskPath string, c *Cluster) (err error)
	addReplica(replica interface{})
}

