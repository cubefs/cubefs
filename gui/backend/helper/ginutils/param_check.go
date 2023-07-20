package ginutils

import (
	"errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/enums"
	"github.com/cubefs/cubefs/console/backend/model"
	"github.com/gin-gonic/gin"
)

type Checker interface {
	Check() error
}

func Check(c *gin.Context, v interface{}) bool {
	if v == nil {
		return true
	}
	if err := c.ShouldBind(v); err != nil {
		log.Errorf("parse param error:%+v", err)
		Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return false
	}

	checker, ok := v.(Checker)
	if !ok {
		return true
	}

	if err := checker.Check(); err != nil {
		log.Errorf("param check failed. err:%+v", err)
		Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return false
	}

	return true
}

const Cluster = "cluster"

func GetClusterMaster(c *gin.Context) (string, error) {
	name := c.Param(Cluster)
	cluster, err := new(model.Cluster).FindName(name)
	if err != nil {
		err = ClusterMasterAddrErr(name, err)
		log.Errorf("cluster.FindName failed.name:%s,err:%+v", name, err)
		Send(c, codes.DatabaseError.Error(), err.Error(), nil)
		return "", err
	}
	if len(cluster.MasterAddr) == 0 {
		err = ClusterMasterAddrErr(name, errors.New("no master addr"))
		log.Errorf("cluster vol_type error or no master_addr.master_addr:%+v,vol_type:%+v,name:%s", cluster.MasterAddr, cluster.VolType, name)
		Send(c, codes.DatabaseError.Error(), err.Error(), nil)
		return "", err
	}
	return cluster.MasterAddr[0], nil
}

func GetConsulAddr(c *gin.Context) (string, error) {
	name := c.Param(Cluster)
	cluster, err := new(model.Cluster).FindName(name)
	if err != nil {
		err = ClusterConsulAddrErr(name, err)
		log.Errorf("cluster.FindName failed. cluster:%s,err:%+v", name, err)
		Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return "", err
	}
	if len(cluster.ConsulAddr) == 0 || cluster.VolType != enums.VolTypeLowFrequency {
		err = ClusterConsulAddrErr(name, errors.New("no consul addr"))
		log.Errorf("cluster vol_type error or no consulAddr:%+v,vol_type:%+v,name:%s", cluster.ConsulAddr, cluster.VolType, c.Param(Cluster))
		Send(c, codes.DatabaseError.Error(), err.Error(), nil)
		return "", err
	}
	return cluster.ConsulAddr, nil
}

func CheckAndGetConsul(c *gin.Context, v interface{}) (string, error) {
	if !Check(c, v) {
		return "", errors.New("check param failed")
	}
	return GetConsulAddr(c)
}

func CheckAndGetMaster(c *gin.Context, v interface{}) (string, error) {
	if !Check(c, v) {
		return "", errors.New("check param failed")
	}
	return GetClusterMaster(c)
}