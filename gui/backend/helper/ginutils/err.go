package ginutils

import "fmt"

func ClusterMasterAddrErr(clusterName string, err error) error {
	return fmt.Errorf("get cluster(%s) master addr error:%+v", clusterName, err)
}

func ClusterConsulAddrErr(name string, err error) error  {
	return fmt.Errorf("get cluster(%s) consul addr error:%+v", name, err)
}