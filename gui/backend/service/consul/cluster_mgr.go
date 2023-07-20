package consul

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
)

type Cluster struct {
	Region    string   `json:"region"`
	ClusterId int      `json:"cluster_id"`
	Capacity  int64    `json:"capacity"`
	Available int64    `json:"available"`
	Readonly  bool     `json:"readonly"`
	Nodes     []string `json:"nodes"`
}

func GetRegionClusters(c *gin.Context, consulAddr string) ([]Cluster, error) {
	reqUrl := fmt.Sprintf("%s/v1/kv/ebs/%s/clusters?recurse=true", consulAddr, c.Param("cluster"))
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		log.Errorf("get clusters from consul failed.reqUrl:%s,err:%+v", reqUrl, err)
		return nil, err
	}
	kvs := make([]KV, 0)
	_, err = httputils.HandleResponse(c, resp, err, &kvs)
	if err != nil {
		log.Errorf("handle response failed.err:%+v", err)
		return nil, err
	}
	clusters := make([]Cluster, 0)
	for _, kv := range kvs {
		val, err := base64.StdEncoding.DecodeString(kv.Value)
		if err != nil {
			log.Errorf("decode failed. kv:%+v,err:%+v", kv, err)
			return nil, err
		}
		cu := Cluster{}
		if err = json.Unmarshal(val, &cu); err != nil {
			log.Errorf("unmarshal value failed.data:%s,err:%+v", string(val), err)
			return nil, err
		}
		clusters = append(clusters, cu)
	}
	return clusters, nil
}

func GetRegionClustersMap(c *gin.Context, consulAddr string) (map[int]Cluster, error) {
	clusters, err := GetRegionClusters(c, consulAddr)
	if err != nil {
		return nil, err
	}
	m := map[int]Cluster{}
	for _, cluster := range clusters {
		m[cluster.ClusterId] = cluster
	}
	return m, nil
}
