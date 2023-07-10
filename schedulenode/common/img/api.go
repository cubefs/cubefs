package img

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
)

func GetVols() (icv *ImgClusterView, err error) {
	url := "http://imgmaster.jd.local/client/getview?cluster=ds_image"
	data, err := DoRequest(url)
	if err != nil {
		return
	}
	icv = &ImgClusterView{}
	err = json.Unmarshal(data, icv)
	if err != nil {
		return
	}
	return
}

func GetClusterVolView(host, clusterName string) (icv *ImgClusterView, err error) {
	url := fmt.Sprintf("http://%v/admin/getview?cluster=%v", host, clusterName)
	data, err := DoRequest(url)
	if err != nil {
		return
	}
	icv = &ImgClusterView{}
	err = json.Unmarshal(data, icv)
	if err != nil {
		return
	}
	log.LogInfo(fmt.Sprintf("GetClusterVolView %v volsCount:%v", clusterName, len(icv.Vols)))
	return
}

func GetClusterSpace(host, clusterName string) (space *Space, err error) {
	url := fmt.Sprintf("http://%v/admin/getcluster?cluster=%v", host, clusterName)
	data, err := DoRequest(url)
	if err != nil {
		return
	}
	clusterSpace := &ClusterSpace{}
	err = json.Unmarshal(data, clusterSpace)
	if err != nil {
		return
	}
	space = clusterSpace.Cluster.Space
	log.LogInfo(fmt.Sprintf("GetClusterSpace %v space:%v", clusterName, *space))
	return
}

func DoRequest(reqUrl string) (data []byte, err error) {
	defer func() {
		if err != nil {
			log.LogError(string(data))
		}
	}()
	var resp *http.Response
	if resp, err = http.Get(reqUrl); err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("action[doRequest] reqRUL[%v],statusCode[%v],body[%v]", reqUrl, resp.StatusCode, string(data))
		return
	}
	return
}
