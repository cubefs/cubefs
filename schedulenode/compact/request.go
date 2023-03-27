package compact

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"io/ioutil"
	"net/http"
)

func doGet(reqURL string) (reply *proto.QueryHTTPResult, err error) {
	resp, err := http.Get(reqURL)
	if err != nil {
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = errors.NewErrorf("compact request: failed, response status code(%v) is not ok, url(%v)", resp.StatusCode, reqURL)
		return
	}
	reply = &proto.QueryHTTPResult{}
	if err = json.Unmarshal(body, reply); err != nil {
		return
	}
	return
}

func genStopCompactUrl(ipPort, cluster, vol string) string {
	url := fmt.Sprintf("http://%v%v?%v=%v&%v=%v",
		ipPort, CmpStop, CLIClusterKey, cluster, CLIVolNameKey, vol)
	return url
}
