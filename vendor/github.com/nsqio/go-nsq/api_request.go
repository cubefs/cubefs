package nsq

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

type deadlinedConn struct {
	Timeout time.Duration
	net.Conn
}

func (c *deadlinedConn) Read(b []byte) (n int, err error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Read(b)
}

func (c *deadlinedConn) Write(b []byte) (n int, err error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Write(b)
}

type wrappedResp struct {
	Status     string      `json:"status_txt"`
	StatusCode int         `json:"status_code"`
	Data       interface{} `json:"data"`
}

// stores the result in the value pointed to by ret(must be a pointer)
func apiRequestNegotiateV1(httpclient *http.Client, method string, endpoint string, headers http.Header, ret interface{}) error {
	req, err := http.NewRequest(method, endpoint, nil)
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header[k] = v
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := httpclient.Do(req)
	if err != nil {
		return err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("got response %s %q", resp.Status, respBody)
	}

	if len(respBody) == 0 {
		respBody = []byte("{}")
	}

	if resp.Header.Get("X-NSQ-Content-Type") == "nsq; version=1.0" {
		return json.Unmarshal(respBody, ret)
	}

	wResp := &wrappedResp{
		Data: ret,
	}

	if err = json.Unmarshal(respBody, wResp); err != nil {
		return err
	}

	// wResp.StatusCode here is equal to resp.StatusCode, so ignore it
	return nil
}
