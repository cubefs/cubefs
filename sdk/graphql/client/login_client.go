package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strings"
)

type MasterGClient struct {
	addrs  []string //first is leader
	active int
}

func NewMasterGClient(addrs []string) *MasterGClient {
	return &MasterGClient{addrs: addrs}
}

type UserToken struct {
	UserID string
	Token  string
	Status bool
}

func (c *MasterGClient) ValidatePassword(ctx context.Context, userID string, password string) (*proto.UserInfo, error) {
	req := &Request{
		header: http.Header{},
		Query: `
			query($userID: String!, $password: String!){
			  validatePassword(userID: $userID, password: $password){
					user_id
					access_key
					secret_key
			  }
			}
		`,
	}
	req.header.Set(proto.UserKey, userID)
	req.header.Set("Content-Type", "application/json; charset=utf-8")
	req.header.Set("Accept", "application/json; charset=utf-8")

	req.Var("userID", userID)
	req.Var("password", password)

	rep, err := c.Query(ctx, proto.AdminUserAPI, req)
	if err != nil {
		return nil, err
	}

	userInfo := &proto.UserInfo{}

	if err := rep.GetValueByType(userInfo, "validatePassword"); err != nil {
		return nil, err
	}

	return userInfo, nil

}

//-------------------client use ------------------//

type Result map[string]interface{}

func (r Result) GetValue(path ...string) interface{} {
	var t interface{}
	m := r
	l := len(path) - 1
	for i, p := range path {
		t = m[p]
		if i == l {
			return t
		}
		if t != nil {
			m = t.(map[string]interface{})
		} else {
			return nil
		}
	}
	return nil
}

func (r Result) GetValueByType(v interface{}, path ...string) error {
	var t interface{}
	m := r
	l := len(path) - 1
	for i, p := range path {
		t = m[p]
		if i == l {
			bs, err := json.Marshal(t)
			if err != nil {
				return err
			}
			return json.Unmarshal(bs, v)
		}
		if t != nil {
			m = t.(map[string]interface{})
		} else {
			return fmt.Errorf("not found value by path:[%v]", path)
		}
	}

	panic("get value by type has out of path range")
}

func (r Result) String() string {
	if v, e := json.Marshal(r); e != nil {
		return e.Error()
	} else {
		return string(v)
	}

}

type Request struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables"`
	header    http.Header            `json:"-"`
}

func (r *Request) Hander(key string, value string) {
	if r.header == nil {
		r.header = http.Header{}
	}
	r.header.Set(key, value)
}

func (r *Request) Var(key string, value interface{}) {
	if r.Variables == nil {
		r.Variables = make(map[string]interface{})
	}
	r.Variables[key] = value
}

func NewRequest(ctx context.Context, query string) *Request {
	req := &Request{
		header: http.Header{},
		Query:  query,
	}

	if userKey := ctx.Value(proto.UserKey); userKey != nil {
		req.header.Set(proto.UserKey, userKey.(string))
	}

	if token := ctx.Value(proto.HeadAuthorized); token != nil {
		req.header.Set(proto.HeadAuthorized, token.(string))
	}

	req.header.Set("Content-Type", "application/json; charset=utf-8")
	req.header.Set("Accept", "application/json; charset=utf-8")
	return req
}

type Response struct {
	Code   int      `json:"code"`
	Data   *Result  `json:"data"`
	Errors []string `json:"errors"`
}

func (c *MasterGClient) Query(ctx context.Context, model string, req *Request) (*Result, error) {
	var err error

	for i := c.active; i < len(c.addrs); i++ {
		var rep *Response
		if rep, err = doPost(ctx, "http://"+c.addrs[i%len(c.addrs)]+model, req); err != nil {
			log.LogErrorf("execute by master clients has err:%s", err.Error())
			c.active = (i + 1) % len(c.addrs)
			continue
		} else {
			if len(rep.Errors) > 0 {
				return nil, errors.New(strings.Join(rep.Errors, " ; "))
			} else {
				return rep.Data, nil
			}
		}
	}
	return nil, errors.New("all masters cannot access")
}

func doPost(ctx context.Context, url string, qr *Request) (*Response, error) {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qr); err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, &body)
	if err != nil {
		return nil, err
	}

	req.Header = qr.header
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	rep := &Response{
		Code: resp.StatusCode,
	}
	if err := json.NewDecoder(resp.Body).Decode(rep); err != nil {
		return nil, err
	}

	return rep, nil
}

func (c *MasterGClient) Proxy(ctx context.Context, r *http.Request, header http.Header) (*Response, error) {
	var err error

	req := &Request{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		return nil, err
	}
	req.header = header

	for i := c.active; i < len(c.addrs); i++ {
		var rep *Response
		if rep, err = doPost(ctx, "http://"+c.addrs[i%len(c.addrs)]+r.RequestURI, req); err != nil {
			log.LogErrorf("execute by master clients has err:%s", err.Error())
			c.active = (i + 1) % len(c.addrs)
			continue
		} else {
			if len(rep.Errors) > 0 {
				return nil, errors.New(strings.Join(rep.Errors, " ; "))
			} else {
				return rep, nil
			}
		}
	}
	return nil, errors.New("all masters cannot access")
}
