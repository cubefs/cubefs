package rpc

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	urllib "net/url"
	"strings"

	"github.com/chubaofs/blobstore/common/trace"
)

var (
	errNoHost = errors.New("no host available")
)

// LbConfig load balance config
type LbConfig struct {
	// hosts
	Hosts []string `json:"hosts"`
	// backup hosts
	BackupHosts []string `json:"backup_hosts"`
	// RequestTryTimes The maximum number of attempts for a request hosts,
	RequestTryTimes uint32 `json:"try_times"`
	//HostTryTimes Number of host failure retries, HostTryTimes < RequestTryTimes, Avoid requesting the unavailable host all the time
	HostTryTimes int32 `json:"host_try_times"`
	// Failure retry interval
	FailRetryIntervalS int64 `json:"fail_retry_interval_s"`

	// should retry function
	ShouldRetry func(code int, err error) bool `json:"-"`

	// config for simple client
	Config
}

type lbClient struct {
	requestTryTimes uint32
	// host for simple client
	clientMap map[string]Client

	sel Selector
	cfg *LbConfig
}

var _ Client = (*lbClient)(nil)

// NewLbClient returns a lb client
func NewLbClient(cfg *LbConfig, sel Selector) Client {
	if cfg.RequestTryTimes == 0 {
		cfg.RequestTryTimes = uint32(len(cfg.Hosts) + len(cfg.BackupHosts))
	}
	if cfg.HostTryTimes == 0 {
		cfg.HostTryTimes = int32(cfg.RequestTryTimes)
	}
	if cfg.ShouldRetry == nil {
		cfg.ShouldRetry = defaultShouldRetry
	}
	if cfg.HostTryTimes > int32(cfg.RequestTryTimes) {
		cfg.HostTryTimes = int32(cfg.RequestTryTimes - 1)
	}
	if cfg.FailRetryIntervalS < 5 {
		cfg.FailRetryIntervalS = 60
	}
	if sel == nil {
		sel = NewSelector(cfg)
	}
	cl := &lbClient{sel: sel, cfg: cfg}
	cl.clientMap = make(map[string]Client)
	for _, host := range cfg.Hosts {
		cl.clientMap[host] = NewClient(&cfg.Config)
	}
	for _, host := range cfg.BackupHosts {
		cl.clientMap[host] = NewClient(&cfg.Config)
	}

	cl.requestTryTimes = cfg.RequestTryTimes
	return cl
}

var defaultShouldRetry = func(code int, err error) bool {
	// use proxy will return 50x，need retry target，don't change proxy（suppose proxy always work）
	if code == 502 || code == 504 {
		return true // server error
	}
	if err == nil {
		return false // ok
	}
	return true
}

func (c *lbClient) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	resp, err := c.doCtx(ctx, req)
	if err != nil {
		return resp, err
	}
	return resp, err
}

func (c *lbClient) Form(ctx context.Context, method, url string, form map[string][]string) (resp *http.Response, err error) {
	body := urllib.Values(form).Encode()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *lbClient) Put(ctx context.Context, url string, params interface{}) (resp *http.Response, err error) {
	data, ct, err := marshalObj(params)
	if err != nil {
		return
	}
	request, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return
	}
	request.Header.Set(HeaderContentType, ct)
	return c.Do(ctx, request)
}

func (c *lbClient) Post(ctx context.Context, url string, params interface{}) (resp *http.Response, err error) {
	data, ct, err := marshalObj(params)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	request.Header.Set(HeaderContentType, ct)
	return c.Do(ctx, request)
}

func (c *lbClient) DoWith(ctx context.Context, req *http.Request, ret interface{}, opts ...Option) error {
	for _, opt := range opts {
		opt(req)
	}
	resp, err := c.Do(ctx, req)
	if err != nil {
		return err
	}
	err = serverCrcEncodeCheck(ctx, req, resp)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *lbClient) GetWith(ctx context.Context, url string, ret interface{}) error {
	resp, err := c.Get(ctx, url)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *lbClient) PutWith(ctx context.Context, url string, ret interface{}, params interface{}, opts ...Option) (err error) {
	data, ct, err := marshalObj(params)
	if err != nil {
		return
	}
	request, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return
	}
	request.Header.Set(HeaderContentType, ct)
	for _, opt := range opts {
		opt(request)
	}
	resp, err := c.Do(ctx, request)
	if err != nil {
		return
	}
	err = serverCrcEncodeCheck(ctx, request, resp)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *lbClient) PostWith(ctx context.Context, url string, ret interface{}, params interface{}, opts ...Option) error {
	data, ct, err := marshalObj(params)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set(HeaderContentType, ct)

	for _, opt := range opts {
		opt(request)
	}
	resp, err := c.Do(ctx, request)
	if err != nil {
		return err
	}

	// set Header and log errors
	err = serverCrcEncodeCheck(ctx, request, resp)
	if err != nil {
		return err
	}
	return ParseData(resp, ret)
}

func (c *lbClient) Head(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *lbClient) Get(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *lbClient) Delete(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return
	}
	return c.Do(ctx, req)
}

func (c *lbClient) doCtx(ctx context.Context, r *http.Request) (resp *http.Response, err error) {
	reqURI := r.URL.RequestURI()
	span := trace.SpanFromContextSafe(ctx)

	// get the host
	host := c.sel.Get()
	if host == "" {
		err = errNoHost
		span.Error("lb.doCtx: get host", err)
		return
	}
	rHost := host
	span.Debug("lb.doCtx: start", rHost, reqURI)
	tryTimes := c.requestTryTimes
	for i := uint32(0); i < tryTimes; i++ {
		// get the real url
		r.URL, err = urllib.Parse(rHost + reqURI)
		if err != nil {
			span.Errorf("lb.doCtx: parse %s error", rHost+reqURI)
			return
		}
		r.Host = r.URL.Host
		resp, err = c.clientMap[host].Do(ctx, r)
		if i == tryTimes-1 {
			span.Warn("lb.doCtx: no more try", host, i)
			return
		}
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		if c.cfg.ShouldRetry(code, err) {
			span.Info("lb.doCtx: retry host, times:", i, "code:", code, "err:", err, "host:", r.URL.String())
			c.sel.SetFail(host)
			// get the host and simple RpcClient again
			host = c.sel.Get()
			if host != "" {
				rHost = host
				continue
			} else { // if host is nil , mean that all the hosts are disabled
				err = errNoHost
				span.Error("lb.doCtx: get retry host", err)
				return
			}
		}
		return
	}
	return
}

func (c *lbClient) Close() {
	c.sel.Close()
}
