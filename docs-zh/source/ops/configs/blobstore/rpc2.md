# 纠删码 RPC2 配置

::: tip 提示
v3.6.0 版本开始支持该项默认配置
:::

## 配置说明

### TransportConfig

包含 `smux` 的配置

```golang
type TransportConfig struct {
	Version           int           `json:"version"`
	KeepAliveDisabled bool          `json:"keepalive_disabled"`
	KeepAliveInterval util.Duration `json:"keepalive_interval"`
	KeepAliveTimeout  util.Duration `json:"keepalive_timeout"`
	MaxFrameSize      int           `json:"max_frame_size"`
	MaxReceiveBuffer  int           `json:"max_receive_buffer"`
	MaxStreamBuffer   int           `json:"max_stream_buffer"`
}
```

### Server

```golang
type NetworkAddress struct {
	Network string `json:"network"`
	Address string `json:"address"`
}

type Server struct {
	Name      string           `json:"name"`
	Addresses []NetworkAddress `json:"addresses"`

	// Request Header|
	//  No Timeout   |
	//               |  Request Body  |
	//               |  ReadTimeout   |
	//               |  Response Header Body |
	//               |  WriteTimeout         |
	ReadTimeout  util.Duration `json:"read_timeout"`
	WriteTimeout util.Duration `json:"write_timeout"`

	Transport        *TransportConfig `json:"transport,omitempty"`
	BufioReaderSize  int              `json:"bufio_reader_size"`
	ConnectionWriteV bool             `json:"connection_writev"`

	StatDuration util.Duration `json:"stat_duration"`
}
```

### Client

```golang
type ConnectorConfig struct {
	Transport *TransportConfig `json:"transport,omitempty"`

	BufioReaderSize  int  `json:"bufio_reader_size"`
	ConnectionWriteV bool `json:"connection_writev"`

	// tcp or rdma
	Network     string        `json:"network"`
	DialTimeout util.Duration `json:"dial_timeout"`

	MaxSessionPerAddress int `json:"max_session_per_address"`
	MaxStreamPerSession  int `json:"max_stream_per_session"`
}

type Client struct {
	ConnectorConfig ConnectorConfig `json:"connector"`

	Retry   int              `json:"retry"`
	// | Request | Response Header |   Response Body  |
	// |      Request Timeout      | Response Timeout |
	// |                 Timeout                      |
	Timeout         util.Duration `json:"timeout"`
	RequestTimeout  util.Duration `json:"request_timeout"`
	ResponseTimeout util.Duration `json:"response_timeout"`

	Auth auth_proto.Config `json:"auth"`

	LbConfig struct {
		Hosts              []string `json:"hosts"`
		BackupHosts        []string `json:"backup_hosts"`
		HostTryTimes       int      `json:"host_try_times"`
		FailRetryIntervalS int      `json:"fail_retry_interval_s"`
		MaxFailsPeriodS    int      `json:"max_fails_period_s"`
	} `json:"lb"`
}
```
