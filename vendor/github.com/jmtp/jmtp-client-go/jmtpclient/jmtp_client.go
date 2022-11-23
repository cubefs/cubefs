package jmtpclient

import (
	"bufio"
	"errors"
	"fmt"
	jc "github.com/jmtp/jmtp-client-go"
	"github.com/jmtp/jmtp-client-go/protocol"
	"github.com/jmtp/jmtp-client-go/protocol/v1"
	"log"
	"net"
	urlParser "net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const jmtpProtocol = "jmtp"

var mutex sync.Mutex

type Config struct {
	Url           string // jmtp server 链接地址
	TimeoutSec    int    // 链接超时时间
	HeartbeatSec  int    // 发送心跳间隔（秒）
	SerializeType int    // 序列化协议
	ApplicationId int    // 应用 id TODO: 后续可能会被删除
	InstanceId    int    // 实例 id TODO: 后续可能会被删除
	ChanSize      int    // 初始化队列长度
}

type JmtpClient struct {
	url                 string
	jmtpUrl             *jmtpUrl
	connection          *net.TCPConn
	hawkServer          *net.TCPAddr
	callBack            jc.Callback
	connectSuccess      bool
	packetChain         chan jc.JmtpPacket
	errorChain          chan error
	clientConfig        *Config
	closePingSignal     chan bool
	closeCallbackSignal chan bool
	isClosed            atomic.Value
}

func NewJmtpClient(config *Config, callback jc.Callback) (*JmtpClient, error) {
	urlParser, err := NewUrlParser(config.Url)
	if err != nil {
		return nil, err
	}
	if config.ChanSize <= 0 {
		config.ChanSize = 1000
	}
	return &JmtpClient{
		url:                 config.Url,
		callBack:            callback,
		packetChain:         make(chan jc.JmtpPacket, config.ChanSize),
		errorChain:          make(chan error, config.ChanSize),
		clientConfig:        config,
		closePingSignal:     make(chan bool, 1),
		closeCallbackSignal: make(chan bool, 1),
		jmtpUrl:             urlParser,
	}, nil
}

func (c *JmtpClient) IsClosed() bool {
	return c.isClosed.Load().(bool)
}

func (c *JmtpClient) Reconnect() error {
	c.Close()
	var err error
	for i := 0; i < 6; i++ {
		if err = c.Connect(); err == nil {
			break
		} else {
			log.Printf("reconnect failed, jmtp url: %v, retry: %d times, reason: %v", c.jmtpUrl, i, err)
		}
		if i == 5 {
			if jmtpUrl, err := NewUrlParser(c.url); err == nil {
				log.Printf("reconnect origin url: %v", jmtpUrl)
				c.jmtpUrl = jmtpUrl
				err = c.Connect()
			}
		}
	}
	return err
}

func (c *JmtpClient) Connect() error {
	mutex.Lock()
	defer mutex.Unlock()
	if c.connection == nil {
		if c.url == "" {
			return errors.New(fmt.Sprintf("invalidate jmtp connect url: %s", c.url))
		}
		hawkServer, err := net.ResolveTCPAddr("tcp", c.jmtpUrl.GetHost())
		if err != nil {
			return err
		}
		conn, err := net.DialTCP("tcp", nil, hawkServer)
		if err != nil {
			return err
		}
		if err := conn.SetKeepAlive(true); err != nil {
			return err
		}
		c.hawkServer = hawkServer
		c.connection = conn
		c.isClosed.Store(false)
		if err = c.sendConnectReq(); err != nil {
			return err

		}
		go c.receivePackets()
		go c.chanListener()
		go c.ping()
	}
	return nil
}

func (c *JmtpClient) SetUrl(url string) {
	c.url = url
}

func (c *JmtpClient) Close() error {
	mutex.Lock()
	defer mutex.Unlock()
	if !c.isClosed.Load().(bool) && c.connection != nil {
		c.isClosed.Store(true)
		c.closeCallbackSignal <- true
		c.closePingSignal <- true
		err := c.connection.Close()
		c.connection = nil
		return err
	}
	return nil
}

func (c *JmtpClient) Destroy() error {
	err := c.Close()
	close(c.closeCallbackSignal)
	close(c.closePingSignal)
	close(c.errorChain)
	close(c.packetChain)
	return err
}

func (c *JmtpClient) SendPacket(packet jc.JmtpPacket) (int, error) {
	for i := 0; i < 10; i++ {
		// 如果发现连接被关闭则循环等待，直到连接被重建
		if c.IsClosed() {
			time.Sleep(time.Duration(1) * time.Millisecond)
		} else {
			break
		}
	}
	if c.connection != nil && !c.IsClosed() {
		out, err := protocol.PacketEncoder(packet)
		if err != nil {
			return 0, err
		}
		return c.connection.Write(out)
	} else {
		return 0, errors.New("connection has been closed")
	}
}

func (c *JmtpClient) Reset() error {
	return nil
}

func (c *JmtpClient) receivePackets() {
	reader := bufio.NewReader(c.connection)
	err := protocol.PacketDecoder(reader, c.packetChain, c.errorChain)
	if err != nil {
		if !c.IsClosed() {
			c.callBack(nil, err)
			c.Close()
		}
	}
}

func (c *JmtpClient) sendConnectReq() error {
	option := &jc.ConnectOption{
		HeartbeatSeconds: int16(c.clientConfig.HeartbeatSec),
		SerializeType:    int16(c.clientConfig.SerializeType),
		ApplicationId:    c.clientConfig.ApplicationId,
		InstanceId:       c.clientConfig.InstanceId,
	}
	connectPack := v1.JMTPV1ProtocolDefineInstance.ConnectPacket(option)
	_, err := c.SendPacket(connectPack)
	return err
}

func (c *JmtpClient) ping() {
	ticker := time.NewTicker(
		time.Duration(c.clientConfig.HeartbeatSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_, err := c.SendPacket(v1.JMTPV1ProtocolDefineInstance.PingPacket())
			if err != nil {
				ticker.Stop()
			}
		case <-c.closePingSignal:
			break
		}
	}
}

func (c *JmtpClient) disconnectReq() error {
	disconnect := &v1.Disconnect{
		RedirectUrl: c.jmtpUrl.ToUrlString(),
	}
	_, err := c.SendPacket(disconnect)
	return err
}

func (c *JmtpClient) chanListener() {
	for {
		select {
		case packet := <-c.packetChain:
			switch pack := packet.(type) {
			case *v1.ConnectAck:
				if pack.Code != 0 {
					err := errors.New("connect to server error, connect has been closed")
					c.callBack(pack, err)
					c.Close()
				}
			case *v1.Pong:
				// TODO: check pong response, reconnect connection
			case *v1.ReportAck:
				c.callBack(packet, nil)
			case *v1.Disconnect:
				if "" != pack.RedirectUrl {
					if jmtpUrl, err := NewUrlParser(pack.RedirectUrl); err == nil {
						c.jmtpUrl = jmtpUrl
						c.Reconnect()
					}
				}
			}
		case err := <-c.errorChain:
			c.callBack(nil, err)
		case <-c.closeCallbackSignal:
			break
		}
	}
}

type jmtpUrl struct {
	hostname string
	host     string
	port     int
}

func (j *jmtpUrl) parseUrl(urlString string) error {
	url, err := urlParser.Parse(urlString)
	if err != nil {
		return err
	}
	if strings.ToLower(url.Scheme) != jmtpProtocol {
		return errors.New("invalidate protocol name")
	}
	j.hostname = url.Hostname()
	j.host = url.Host
	j.port, err = strconv.Atoi(url.Port())
	if err != nil {
		return err
	}
	return nil
}

func (j *jmtpUrl) GetHost() string {
	return j.host
}

func (j *jmtpUrl) ToUrlString() string {
	return fmt.Sprintf("%s://%s", jmtpProtocol, j.host)
}

func (j *jmtpUrl) GetPort() int {
	return j.port
}

func (j *jmtpUrl) GetHostname() string {
	return j.hostname
}

func NewUrlParser(urlStr string) (*jmtpUrl, error) {
	jmtpUrl := &jmtpUrl{}
	return jmtpUrl, jmtpUrl.parseUrl(urlStr)
}
