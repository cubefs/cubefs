package pool

import (
	"net"
	"sync"
	"time"
)

type ConnTestFunc func(conn *net.TCPConn) bool

type ConnPool struct {
	sync.Mutex
	pools    map[string]Pool
	initCap  int
	maxCap   int
	idleTime time.Duration
	testFunc ConnTestFunc
}

func NewConnPool() (connP *ConnPool) {
	return &ConnPool{pools: make(map[string]Pool), initCap: 3, maxCap: 20, idleTime: time.Second * 10}
}

func NewConnPoolWithPara(initCap, maxCap int, idleTime time.Duration, testFunc ConnTestFunc) (connP *ConnPool) {
	return &ConnPool{
		pools:    make(map[string]Pool),
		initCap:  initCap,
		maxCap:   maxCap,
		idleTime: idleTime,
		testFunc: testFunc,
	}
}

func (connP *ConnPool) Get(targetAddr string) (c *net.TCPConn, err error) {
	var obj interface{}

	factoryFunc := func(addr interface{}) (interface{}, error) {
		var connect *net.TCPConn
		conn, err := net.DialTimeout("tcp", addr.(string), time.Second)
		if err == nil {
			connect, _ = conn.(*net.TCPConn)
			connect.SetKeepAlive(true)
			connect.SetNoDelay(true)
		}

		return connect, err
	}
	closeFunc := func(v interface{}) error { return v.(net.Conn).Close() }

	testFunc := func(item interface{}) bool {
		if connP != nil {
			return connP.testFunc(item.(*net.TCPConn))
		}
		return true
	}

	connP.Lock()
	pool, ok := connP.pools[targetAddr]
	if !ok {
		poolConfig := &PoolConfig{
			InitialCap:  connP.initCap,
			MaxCap:      connP.maxCap,
			Factory:     factoryFunc,
			Close:       closeFunc,
			Test:        testFunc,
			IdleTimeout: connP.idleTime,
			Para:        targetAddr,
		}
		pool, err = NewChannelPool(poolConfig)
		if err != nil {
			connP.Unlock()
			conn, err := factoryFunc(targetAddr)
			return conn.(*net.TCPConn), err
		}
		connP.pools[targetAddr] = pool
	}
	connP.Unlock()

	if obj, err = pool.Get(); err != nil {
		conn, err := factoryFunc(targetAddr)
		return conn.(*net.TCPConn), err
	}
	c = obj.(*net.TCPConn)
	return
}

func (connP *ConnPool) Put(c *net.TCPConn, forceClose bool) {
	if c == nil {
		return
	}
	if forceClose {
		c.Close()
		return
	}
	addr := c.RemoteAddr().String()
	connP.Lock()
	pool, ok := connP.pools[addr]
	connP.Unlock()
	if !ok {
		c.Close()
		return
	}
	pool.Put(c)
	return
}

func (connP *ConnPool) checkConn(conn *net.TCPConn) error {
	//return proto.NewPingPacket().WriteToConn(conn)
	return nil
}
