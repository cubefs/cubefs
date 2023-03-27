package codecnode

import (
	"context"
	"github.com/cubefs/cubefs/proto"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

const defaultConfigPath = "/tmp/codecnode.json"

func TestNewServer(t *testing.T) {
	if cs := NewServer(); cs == nil {
		t.Fatalf("NewServer fail")
	}
}

func TestCodecServer_Shutdown(t *testing.T) {
	fcs := newFakeCodecServer(t, "9502")
	fcs.wg.Add(1)
	fcs.Shutdown()
}

func TestCodecServer_Sync(t *testing.T) {
	fcs := newFakeCodecServer(t, "9503")
	fcs.Sync()
}

func TestCodecServer_Start(t *testing.T) {
	fcs := newFakeCodecServer(t, "")
	runHttp(t)

	conf := &config.Config{}
	if err := fcs.Start(conf); err == nil {
		t.Fatalf("Start success")
	}
	data := []byte(`
		{
			"role": "codecnode",
			"listen": "9504",
			"masterAddr": [
				"127.0.0.1:8888"
			]
		}
	`)
	conf = fakeCreateConfig(t, data)
	if err := fcs.parseConfig(conf); err != nil {
		t.Fatalf("parseConfig fail err :%v", err)
	}

	if err := fcs.Start(conf); err != nil {
		t.Fatalf("Start fail err :%v", err)
	}
}

func TestCodecServer_parseConfig(t *testing.T) {
	fcs := newFakeCodecServer(t, "9505")
	data := []byte(`
		{
			"role": "codecnode",
			"listen": "9505"
		}
	`)
	conf := fakeCreateConfig(t, data)
	if err := fcs.parseConfig(conf); err == nil {
		t.Fatalf("parseConfig success")
	}

	data = []byte(`
		{
			"role": "codecnode",
			"listen": "9500",
			"masterAddr": [
				"192.168.0.11:17010",
				"192.168.0.12:17010",
				"192.168.0.13:17010"
			]
		}
	`)
	conf = fakeCreateConfig(t, data)
	if err := fcs.parseConfig(conf); err != nil {
		t.Fatalf("parseConfig fail err :%v", err)
	}
}

func TestCodecServer_startTCPService(t *testing.T) {
	fcs := newFakeCodecServer(t, "")

	if err := fcs.startTCPService(); err != nil {
		t.Fatalf("startTCPService fail err :%v", err)
	}

	fcs.port = TcpPort
	if err := fcs.startTCPService(); err == nil {
		t.Fatalf("startTCPService fail")
	}
}

func TestCodecServer_stopTCPService(t *testing.T) {
	fcs := newFakeCodecServer(t, "")

	l, err := net.Listen(NetworkProtocol, "127.0.0.1:9506")
	if err != nil {
		t.Fatalf("Listen fail err :%v", err)
	}
	fcs.tcpListener = l
	if err := fcs.stopTCPService(); err != nil {
		t.Fatalf("stopTCPService fail err :%v", err)
	}
}

func TestCodecServer_serverConn(t *testing.T) {
	fcs := newFakeCodecServer(t, "")
	l, err := net.Listen(NetworkProtocol, "127.0.0.1:9507")
	if err != nil {
		t.Fatalf("Listen fail err :%v", err)
	}
	var wg sync.WaitGroup
	go func(wg *sync.WaitGroup) {
		for {
			conn, err := l.Accept()
			if err != nil {
				t.Fatalf("Accept fail err :%v", err)
			}

			go fcs.serverConn(conn)
		}

	}(&wg)
	if err := fakeSendToHost(context.Background(), "127.0.0.1:9507", 0); err != nil {
		t.Fatalf("fakeSendToHost fail err :%v", err)
	}

	if err := fakeSendToHost(context.Background(), "127.0.0.1:9507", proto.OpIssueMigrationTask); err != nil {
		t.Fatalf("fakeSendToHost fail err :%v", err)
	}

	if err := fakeSendToHost(context.Background(), "127.0.0.1:9507", proto.OpCodecNodeHeartbeat); err != nil {
		t.Fatalf("fakeSendToHost fail err :%v", err)
	}
}

func TestCodecServer_register(t *testing.T) {
	fcs := newFakeCodecServer(t, "")
	runHttp(t)

	conf := &config.Config{}
	MasterClient = masterSDK.NewMasterClient([]string{"127.0.0.1:" + HttpPort}, false)

	fcs.register(conf)
	MasterClient = masterSDK.NewMasterClient([]string{"127.0.0.1:1234"}, false)
	go func() {
		time.Sleep(time.Microsecond * 500)
		fcs.stopC <- false
	}()
	fcs.register(conf)
}

func fakeCreateConfig(t *testing.T, data []byte) (conf *config.Config) {
	_, err := ioutil.ReadFile(defaultConfigPath)
	if os.IsNotExist(err) {
		if err = ioutil.WriteFile(defaultConfigPath, data, 0600); err != nil {
			t.Fatalf("WriteFile fail err :%v", err)
		}
	}
	defer os.Remove(defaultConfigPath)

	conf, err = config.LoadConfigFile(defaultConfigPath)
	if err != nil {
		t.Fatalf("LoadConfigFile fail err :%v", err)
	}
	return
}

func fakeSendToHost(ctx context.Context, host string, opCode uint8) (err error) {
	var conn *net.TCPConn
	if conn, err = gConnPool.GetConnect(host); err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)

	p := proto.NewPacketReqID(ctx)
	p.Opcode = opCode
	err = p.WriteToConn(conn, proto.WriteDeadlineTime)
	return
}
