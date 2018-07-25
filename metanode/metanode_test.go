package metanode

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
)

func TestValidNodeID(t *testing.T) {
	logDir := "testlog"
	defer os.RemoveAll(logDir)
	_, err := log.NewLog(logDir, "MetaNode", log.DebugLevel)
	if err != nil {
		t.Fatalf("util/log module test failed: %s", err.Error())
	}
	count := 0
	httpServe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,
		r *http.Request) {
		count++
		if count < 3 {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if count == 3 {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte("127.0.0.1:12345"))
			return
		}
		w.Write([]byte("55555"))
		return
	}))
	defer httpServe.Close()
	masterAddr := httpServe.Listener.Addr().String()
	m := NewServer()
	masterAddrs = []string{"127.0.0.1:10234", "127.0.0.1:22666", masterAddr}
	if err = m.register(); err != nil {
		t.Fatalf("register: %s failed!", err.Error())
	}
	if m.nodeId != 55555 {
		t.Fatalf("valideNodeID: want nodeID=5555, have nodeID=%d failed!",
			m.nodeId)
	}
	t.Logf("valideNodeID success!")
}

func Test_parseConfig(t *testing.T) {
	var mConfig *config.Config
	m := NewServer()
	err := m.parseConfig(mConfig)
	if err == nil {
		t.Fatalf("parseConfig: failed!")
	}
	if err.Error() != "invalid configuration" {
		t.Fatalf("parseConfig: %s failed!", err.Error())
	}

	confStr := "{}"
	mConfig = config.LoadConfigString(confStr)
	err = m.parseConfig(mConfig)
	if err == nil {
		t.Fatalf("parseConfig listen failed!")
	}
	if !strings.Contains(err.Error(), "listen port: ") {
		t.Logf("parseConfig listen failed!")
	}
	confStr = `{"listen":10}`
	masterAddrs = nil
	mConfig = config.LoadConfigString(confStr)
	err = m.parseConfig(mConfig)
	if err == nil {
		t.Fatalf("parseConfig failed!")
	}
	if err != nil {
		if err.Error() != "master Addrs is empty!" {
			t.Fatalf("parseConfig failed!")
		}
	}
	confStr = `{"listen": 11111, "masterAddrs":["1.1.1.1:11111"]}`
	mConfig = config.LoadConfigString(confStr)
	if err = m.parseConfig(mConfig); err != nil {
		t.Fatalf("parseConfig masterAddrs failed!")
	}

}
