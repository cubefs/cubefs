package bcache

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/config"
	"os"
	"strings"
	"testing"
)

var server = NewServer()
var client = NewBcacheClient()

func setup() {
	fmt.Println("Before all tests")
	//os.Remove("/var/lib/adls/bcache.socket")

}

func teardown() {
	fmt.Println("After all tests")
	//os.Remove("/var/lib/adls/bcache.socket")
	server.Shutdown()
}
func TestRW(t *testing.T) {
	jsonConfig := `{
     "cacheDir":"/tmp/block1:1024000"}`
	cfg := config.LoadConfigString(jsonConfig)
	err := server.Start(cfg)
	if err != nil {
		t.Fatalf("start server failed %v", err)
	}

	input := "123456"
	err = client.Put("test_1_00000", []byte(input))
	if err != nil {
		t.Fatalf("put failed %v", err)
	}
	output := make([]byte, 6)
	n, err := client.Get("test_1_00000", output, 0, 6)
	if err != nil {
		t.Fatalf("get failed %v", err)
	}
	if n != len(input) {
		t.Fatalf("get wrong size %v,expected %v", n, len(input))
	}

	if strings.EqualFold(input, string(output)) == false {
		t.Fatalf("get wrong value %v,expected %v", string(output), input)
	}

	client.Evict("test_1_00000")
}
func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}
