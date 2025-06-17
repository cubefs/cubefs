package transport

import (
	"testing"
)

func TestConfig(t *testing.T) {
	VerifyConfig(DefaultConfig())

	config := DefaultConfig()
	config.Version = 0xff
	err := VerifyConfig(config)
	t.Log(err)
	if err == nil {
		t.Fatal(err)
	}

	config = DefaultConfig()
	config.KeepAliveInterval = 0
	err = VerifyConfig(config)
	t.Log(err)
	if err == nil {
		t.Fatal(err)
	}

	config = DefaultConfig()
	config.KeepAliveInterval = 10
	config.KeepAliveTimeout = 5
	err = VerifyConfig(config)
	t.Log(err)
	if err == nil {
		t.Fatal(err)
	}

	config = DefaultConfig()
	config.MaxFrameSize = 0
	err = VerifyConfig(config)
	t.Log(err)
	if err == nil {
		t.Fatal(err)
	}

	config = DefaultConfig()
	config.MaxFrameSize = 1 << 24
	err = VerifyConfig(config)
	t.Log(err)
	if err == nil {
		t.Fatal(err)
	}

	config = DefaultConfig()
	config.MaxReceiveBuffer = 0
	err = VerifyConfig(config)
	t.Log(err)
	if err == nil {
		t.Fatal(err)
	}

	config = DefaultConfig()
	config.MaxStreamBuffer = 0
	err = VerifyConfig(config)
	t.Log(err)
	if err == nil {
		t.Fatal(err)
	}

	config = DefaultConfig()
	config.MaxStreamBuffer = 100
	config.MaxReceiveBuffer = 99
	err = VerifyConfig(config)
	t.Log(err)
	if err == nil {
		t.Fatal(err)
	}

	config = DefaultConfig()
	config.MaxStreamBuffer = 1 << 33
	config.MaxReceiveBuffer = 1 << 34
	err = VerifyConfig(config)
	t.Log(err)
	if err == nil {
		t.Fatal(err)
	}

	if _, err := Server(nil, config); err == nil {
		t.Fatal("server started with wrong config")
	}
	if _, err := Client(nil, config); err == nil {
		t.Fatal("client started with wrong config")
	}
}
