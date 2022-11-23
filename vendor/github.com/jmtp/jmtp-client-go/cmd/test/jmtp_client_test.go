package test

import (
	"fmt"
	"github.com/jmtp/jmtp-client-go"
	"github.com/jmtp/jmtp-client-go/jmtpclient"
	"github.com/jmtp/jmtp-client-go/protocol/v1"
	"testing"
	"time"
)

func TestJmtpClient(t *testing.T) {
	config := &jmtpclient.Config{
		Url:           "jmtp://localhost:20560",
		TimeoutSec:    2,
		HeartbeatSec:  10,
		SerializeType: 1,
		ApplicationId: 999,
		InstanceId:    999,
	}
	client, _ := jmtpclient.NewJmtpClient(config, func(packet jmtp_client_go.JmtpPacket, err error) {
		if err != nil {
			t.Error(err)
		} else {
			fmt.Printf("%v", packet)
		}

	})
	err := client.Connect()
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 10; i++ {
		report := &v1.Report{}
		report.PacketId = []byte{0x01}
		report.ReportType = 1
		report.Payload = []byte{byte(i)}
		report.SerializeType = 3
		fmt.Println(report)
		if _, err := client.SendPacket(report); err != nil {
			t.Error(err)
			i--
		}
		time.Sleep(time.Duration(3000) * time.Millisecond)

	}
	client.Close()
}
