package iputil

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net"
	"strings"
	"testing"
)

func getLocalIp() (localIp string, err error){
	nets, err := net.Interfaces()
	if err != nil {
		return
	}
	for _, iter := range nets {
		var addrIp []net.Addr
		addrIp, err = iter.Addrs()
		if err != nil {
			continue
		}
		if strings.Contains(iter.Name, "bo") ||  strings.Contains(iter.Name, "eth")  || strings.Contains(iter.Name, "enp") {
			for _, addr := range addrIp {
				if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
					localIp = strings.Split(addr.String(), "/")[0]
					err = nil
					return
				}
			}
		}
	}
	return
}

func TestVerifyLocalIP(t *testing.T) {
	localIp, err := getLocalIp()
	fmt.Printf("local ip: %v\n", localIp)
	assert.Equal(t, nil, err)
	err = VerifyLocalIP(localIp)
	assert.Equal(t, nil, err)
}