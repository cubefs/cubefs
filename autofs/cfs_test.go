package main

import (
	"fmt"
	"log"
	"strings"
	"testing"
)

func TestCfsOption_Parse(t *testing.T) {
	c1 := cfsParse("tmp/mp", "subdir=sc1ch,volName=m1-project,owner=11111111,accessKey=ya0VE1xxxyyy===gY431,secretKey=5Clssf7831eXSmxxxyyyDQl2Is6J2x,masterAddr=10.0.0.1:22222,enablePosixACL")

	if c1 == nil {
		t.Error("parsed nil")
	}

	if c1.MountPoint != "tmp/mp" || c1.Subdir != "sc1ch" || c1.AccessKey != "ya0VE1xxxyyy===gY431" {
		t.Error("parsedErr, c1: ", fmt.Sprintf("%+v", c1))
	}

	log.Println(fmt.Sprintf("option: %+v", c1))
}

func TestCfsOption_ConvertToCliOptions(t *testing.T) {
	c1 := cfsParse("tmp/mp", "subdir=sc1ch,volName=m1-project,owner=11111111,accessKey=ya0VE1xxxyyygY431,secretKey=5Clssf7831eXSmxxxyyyDQl2Is6J2x,masterAddr=10.0.0.1:22222,enablePosixACL")

	s := c1.ConvertToCliOptions()

	if strings.Join(s, " ") != "-mountPoint tmp/mp -subdir sc1ch -volName m1-project -owner 11111111 -accessKey ya0VE1xxxyyygY431 -secretKey 5Clssf7831eXSmxxxyyyDQl2Is6J2x -masterAddr 10.0.0.1:22222 -logDir -enablePosixACL -logLevel" {
		t.Error("convertErr, s: ", s)
	}
}

func TestIsMounted(t *testing.T) {
	ism := cfsIsMounted("/sys/fs/cgroup/memory")
	ism2 := cfsIsMounted("/sys/fs/cgroup/memory2")

	fmt.Println("ism: ", ism)
	fmt.Println("ism2: ", ism2)
}

func TestUmount(t *testing.T) {
	mp1 := cfsUmount("/home/tmp")
	mp2 := cfsUmount("/home/tmp2")

	fmt.Println("mp1: ", mp1)
	fmt.Println("mp2: ", mp2)
}

func TestCfsMount(t *testing.T) {

	var err error

	mountPoint := "/home/tmp"
	options := "subdir=sc1ch,volName=m1-project,owner=11111111,accessKey=ya0VE1xxxyyygY431,secretKey=5Clssf7831eXSmxxxyyyDQl2Is6J2x,masterAddr=10.0.0.1:22222,enablePosixACL"
	err = cfsMount(mountPoint, options)

	fmt.Println("err: ", err)

}
