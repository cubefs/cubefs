// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"unsafe"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse/fs"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
)

var (
	optFuseHttpPort = flag.String("p", "", "FUSE client http server port")
	optDynamicUDS   = flag.Bool("n", false, "use dynamic UDS file name")
	optSuspend      = flag.Bool("s", false, "suspend fuse")
	optResume       = flag.Bool("r", false, "resume fuse")
	optDump         = flag.String("d", "", "dump nodes/handles list files, <nodes list>,<handles list>")
	optVersion      = flag.Bool("v", false, "show version")
)

const (
	DefaultUDS string = "/tmp/CubeFS-fdstore.sock"
	DefaultIP  string = "127.0.0.1"
)

func createUDS() (listener net.Listener, err error) {
	var sockAddr string
	var addr *net.UnixAddr

	if *optDynamicUDS {
		sockAddr = fmt.Sprintf("/tmp/CubeFS-fdstore-%v.sock", os.Getpid())
	} else {
		sockAddr = DefaultUDS
	}
	fmt.Printf("sockaddr: %s\n", sockAddr)

	if addr, err = net.ResolveUnixAddr("unix", sockAddr); err != nil {
		fmt.Printf("cannot resolve unix addr: %v\n", err)
		return
	}

	if listener, err = net.ListenUnix("unix", addr); err != nil {
		fmt.Printf("cannot create unix domain: %v\n", err)
		return
	}

	if err = os.Chmod(sockAddr, 0666); err != nil {
		fmt.Printf("failed to chmod socket file: %v\n", err)
		listener.Close()
		return
	}

	return
}

func destroyUDS(listener net.Listener) {
	sockAddr := listener.Addr().String()
	listener.Close()
	os.Remove(sockAddr)
}

func RecvFuseFdFromOldClient(udsListener net.Listener) (file *os.File, err error) {
	var conn net.Conn
	var socket *os.File

	if conn, err = udsListener.Accept(); err != nil {
		fmt.Fprintf(os.Stderr, "unix domain accepts fail: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Printf("a new connection accepted\n")
	unixconn := conn.(*net.UnixConn)
	if socket, err = unixconn.File(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to get socket file: %v\n", err)
		return
	}
	defer socket.Close()

	if file, err = util.RecvFd(socket); err != nil {
		fmt.Fprintf(os.Stderr, "failed to receive fd: %v\n", err)
		return
	}

	fmt.Printf("Received file %s fd %v\n", file.Name(), file.Fd())
	return file, nil
}

func SendFuseFdToNewClient(udsListener net.Listener, file *os.File) (err error) {
	var conn net.Conn
	var socket *os.File

	if conn, err = udsListener.Accept(); err != nil {
		fmt.Fprintf(os.Stderr, "unix domain accepts fail: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Printf("a new connection accepted\n")
	unixconn := conn.(*net.UnixConn)
	if socket, err = unixconn.File(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to get socket file: %v\n", err)
		return
	}
	defer socket.Close()

	if file == nil {
		err = fmt.Errorf("no file is received")
		fmt.Fprintf(os.Stderr, err.Error())
		return
	} else {
		if err = util.SendFd(socket, file.Name(), file.Fd()); err != nil {
			fmt.Fprintf(os.Stderr, "failed to send fd %v: %v\n", file.Fd(), err)
			return
		}
	}

	fmt.Printf("Sent file %s fd %v\n", file.Name(), file.Fd())
	return nil
}

func SendSuspendRequest(port string, udsListener net.Listener) (err error) {
	var (
		req  *http.Request
		resp *http.Response
		data []byte
	)
	udsFilePath := udsListener.Addr().String()

	url := fmt.Sprintf("http://%s:%s/suspend?sock=%s", DefaultIP, port, udsFilePath)
	if req, err = http.NewRequest("POST", url, nil); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get new request: %v\n", err)
		return err
	}
	req.Header.Set("Content-Type", "application/text")

	client := http.DefaultClient
	if resp, err = client.Do(req); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to post request: %v\n", err)
		return err
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read response: %v\n", err)
		return err
	}

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("\n==> %s\n==> Could restore cfs-client now with -r option.\n\n", string(data))
	} else {
		fmt.Printf("\n==> %s\n==> Status: %s\n\n", string(data), resp.Status)
		return fmt.Errorf(resp.Status)
	}

	return nil
}

func WaitSuspendFinish(ch chan error) error {
	err := <-ch
	return err
}

func doSuspend(port string) error {
	var fud *os.File

	udsListener, err := createUDS()
	if err != nil {
		fmt.Fprintf(os.Stderr, "doSuspend: failed to create UDS: %v\n", err)
		return err
	}
	defer destroyUDS(udsListener)

	if err = SendSuspendRequest(port, udsListener); err != nil {
		//SendResumeRequest(port)
		return err
	}

	if fud, err = RecvFuseFdFromOldClient(udsListener); err != nil {
		//SendResumeRequest(port)
		return err
	}

	if err = SendFuseFdToNewClient(udsListener, fud); err != nil {
		//SendResumeRequest(port)
		return err
	}

	return nil
}

func SendResumeRequest(port string) (err error) {
	var (
		req  *http.Request
		resp *http.Response
		data []byte
	)

	url := fmt.Sprintf("http://%s:%s/resume", DefaultIP, port)
	if req, err = http.NewRequest("POST", url, nil); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get new request: %v\n", err)
		return err
	}
	req.Header.Set("Content-Type", "application/text")

	client := http.DefaultClient
	if resp, err = client.Do(req); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to post request: %v\n", err)
		return err
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read response: %v\n", err)
		return err
	}

	fmt.Printf("data: %s\n", string(data))
	return nil
}

func doResume(port string) error {
	err := SendResumeRequest(port)
	return err
}

func doDump(filePathes string) {
	pathes := strings.Split(filePathes, ",")
	if len(pathes) != 2 {
		fmt.Fprintf(os.Stderr, "Invalid dump parameter '%s'\n", filePathes)
		return
	}

	nodes := make([]*fs.ContextNode, 0)
	handles := make([]*fs.ContextHandle, 0)

	nodeListFile, err := os.OpenFile(pathes[0], os.O_RDONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open nodes list file: %v\n", err)
		return
	}
	defer nodeListFile.Close()

	cnVer, err := fs.ReadVersion(nodeListFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read nodes version: %v\n", err)
		return
	}

	i := 0
	for {
		var (
			data    []byte = make([]byte, unsafe.Sizeof(fs.ContextNode{}))
			rsize   int
			skipped int
		)

		rsize, err = nodeListFile.Read(data)
		if rsize == 0 || err == io.EOF {
			err = nil
			break
		}

		if cnVer == fs.ContextNodeVersionV1 {
			cn := fs.ContextNodeFromBytes(data)
			for uint64(len(nodes)) < cn.NodeID {
				nodes = append(nodes, nil)
				skipped++
				i++
			}
			if skipped > 0 {
				fmt.Printf("[... skipped %d]\n", skipped)
			}

			fmt.Printf("[%d] snode(%s)\n", i, cn)
			nodes = append(nodes, cn)
		}
		i++
	}

	handleListFile, err := os.OpenFile(pathes[1], os.O_RDONLY, 0644)
	if err != nil {
		err = fmt.Errorf("failed to open handles list file: %v\n", err)
		return
	}
	defer handleListFile.Close()

	chVer, err := fs.ReadVersion(handleListFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read handles version: %v\n", err)
		return
	}

	i = 0
	for {
		var (
			data    []byte = make([]byte, unsafe.Sizeof(fs.ContextHandle{}))
			rsize   int
			skipped int
		)
		rsize, err = handleListFile.Read(data)
		if rsize == 0 || err == io.EOF {
			err = nil
			break
		}

		if chVer == fs.ContextHandleVersionV1 {
			ch := fs.ContextHandleFromBytes(data)

			for uint64(len(handles)) < ch.HandleID {
				handles = append(handles, nil)
				skipped++
				i++
			}
			if skipped > 0 {
				fmt.Printf("[... skipped %d]\n", skipped)
			}

			if ch.NodeID > uint64(len(nodes)) {
				fmt.Printf("[%d] shandle(handleid:%v nodeid:%v) [out of nodes range]\n",
					i, ch.HandleID, ch.NodeID)
			} else if nodes[ch.NodeID] == nil {
				fmt.Printf("[%d] shandle(handleid:%v nodeid:%v) [no associated node]\n",
					i, ch.HandleID, ch.NodeID)
			} else {
				fmt.Printf("[%d] shandle(handleid:%v nodeid:%v)\n", i, ch.HandleID, ch.NodeID)
			}

			handles = append(handles, ch)
		}
		i++
	}
}

func main() {
	var err error

	flag.Parse()

	if *optVersion {
		fmt.Printf(proto.DumpVersion("fdstore"))
		os.Exit(0)
	}

	if *optDump == "" {
		if *optFuseHttpPort == "" || (!*optSuspend && !*optResume) {
			flag.Usage()
			os.Exit(-1)
		}
	}

	fmt.Printf("Fuse address: %s:%s\n", DefaultIP, *optFuseHttpPort)

	if *optDump != "" {
		doDump(*optDump)
	} else if *optSuspend {
		fmt.Printf("Do suspend ...\n")
		err = doSuspend(*optFuseHttpPort)
	} else if *optResume {
		fmt.Printf("Do Resume ...\n")
		err = doResume(*optFuseHttpPort)
	}

	if err != nil {
		fmt.Printf("Done FAILED\n")
		os.Exit(-1)
	}

	fmt.Printf("Done Successfully\n")
	return
}
