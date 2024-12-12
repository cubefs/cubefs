package main

import (
	"flag"
	"net"
	"rdma_test/common"
	"rdma_test/rdma"
	"time"
)

var exit = false
var Config = &rdma.RdmaEnvConfig{}

func ReadBytes(conn net.Conn, buf []byte) error {
	offset := 0
	for offset < len(buf) {
		n, err := conn.Read(buf[offset:])
		if n == -1 || (err != nil) {
			println("ReadBytes failed, err: ", err)
			return err
		}
		offset += n
	}
	return nil
}

func init() {
	flag.StringVar(&Config.RdmaPort, "rdma-port", "9000", "rdma-port")
	flag.IntVar(&Config.MemBlockNum, "memory-block-num", 0, "memory-block-num")
	flag.IntVar(&Config.MemBlockSize, "memory-block-size", 0, "memory-block-size")
	flag.IntVar(&Config.MemPoolLevel, "memory-pool-level", 0, "memory-pool-level")
	flag.IntVar(&Config.ConnDataSize, "connect-data-size", 0, "connect-data-size")
	flag.IntVar(&Config.WqDepth, "wq-depth", 0, "wq-depth")
	flag.BoolVar(&Config.EnableRdmaLog, "enable-rdma-log", false, "enable-rdma-log")
	flag.IntVar(&Config.WorkerNum, "worker-num", 0, "worker-num")
	flag.StringVar(&Config.RdmaLogDir, "rdma-log-dir", "", "rdma-log-dir")
}

func testRdma() {
	if err := rdma.InitPool(Config); err != nil {
		println(err.Error())
		return
	}
	server, _ := rdma.NewRdmaServer(common.GParam.Ip, common.GParam.Port)
	defer server.Close()

	for {
		conn, _ := server.Accept()
		go func() {
			for !exit {
				beginTm := time.Now()

				request := common.NewPacket()
				if err := request.ReadFromRDMAConnFromCli(conn, -1); err != nil {
					println(err.Error())
					break
				}

				if request.Opcode == common.OpWrite {
					if err := conn.ReleaseConnRxDataBuffer(request.RdmaBuffer); err != nil {
						println(err.Error())
						break
					}
					request.PacketOkReply()
				}

				if err := request.SendRespToRDMAConn(conn); err != nil {
					println(err.Error())
					break
				}

				common.Stat().AddSumTime(common.GParam.IoSize, time.Now().UnixNano()/1000-beginTm.UnixNano()/1000)
			}
			conn.Close()
		}()
	}
}

func testTcp() {
	common.InitBufferPool(0)
	server, _ := net.Listen("tcp", common.GParam.Ip+":"+common.GParam.Port)
	defer server.Close()

	for {
		c, _ := server.Accept()
		conn, _ := c.(*net.TCPConn)
		conn.SetKeepAlive(true)
		conn.SetNoDelay(true)
		go func() {
			for !exit {
				beginTm := time.Now()

				request := common.NewPacket()
				if err := request.ReadFromTCPConnFromCli(conn, -1); err != nil {
					println(err.Error())
					break
				}

				if request.Opcode == common.OpWrite {
					request.PacketOkReply()
				}

				if err := request.WriteToConn(conn); err != nil {
					println(err.Error())
					break
				}

				common.Stat().AddSumTime(common.GParam.IoSize, time.Now().UnixNano()/1000-beginTm.UnixNano()/1000)
			}
		}()
	}
}

func main() {
	common.ParseParam()
	if common.GParam.Protocol == "rdma" {
		go testRdma()
	} else {
		go testTcp()
	}

	for !exit {
		time.Sleep(time.Millisecond * 1000)
		common.Stat().Print()
	}
}
