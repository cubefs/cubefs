package main

import (
	"net"
	"rdma_test/common"
	"rdma_test/rdma"
	"time"
)

var exit = false
var Config = &rdma.RdmaEnvConfig{}

func ReadBytes(conn net.Conn, buf []byte) error {
	offset := 0
	for  offset < len(buf) {
		n, err := conn.Read(buf[offset:])
		if n == -1 || (err != nil) {
			println("ReadBytes failed, err: ", err)
			return err
		}
		offset += n
	}
	return nil
}


func testRdma()  {
	if err := rdma.InitPool(Config); err != nil {
		println("init rdma pool failed")
		return
	}

	for i := 0; i < common.GParam.IoDeep; i++ {

		go func() {
			conn := &rdma.Connection{}
			if err := conn.Dial(common.GParam.Ip,common.GParam.Port); err != nil {
				println("client rdma conn dial failed")
				return
			}
			defer conn.Close()
			for !exit {
				beginTm := time.Now()
				p := common.NewWritePacket(common.NormalExtentType, conn)
				p.Size = uint32(common.GParam.IoSize)

				if err := p.WriteToRDMAConn(conn); err != nil {
					println(err.Error())
					break
				}

				reply := common.NewReply(p.ReqID)

				if err := reply.RecvRespFromRDMAConn(conn, 5); err != nil {
					println(err.Error())
					break
				}

				err := conn.ReleaseConnRxDataBuffer(reply.RdmaBuffer)
				if err != nil {
					println(err.Error())
					break
				}
				err = rdma.ReleaseDataBuffer(conn, p.RdmaBuffer, uint32(105 + common.GParam.IoSize))
				if err != nil {
					println(err.Error())
					break
				}

				common.Stat().AddSumTime(common.GParam.IoSize, time.Now().UnixNano() / 1000 - beginTm.UnixNano() / 1000)
			}
		}()
	}
}

func testTcp() {
	common.InitBufferPool(0)

	for i := 0; i < common.GParam.IoDeep; i++ {
		go func() {
			c, err := net.Dial("tcp", common.GParam.Ip + ":" + common.GParam.Port)
			if err != nil {
				println(err.Error())
				return
			}
			conn, _ := c.(*net.TCPConn)
			conn.SetKeepAlive(true)
			conn.SetNoDelay(true)
			defer conn.Close()
			for !exit {
				beginTm := time.Now()
				p := common.NewWritePacket(common.NormalExtentType, conn)
				p.Size = uint32(common.GParam.IoSize)

				if err := p.WriteToConn(conn); err != nil {
					println(err.Error())
					break
				}

				reply := common.NewReply(p.ReqID)
				if err := reply.ReadFromConn(conn, 5); err != nil {
					println(err.Error())
					break
				}

				common.Stat().AddSumTime(common.GParam.IoSize, time.Now().UnixNano() / 1000 - beginTm.UnixNano() / 1000)
			}
		}()
	}
}


func main()  {
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

