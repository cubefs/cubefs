//go:build linux && rdma

package rdma

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
)

// TestLoopback runs a full write→receive→response cycle over localhost rxe.
// Requires a software RoCE device to be configured:
//
//	modprobe rdma_rxe && rdma link add rxe0 type rxe netdev lo
func TestLoopback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping loopback RDMA test in short mode")
	}

	const (
		numSlots = 8
		slotSize = 256 * 1024 // 256 KB
		port     = 18600
	)
	cfg := RDMAConnConfig{
		NumSlots: numSlots,
		SlotSize: slotSize,
	}

	// --- Server goroutine ---
	serverReady := make(chan struct{})
	serverDone := make(chan error, 1)
	receivedPacket := make(chan *proto.Packet, 1)

	go func() {
		ch, err := createEventChannel()
		if err != nil {
			serverDone <- fmt.Errorf("server createEventChannel: %w", err)
			return
		}
		defer destroyEventChannel(ch)

		listenID, err := bindAndListen(ch, port)
		if err != nil {
			serverDone <- fmt.Errorf("server bindAndListen: %w", err)
			return
		}
		defer destroyCMID(listenID)

		close(serverReady) // signal client to connect

		// Accept one connection (ci not needed — peer info already encoded in conn)
		conn, _, err := Accept(listenID, cfg)
		if err != nil {
			serverDone <- fmt.Errorf("server Accept: %w", err)
			return
		}
		defer conn.Close()

		// Poll for request doorbell on slot 0
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var lastSeq uint32
		for {
			seq, ok := conn.PollRecvDoorbell(0, lastSeq)
			if ok {
				lastSeq = seq
				break
			}
			select {
			case <-ctx.Done():
				serverDone <- fmt.Errorf("server: timeout waiting for request")
				return
			default:
			}
		}

		// Deserialize received packet
		p, err := DeserializePacket(conn.RecvSlotBytes(0))
		if err != nil {
			serverDone <- fmt.Errorf("server DeserializePacket: %w", err)
			return
		}
		receivedPacket <- p

		// Send back a simple response (just a SlotHeader + result code)
		respData := make([]byte, SlotHeaderSize+1)
		WriteSlotHeader(respData, 1, uint32(len(respData)))
		respData[SlotHeaderSize] = proto.OpOk
		if err = conn.WriteData(0, respData); err != nil {
			serverDone <- fmt.Errorf("server WriteResponse: %w", err)
			return
		}

		serverDone <- nil
	}()

	// Wait for server to be ready
	select {
	case <-serverReady:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not start in time")
	}

	// --- Client side ---
	poolCfg := RDMAPoolConfig{
		NumSlots: numSlots,
		SlotSize: slotSize,
		MaxConns: 1,
	}
	pool, err := NewRDMAConnPool(poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	localIP := getLoopbackIP(t)
	addr := fmt.Sprintf("%s:%d", localIP, port)

	conn, err := pool.GetConnect(addr)
	if err != nil {
		t.Fatalf("client GetConnect: %v", err)
	}
	defer pool.PutConnect(conn, true)

	// Build test packet
	p := proto.NewPacket()
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpWrite
	p.PartitionID = 42
	p.ExtentID = 7
	p.ExtentOffset = 0
	p.ReqID = 1
	p.Size = 5
	p.Data = []byte("hello")

	// Send via RDMA
	if err = conn.WritePacket(0, p); err != nil {
		t.Fatalf("client WritePacket: %v", err)
	}

	// Wait for response doorbell (client's recvDB seq changes from 0)
	var respLastSeq uint32
	deadline := time.Now().Add(5 * time.Second)
	for {
		if seq, ok := conn.PollRecvDoorbell(0, respLastSeq); ok {
			respLastSeq = seq
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for response")
		}
	}

	// Verify server received correct packet
	select {
	case recv := <-receivedPacket:
		if recv.PartitionID != 42 {
			t.Errorf("PartitionID: got %d want 42", recv.PartitionID)
		}
		if recv.ExtentID != 7 {
			t.Errorf("ExtentID: got %d want 7", recv.ExtentID)
		}
		if string(recv.Data) != "hello" {
			t.Errorf("Data: got %q want %q", recv.Data, "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive packet on server side")
	}

	// Wait for server to finish
	if err = <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func BenchmarkWritePacket4MB(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping RDMA benchmark in short mode")
	}
	b.Skip("benchmark requires hardware RoCEv2 or rxe; run manually on Linux")
}

// getLoopbackIP returns the rxe-capable loopback address (usually "127.0.0.1").
func getLoopbackIP(t *testing.T) string {
	t.Helper()
	addrs, err := net.InterfaceByName("lo")
	if err != nil {
		t.Skip("no lo interface found, skipping RDMA loopback test")
	}
	ips, err := addrs.Addrs()
	if err != nil || len(ips) == 0 {
		t.Skip("lo has no addresses")
	}
	for _, a := range ips {
		if ip, _, err := net.ParseCIDR(a.String()); err == nil && ip.To4() != nil {
			return ip.String()
		}
	}
	return "127.0.0.1"
}
