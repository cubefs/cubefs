package ping

import (
	"bytes"
	"net"
	"runtime/debug"
	"testing"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

func TestProcessPacket(t *testing.T) {
	pinger := makeTestPinger()
	shouldBe1 := 0
	// this function should be called
	pinger.OnRecv = func(pkt *Packet) {
		shouldBe1++
	}

	data := append(timeToBytes(time.Now()), uintToBytes(pinger.Tracker)...)
	if remainSize := pinger.Size - timeSliceLength - trackerLength; remainSize > 0 {
		data = append(data, bytes.Repeat([]byte{1}, remainSize)...)
	}

	body := &icmp.Echo{
		ID:   pinger.id,
		Seq:  pinger.sequence,
		Data: data,
	}
	pinger.awaitingSequences[pinger.sequence] = struct{}{}

	msg := &icmp.Message{
		Type: ipv4.ICMPTypeEchoReply,
		Code: 0,
		Body: body,
	}

	msgBytes, _ := msg.Marshal(nil)

	pkt := packet{
		nbytes: len(msgBytes),
		bytes:  msgBytes,
		ttl:    24,
	}

	err := pinger.processPacket(&pkt)
	AssertNoError(t, err)
	AssertTrue(t, shouldBe1 == 1)
}

func TestProcessPacket_IgnoreNonEchoReplies(t *testing.T) {
	pinger := makeTestPinger()
	shouldBe0 := 0
	// this function should not be called because the tracker is mismatched
	pinger.OnRecv = func(pkt *Packet) {
		shouldBe0++
	}

	data := append(timeToBytes(time.Now()), uintToBytes(pinger.Tracker)...)
	if remainSize := pinger.Size - timeSliceLength - trackerLength; remainSize > 0 {
		data = append(data, bytes.Repeat([]byte{1}, remainSize)...)
	}

	body := &icmp.Echo{
		ID:   pinger.id,
		Seq:  pinger.sequence,
		Data: data,
	}

	msg := &icmp.Message{
		Type: ipv4.ICMPTypeDestinationUnreachable,
		Code: 0,
		Body: body,
	}

	msgBytes, _ := msg.Marshal(nil)

	pkt := packet{
		nbytes: len(msgBytes),
		bytes:  msgBytes,
		ttl:    24,
	}

	err := pinger.processPacket(&pkt)
	AssertNoError(t, err)
	AssertTrue(t, shouldBe0 == 0)
}

func TestProcessPacket_IDMismatch(t *testing.T) {
	pinger := makeTestPinger()
	pinger.protocol = "icmp" // ID is only checked on "icmp" protocol
	shouldBe0 := 0
	// this function should not be called because the tracker is mismatched
	pinger.OnRecv = func(pkt *Packet) {
		shouldBe0++
	}

	data := append(timeToBytes(time.Now()), uintToBytes(pinger.Tracker)...)
	if remainSize := pinger.Size - timeSliceLength - trackerLength; remainSize > 0 {
		data = append(data, bytes.Repeat([]byte{1}, remainSize)...)
	}

	body := &icmp.Echo{
		ID:   999999,
		Seq:  pinger.sequence,
		Data: data,
	}

	msg := &icmp.Message{
		Type: ipv4.ICMPTypeEchoReply,
		Code: 0,
		Body: body,
	}

	msgBytes, _ := msg.Marshal(nil)

	pkt := packet{
		nbytes: len(msgBytes),
		bytes:  msgBytes,
		ttl:    24,
	}

	err := pinger.processPacket(&pkt)
	AssertNoError(t, err)
	AssertTrue(t, shouldBe0 == 0)
}

func TestProcessPacket_TrackerMismatch(t *testing.T) {
	pinger := makeTestPinger()
	shouldBe0 := 0
	// this function should not be called because the tracker is mismatched
	pinger.OnRecv = func(pkt *Packet) {
		shouldBe0++
	}

	data := append(timeToBytes(time.Now()), uintToBytes(999)...)
	if remainSize := pinger.Size - timeSliceLength - trackerLength; remainSize > 0 {
		data = append(data, bytes.Repeat([]byte{1}, remainSize)...)
	}

	body := &icmp.Echo{
		ID:   pinger.id,
		Seq:  pinger.sequence,
		Data: data,
	}

	msg := &icmp.Message{
		Type: ipv4.ICMPTypeEchoReply,
		Code: 0,
		Body: body,
	}

	msgBytes, _ := msg.Marshal(nil)

	pkt := packet{
		nbytes: len(msgBytes),
		bytes:  msgBytes,
		ttl:    24,
	}

	err := pinger.processPacket(&pkt)
	AssertNoError(t, err)
	AssertTrue(t, shouldBe0 == 0)
}

func TestProcessPacket_LargePacket(t *testing.T) {
	pinger := makeTestPinger()
	pinger.Size = 4096

	data := append(timeToBytes(time.Now()), uintToBytes(pinger.Tracker)...)
	if remainSize := pinger.Size - timeSliceLength - trackerLength; remainSize > 0 {
		data = append(data, bytes.Repeat([]byte{1}, remainSize)...)
	}

	body := &icmp.Echo{
		ID:   pinger.id,
		Seq:  pinger.sequence,
		Data: data,
	}

	msg := &icmp.Message{
		Type: ipv4.ICMPTypeEchoReply,
		Code: 0,
		Body: body,
	}

	msgBytes, _ := msg.Marshal(nil)

	pkt := packet{
		nbytes: len(msgBytes),
		bytes:  msgBytes,
		ttl:    24,
	}

	err := pinger.processPacket(&pkt)
	AssertNoError(t, err)
}

func TestProcessPacket_PacketTooSmall(t *testing.T) {
	pinger := makeTestPinger()
	data := []byte("foo")

	body := &icmp.Echo{
		ID:   pinger.id,
		Seq:  pinger.sequence,
		Data: data,
	}

	msg := &icmp.Message{
		Type: ipv4.ICMPTypeEchoReply,
		Code: 0,
		Body: body,
	}

	msgBytes, _ := msg.Marshal(nil)

	pkt := packet{
		nbytes: len(msgBytes),
		bytes:  msgBytes,
		ttl:    24,
	}

	err := pinger.processPacket(&pkt)
	AssertError(t, err, "")
}

func TestNewPingerValid(t *testing.T) {
	p := New("www.google.com")
	err := p.Resolve()
	AssertNoError(t, err)
	AssertEqualStrings(t, "www.google.com", p.Addr())
	// DNS names should resolve into IP addresses
	AssertNotEqualStrings(t, "www.google.com", p.IPAddr().String())
	AssertTrue(t, isIPv4(p.IPAddr().IP))
	AssertFalse(t, p.Privileged())
	// Test that SetPrivileged works
	p.SetPrivileged(true)
	AssertTrue(t, p.Privileged())
	// Test setting to ipv4 address
	err = p.SetAddr("www.google.com")
	AssertNoError(t, err)
	AssertTrue(t, isIPv4(p.IPAddr().IP))
	// Test setting to ipv6 address
	err = p.SetAddr("ipv6.google.com")
	AssertNoError(t, err)
	AssertFalse(t, isIPv4(p.IPAddr().IP))

	p = New("localhost")
	err = p.Resolve()
	AssertNoError(t, err)
	AssertEqualStrings(t, "localhost", p.Addr())
	// DNS names should resolve into IP addresses
	AssertNotEqualStrings(t, "localhost", p.IPAddr().String())
	AssertTrue(t, isIPv4(p.IPAddr().IP))
	AssertFalse(t, p.Privileged())
	// Test that SetPrivileged works
	p.SetPrivileged(true)
	AssertTrue(t, p.Privileged())
	// Test setting to ipv4 address
	err = p.SetAddr("www.google.com")
	AssertNoError(t, err)
	AssertTrue(t, isIPv4(p.IPAddr().IP))
	// Test setting to ipv6 address
	err = p.SetAddr("ipv6.google.com")
	AssertNoError(t, err)
	AssertFalse(t, isIPv4(p.IPAddr().IP))

	p = New("127.0.0.1")
	err = p.Resolve()
	AssertNoError(t, err)
	AssertEqualStrings(t, "127.0.0.1", p.Addr())
	AssertTrue(t, isIPv4(p.IPAddr().IP))
	AssertFalse(t, p.Privileged())
	// Test that SetPrivileged works
	p.SetPrivileged(true)
	AssertTrue(t, p.Privileged())
	// Test setting to ipv4 address
	err = p.SetAddr("www.google.com")
	AssertNoError(t, err)
	AssertTrue(t, isIPv4(p.IPAddr().IP))
	// Test setting to ipv6 address
	err = p.SetAddr("ipv6.google.com")
	AssertNoError(t, err)
	AssertFalse(t, isIPv4(p.IPAddr().IP))

	p = New("ipv6.google.com")
	err = p.Resolve()
	AssertNoError(t, err)
	AssertEqualStrings(t, "ipv6.google.com", p.Addr())
	// DNS names should resolve into IP addresses
	AssertNotEqualStrings(t, "ipv6.google.com", p.IPAddr().String())
	AssertFalse(t, isIPv4(p.IPAddr().IP))
	AssertFalse(t, p.Privileged())
	// Test that SetPrivileged works
	p.SetPrivileged(true)
	AssertTrue(t, p.Privileged())
	// Test setting to ipv4 address
	err = p.SetAddr("www.google.com")
	AssertNoError(t, err)
	AssertTrue(t, isIPv4(p.IPAddr().IP))
	// Test setting to ipv6 address
	err = p.SetAddr("ipv6.google.com")
	AssertNoError(t, err)
	AssertFalse(t, isIPv4(p.IPAddr().IP))

	// ipv6 localhost:
	p = New("::1")
	err = p.Resolve()
	AssertNoError(t, err)
	AssertEqualStrings(t, "::1", p.Addr())
	AssertFalse(t, isIPv4(p.IPAddr().IP))
	AssertFalse(t, p.Privileged())
	// Test that SetPrivileged works
	p.SetPrivileged(true)
	AssertTrue(t, p.Privileged())
	// Test setting to ipv4 address
	err = p.SetAddr("www.google.com")
	AssertNoError(t, err)
	AssertTrue(t, isIPv4(p.IPAddr().IP))
	// Test setting to ipv6 address
	err = p.SetAddr("ipv6.google.com")
	AssertNoError(t, err)
	AssertFalse(t, isIPv4(p.IPAddr().IP))
}

func TestNewPingerInvalid(t *testing.T) {
	_, err := NewPinger("127.0.0.0.1")
	AssertError(t, err, "127.0.0.0.1")

	_, err = NewPinger("127..0.0.1")
	AssertError(t, err, "127..0.0.1")

	// The .invalid tld is guaranteed not to exist by RFC2606.
	_, err = NewPinger("wtf.invalid.")
	AssertError(t, err, "wtf.invalid.")

	_, err = NewPinger(":::1")
	AssertError(t, err, ":::1")

	_, err = NewPinger("ipv5.google.com")
	AssertError(t, err, "ipv5.google.com")
}

func TestSetIPAddr(t *testing.T) {
	googleaddr, err := net.ResolveIPAddr("ip", "www.google.com")
	if err != nil {
		t.Fatal("Can't resolve www.google.com, can't run tests")
	}

	// Create a localhost ipv4 pinger
	p := New("localhost")
	err = p.Resolve()
	AssertNoError(t, err)
	AssertEqualStrings(t, "localhost", p.Addr())

	// set IPAddr to google
	p.SetIPAddr(googleaddr)
	AssertEqualStrings(t, googleaddr.String(), p.Addr())
}

func TestEmptyIPAddr(t *testing.T) {
	_, err := NewPinger("")
	AssertError(t, err, "empty pinger did not return an error")
}

func TestStatisticsSunny(t *testing.T) {
	// Create a localhost ipv4 pinger
	p := New("localhost")
	err := p.Resolve()
	AssertNoError(t, err)
	AssertEqualStrings(t, "localhost", p.Addr())

	p.PacketsSent = 10
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})

	stats := p.Statistics()
	if stats.PacketsRecv != 10 {
		t.Errorf("Expected %v, got %v", 10, stats.PacketsRecv)
	}
	if stats.PacketsSent != 10 {
		t.Errorf("Expected %v, got %v", 10, stats.PacketsSent)
	}
	if stats.PacketLoss != 0 {
		t.Errorf("Expected %v, got %v", 0, stats.PacketLoss)
	}
	if stats.MinRtt != time.Duration(1000) {
		t.Errorf("Expected %v, got %v", time.Duration(1000), stats.MinRtt)
	}
	if stats.MaxRtt != time.Duration(1000) {
		t.Errorf("Expected %v, got %v", time.Duration(1000), stats.MaxRtt)
	}
	if stats.AvgRtt != time.Duration(1000) {
		t.Errorf("Expected %v, got %v", time.Duration(1000), stats.AvgRtt)
	}
	if stats.StdDevRtt != time.Duration(0) {
		t.Errorf("Expected %v, got %v", time.Duration(0), stats.StdDevRtt)
	}
}

func TestStatisticsLossy(t *testing.T) {
	// Create a localhost ipv4 pinger
	p := New("localhost")
	err := p.Resolve()
	AssertNoError(t, err)
	AssertEqualStrings(t, "localhost", p.Addr())

	p.PacketsSent = 20
	p.updateStatistics(&Packet{Rtt: time.Duration(10)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(10000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(800)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(40)})
	p.updateStatistics(&Packet{Rtt: time.Duration(100000)})
	p.updateStatistics(&Packet{Rtt: time.Duration(1000)})

	stats := p.Statistics()
	if stats.PacketsRecv != 10 {
		t.Errorf("Expected %v, got %v", 10, stats.PacketsRecv)
	}
	if stats.PacketsSent != 20 {
		t.Errorf("Expected %v, got %v", 20, stats.PacketsSent)
	}
	if stats.PacketLoss != 50 {
		t.Errorf("Expected %v, got %v", 50, stats.PacketLoss)
	}
	if stats.MinRtt != time.Duration(10) {
		t.Errorf("Expected %v, got %v", time.Duration(10), stats.MinRtt)
	}
	if stats.MaxRtt != time.Duration(100000) {
		t.Errorf("Expected %v, got %v", time.Duration(100000), stats.MaxRtt)
	}
	if stats.AvgRtt != time.Duration(11585) {
		t.Errorf("Expected %v, got %v", time.Duration(11585), stats.AvgRtt)
	}
	if stats.StdDevRtt != time.Duration(29603) {
		t.Errorf("Expected %v, got %v", time.Duration(29603), stats.StdDevRtt)
	}
}

// Test helpers
func makeTestPinger() *Pinger {
	pinger := New("127.0.0.1")

	pinger.ipv4 = true
	pinger.addr = "127.0.0.1"
	pinger.protocol = "icmp"
	pinger.id = 123
	pinger.Tracker = 456
	pinger.Size = 0

	return pinger
}

func AssertNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Expected No Error but got %s, Stack:\n%s",
			err, string(debug.Stack()))
	}
}

func AssertError(t *testing.T, err error, info string) {
	if err == nil {
		t.Errorf("Expected Error but got %s, %s, Stack:\n%s",
			err, info, string(debug.Stack()))
	}
}

func AssertEqualStrings(t *testing.T, expected, actual string) {
	if expected != actual {
		t.Errorf("Expected %s, got %s, Stack:\n%s",
			expected, actual, string(debug.Stack()))
	}
}

func AssertNotEqualStrings(t *testing.T, expected, actual string) {
	if expected == actual {
		t.Errorf("Expected %s, got %s, Stack:\n%s",
			expected, actual, string(debug.Stack()))
	}
}

func AssertTrue(t *testing.T, b bool) {
	if !b {
		t.Errorf("Expected True, got False, Stack:\n%s", string(debug.Stack()))
	}
}

func AssertFalse(t *testing.T, b bool) {
	if b {
		t.Errorf("Expected False, got True, Stack:\n%s", string(debug.Stack()))
	}
}

func BenchmarkProcessPacket(b *testing.B) {
	pinger := New("127.0.0.1")

	pinger.ipv4 = true
	pinger.addr = "127.0.0.1"
	pinger.protocol = "ip4:icmp"
	pinger.id = 123
	pinger.Tracker = 456

	t := append(timeToBytes(time.Now()), uintToBytes(pinger.Tracker)...)
	if remainSize := pinger.Size - timeSliceLength - trackerLength; remainSize > 0 {
		t = append(t, bytes.Repeat([]byte{1}, remainSize)...)
	}

	body := &icmp.Echo{
		ID:   pinger.id,
		Seq:  pinger.sequence,
		Data: t,
	}

	msg := &icmp.Message{
		Type: ipv4.ICMPTypeEchoReply,
		Code: 0,
		Body: body,
	}

	msgBytes, _ := msg.Marshal(nil)

	pkt := packet{
		nbytes: len(msgBytes),
		bytes:  msgBytes,
		ttl:    24,
	}

	for k := 0; k < b.N; k++ {
		pinger.processPacket(&pkt)
	}
}

func TestProcessPacket_IgnoresDuplicateSequence(t *testing.T) {
	pinger := makeTestPinger()
	// pinger.protocol = "icmp" // ID is only checked on "icmp" protocol
	shouldBe0 := 0
	dups := 0

	// this function should not be called because the tracker is mismatched
	pinger.OnRecv = func(pkt *Packet) {
		shouldBe0++
	}

	pinger.OnDuplicateRecv = func(pkt *Packet) {
		dups++
	}

	data := append(timeToBytes(time.Now()), uintToBytes(pinger.Tracker)...)
	if remainSize := pinger.Size - timeSliceLength - trackerLength; remainSize > 0 {
		data = append(data, bytes.Repeat([]byte{1}, remainSize)...)
	}

	body := &icmp.Echo{
		ID:   123,
		Seq:  0,
		Data: data,
	}
	// register the sequence as sent
	pinger.awaitingSequences[0] = struct{}{}

	msg := &icmp.Message{
		Type: ipv4.ICMPTypeEchoReply,
		Code: 0,
		Body: body,
	}

	msgBytes, _ := msg.Marshal(nil)

	pkt := packet{
		nbytes: len(msgBytes),
		bytes:  msgBytes,
		ttl:    24,
	}

	err := pinger.processPacket(&pkt)
	AssertNoError(t, err)
	// receive a duplicate
	err = pinger.processPacket(&pkt)
	AssertNoError(t, err)

	AssertTrue(t, shouldBe0 == 1)
	AssertTrue(t, dups == 1)
	AssertTrue(t, pinger.PacketsRecvDuplicates == 1)
}
