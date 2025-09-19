package metanode

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuffDentryBuf(t *testing.T) {
	b1 := GetDentryBuf()
	require.True(t, b1.Cap() == dentryBufSize && b1.Len() == 0)

	d1 := []byte("test hello")
	n, err := b1.Write(d1)
	if err != nil || n != len(d1) {
		t.Fail()
	}
	PutDentryBuf(b1)

	b2 := GetDentryBuf()
	require.True(t, b2.Cap() == dentryBufSize)
	require.True(t, b2.Len() == 0)

	// data overflow buf size
	d2 := make([]byte, dentryBufSize*2)
	n, err = b2.Write(d2)
	if err != nil || n != len(d2) || b2.Len() != n {
		t.Fail()
	}
	PutDentryBuf(b2)

	b3 := GetDentryBuf()
	if b3.Len() != 0 {
		t.Fail()
	}
}

func TestBuffInodeBuf(t *testing.T) {
	b1 := GetInodeBuf()
	require.True(t, b1.Cap() == inodeBufSize)

	d1 := []byte("test hello")
	n, err := b1.Write(d1)
	if err != nil || n != len(d1) {
		t.Fail()
	}
	PutInodeBuf(b1)

	b2 := GetInodeBuf()
	require.True(t, b2.Cap() == inodeBufSize)
	require.True(t, b2.Len() == 0)

	// data overflow buf size
	d2 := make([]byte, inodeBufSize*2)
	n, err = b2.Write(d2)
	if err != nil || n != len(d2) || b2.Len() != n {
		t.Fail()
	}
	PutInodeBuf(b2)

	b3 := GetInodeBuf()
	if b3.Len() != 0 {
		t.Fail()
	}
}

func TestBuffReadBuf(t *testing.T) {
	data := []byte("test hello")
	b1 := GetReadBuf(data)
	require.True(t, b1.Len() == len(data))
	_, err := b1.ReadUint64()
	if err != nil {
		t.Fail()
	}
	PutReadBuf(b1)

	b2 := GetReadBuf(data)
	require.True(t, b2.Len() == len(data))
	PutReadBuf(b2)
}

// TestBuffNilHandling tests nil pointer handling in Put functions
func TestBuffNilHandling(t *testing.T) {
	// Test PutInodeBuf with nil
	PutInodeBuf(nil)

	// Test PutDentryBuf with nil
	PutDentryBuf(nil)

	// Test PutReadBuf with nil
	PutReadBuf(nil)

	// Should not panic
	require.True(t, true)
}

// TestBuffBufferReuse tests that buffers are properly reused from the pool
func TestBuffBufferReuse(t *testing.T) {
	// Test inode buffer reuse
	buf1 := GetInodeBuf()
	buf1.Write([]byte("test data"))
	PutInodeBuf(buf1)

	buf2 := GetInodeBuf()
	require.True(t, buf2.Len() == 0, "Buffer should be reset after Put")
	require.True(t, buf2.Cap() == inodeBufSize, "Buffer capacity should be maintained")

	// Test dentry buffer reuse
	dentryBuf1 := GetDentryBuf()
	dentryBuf1.Write([]byte("dentry data"))
	PutDentryBuf(dentryBuf1)

	dentryBuf2 := GetDentryBuf()
	require.True(t, dentryBuf2.Len() == 0, "Dentry buffer should be reset after Put")
	require.True(t, dentryBuf2.Cap() == dentryBufSize, "Dentry buffer capacity should be maintained")

	// Test read buffer reuse
	readData := []byte("read test data")
	readBuf1 := GetReadBuf(readData)
	require.True(t, readBuf1.Len() == len(readData))
	PutReadBuf(readBuf1)

	readBuf2 := GetReadBuf(readData)
	require.True(t, readBuf2.Len() == len(readData), "Read buffer should work correctly after reuse")
}

// TestBuffConcurrentAccess tests concurrent access to buffer pools
func TestBuffConcurrentAccess(t *testing.T) {
	const numGoroutines = 100
	const numOperations = 10

	var wg sync.WaitGroup

	// Test concurrent inode buffer access
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				buf := GetInodeBuf()
				buf.Write([]byte("concurrent test data"))
				PutInodeBuf(buf)
			}
		}()
	}
	wg.Wait()

	// Test concurrent dentry buffer access
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				buf := GetDentryBuf()
				buf.Write([]byte("concurrent dentry data"))
				PutDentryBuf(buf)
			}
		}()
	}
	wg.Wait()

	// Test concurrent read buffer access
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				data := []byte("concurrent read data")
				buf := GetReadBuf(data)
				require.True(t, buf.Len() == len(data))
				PutReadBuf(buf)
			}
		}()
	}
	wg.Wait()
}

// TestBuffBufferCapacity tests buffer capacity limits
func TestBuffBufferCapacity(t *testing.T) {
	// Test inode buffer capacity
	buf := GetInodeBuf()
	require.True(t, buf.Cap() == inodeBufSize, "Inode buffer should have correct capacity")

	// Write data up to capacity
	largeData := make([]byte, inodeBufSize)
	n, err := buf.Write(largeData)
	require.NoError(t, err)
	require.True(t, n == inodeBufSize, "Should write all data")
	require.True(t, buf.Len() == inodeBufSize, "Buffer length should match written data")

	PutInodeBuf(buf)

	// Test dentry buffer capacity
	dentryBuf := GetDentryBuf()
	require.True(t, dentryBuf.Cap() == dentryBufSize, "Dentry buffer should have correct capacity")

	// Write data up to capacity
	dentryData := make([]byte, dentryBufSize)
	n, err = dentryBuf.Write(dentryData)
	require.NoError(t, err)
	require.True(t, n == dentryBufSize, "Should write all dentry data")
	require.True(t, dentryBuf.Len() == dentryBufSize, "Dentry buffer length should match written data")

	PutDentryBuf(dentryBuf)
}

// TestBuffReadBufferOperations tests various read buffer operations
func TestBuffReadBufferOperations(t *testing.T) {
	// Test with empty data
	emptyData := []byte{}
	buf := GetReadBuf(emptyData)
	require.True(t, buf.Len() == 0, "Empty data should result in zero length")
	PutReadBuf(buf)

	// Test with small data
	smallData := []byte{0x01, 0x02, 0x03, 0x04}
	buf = GetReadBuf(smallData)
	require.True(t, buf.Len() == len(smallData), "Small data should be handled correctly")
	PutReadBuf(buf)

	// Test with large data
	largeData := make([]byte, 10000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	buf = GetReadBuf(largeData)
	require.True(t, buf.Len() == len(largeData), "Large data should be handled correctly")
	PutReadBuf(buf)
}

// TestBuffBufferReset tests that buffers are properly reset after Put
func TestBuffBufferReset(t *testing.T) {
	// Test inode buffer reset
	buf := GetInodeBuf()
	buf.Write([]byte("test data"))
	require.True(t, buf.Len() > 0, "Buffer should have data")
	PutInodeBuf(buf)

	// Get buffer again and verify it's reset
	buf = GetInodeBuf()
	require.True(t, buf.Len() == 0, "Buffer should be reset after Put")

	// Test dentry buffer reset
	dentryBuf := GetDentryBuf()
	dentryBuf.Write([]byte("dentry test data"))
	require.True(t, dentryBuf.Len() > 0, "Dentry buffer should have data")
	PutDentryBuf(dentryBuf)

	// Get dentry buffer again and verify it's reset
	dentryBuf = GetDentryBuf()
	require.True(t, dentryBuf.Len() == 0, "Dentry buffer should be reset after Put")

	// Test read buffer reset
	readData := []byte("read test data")
	readBuf := GetReadBuf(readData)
	require.True(t, readBuf.Len() == len(readData), "Read buffer should have correct length")
	PutReadBuf(readBuf)

	// Get read buffer again and verify it's reset
	readBuf = GetReadBuf(readData)
	require.True(t, readBuf.Len() == len(readData), "Read buffer should work correctly after reset")
}

// BenchmarkBuffInodeBuf benchmarks inode buffer operations
func BenchmarkBuffInodeBuf(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := GetInodeBuf()
		buf.Write([]byte("benchmark test data"))
		PutInodeBuf(buf)
	}
}

// BenchmarkBuffDentryBuf benchmarks dentry buffer operations
func BenchmarkBuffDentryBuf(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := GetDentryBuf()
		buf.Write([]byte("benchmark dentry data"))
		PutDentryBuf(buf)
	}
}

// BenchmarkBuffReadBuf benchmarks read buffer operations
func BenchmarkBuffReadBuf(b *testing.B) {
	testData := []byte("benchmark read data")
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := GetReadBuf(testData)
		_ = buf.Len()
		PutReadBuf(buf)
	}
}
