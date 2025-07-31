package qos

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
)

type MockDiskViewer struct {
	readStat  *iostat.StatData
	writeStat *iostat.StatData
}

func (m *MockDiskViewer) ReadStat() *iostat.StatData  { return m.readStat }
func (m *MockDiskViewer) WriteStat() *iostat.StatData { return m.writeStat }
func (m *MockDiskViewer) Update()                     {}
func (m *MockDiskViewer) Close()                      {}

func getQosBpsLimiter(mgr *QosMgr, qosType bnapi.IOType) *rate.Limiter {
	ret, ok := mgr.qos[qosType]
	if ok {
		return ret.limitBps
	}
	return &rate.Limiter{}
}

// Test configuration functions
func TestInitAndFixQosConfig(t *testing.T) {
	t.Run("empty config with defaults", func(t *testing.T) {
		conf := &Config{}
		err := FixQosConfigOnInit(conf)
		require.NoError(t, err)
		require.NotNil(t, conf.FlowConf.Level)
		require.Len(t, conf.FlowConf.Level, 4)

		// Check default values
		require.Equal(t, defaultConfs[bnapi.ReadIO.String()], conf.FlowConf.Level[bnapi.ReadIO.String()])
	})

	t.Run("config with custom values", func(t *testing.T) {
		conf := &Config{
			FlowConf: FlowConfig{
				CommonDiskConfig: CommonDiskConfig{
					DiskBandwidthMB:  100,
					UpdateIntervalMs: 200,
					DiskIdleFactor:   0.5,
				},
				Level: map[string]LevelFlowConfig{
					bnapi.ReadIO.String():       {Concurrency: 500, MBPS: 50, Factor: 0.8},
					bnapi.WriteIO.String():      {Concurrency: 300, MBPS: 30, Factor: 0.9},
					bnapi.DeleteIO.String():     {Concurrency: 100, MBPS: 10, Factor: 0.5},
					bnapi.BackgroundIO.String(): {Concurrency: 50, MBPS: 5, Factor: 0.3},
				},
			},
		}
		err := FixQosConfigOnInit(conf)
		require.NoError(t, err)
		require.Equal(t, int64(500), conf.FlowConf.Level[bnapi.ReadIO.String()].Concurrency)
	})

	t.Run("config with invalid values", func(t *testing.T) {
		conf := &Config{
			FlowConf: FlowConfig{
				Level: map[string]LevelFlowConfig{
					bnapi.ReadIO.String(): {Concurrency: -1, MBPS: -1, Factor: -1},
				},
			},
		}
		err := FixQosConfigOnInit(conf)
		require.NoError(t, err)
		require.Equal(t, defaultConfs[bnapi.ReadIO.String()], conf.FlowConf.Level[bnapi.ReadIO.String()])
	})
}

func TestFixParaConfig(t *testing.T) {
	tests := []struct {
		name     string
		ioType   bnapi.IOType
		input    LevelFlowConfig
		expected LevelFlowConfig
		hasError bool
	}{
		{
			name:   "valid config",
			ioType: bnapi.ReadIO,
			input: LevelFlowConfig{
				Concurrency: 500,
				MBPS:        50,
				Factor:      0.8,
			},
			expected: LevelFlowConfig{
				BidConcurrency: defaultConfs[bnapi.ReadIO.String()].BidConcurrency,
				Concurrency:    500,
				MBPS:           50,
				Factor:         0.8,
				IdleFactor:     defaultConfs[bnapi.ReadIO.String()].IdleFactor,
			},
			hasError: false,
		},
		{
			name:     "zero values with defaults",
			ioType:   bnapi.WriteIO,
			input:    LevelFlowConfig{},
			expected: defaultConfs[bnapi.WriteIO.String()],
			hasError: false,
		},
		{
			name:   "negative values",
			ioType: bnapi.DeleteIO,
			input: LevelFlowConfig{
				Concurrency: -1,
				MBPS:        -1,
				Factor:      -1,
			},
			expected: defaultConfs[bnapi.DeleteIO.String()],
			hasError: false,
		},
		{
			name:   "factor out of range",
			ioType: bnapi.BackgroundIO,
			input: LevelFlowConfig{
				Concurrency: 1,
				MBPS:        1,
				Factor:      1.5, // > 1.0
			},
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixLevelFlowConfig(tt.input, defaultConfs[tt.ioType.String()], true)
			if tt.hasError {
				require.Error(t, err)
				require.Equal(t, ErrQosWrongConfig, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestNewQosMgr(t *testing.T) {
	t.Run("create QoS manager with valid config", func(t *testing.T) {
		// statGet, _ := flow.NewIOFlowStat("110", true)
		ioStat, _ := iostat.StatInit("", 0, true)
		iostat1, _ := iostat.StatInit("", 0, true)
		iom := &flow.IOFlowStat{}
		iom[0] = ioStat
		iom[1] = iostat1
		for i := range bnapi.GetAllIOType() {
			iom[i], _ = iostat.StatInit("", 0, true)
		}
		conf := Config{
			StatGetter: iom,
			DiskViewer: &MockDiskViewer{},
			FlowConf: FlowConfig{
				CommonDiskConfig: CommonDiskConfig{
					DiskBandwidthMB:  100,
					UpdateIntervalMs: 200,
					DiskIdleFactor:   0.5,
				},
				Level: map[string]LevelFlowConfig{
					bnapi.ReadIO.String():       {Concurrency: 500, MBPS: 50},
					bnapi.WriteIO.String():      {Concurrency: 300, MBPS: 30},
					bnapi.DeleteIO.String():     {Concurrency: 100, MBPS: 10},
					bnapi.BackgroundIO.String(): {Concurrency: 50, MBPS: 5},
				},
			},
		}

		mgr, err := NewQosMgr(conf)
		require.NoError(t, err)
		require.NotNil(t, mgr)
		defer mgr.Close()

		// Check that all IO types are initialized
		require.Len(t, mgr.qos, 4)
		require.NotNil(t, mgr.qos[bnapi.ReadIO])
		require.NotNil(t, mgr.qos[bnapi.WriteIO])
		require.NotNil(t, mgr.qos[bnapi.DeleteIO])
		require.NotNil(t, mgr.qos[bnapi.BackgroundIO])
	})

	t.Run("create QoS manager with invalid config", func(t *testing.T) {
		conf := Config{
			FlowConf: FlowConfig{
				Level: map[string]LevelFlowConfig{
					bnapi.ReadIO.String(): {Factor: 1.5}, // Invalid config
				},
			},
		}

		mgr, err := NewQosMgr(conf)
		require.Error(t, err)
		require.Nil(t, mgr)
	})
}

func TestQosMgr_GetQueueQos(t *testing.T) {
	ioStat, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	for i := range bnapi.GetAllIOType() {
		iom[i] = ioStat
	}
	conf := Config{
		StatGetter: iom,
		FlowConf: FlowConfig{
			CommonDiskConfig: CommonDiskConfig{
				DiskBandwidthMB:  100,
				UpdateIntervalMs: 200,
				DiskIdleFactor:   0.5,
			},
			Level: map[string]LevelFlowConfig{
				bnapi.ReadIO.String():       {Concurrency: 500, MBPS: 50},
				bnapi.WriteIO.String():      {Concurrency: 300, MBPS: 30},
				bnapi.DeleteIO.String():     {Concurrency: 100, MBPS: 10},
				bnapi.BackgroundIO.String(): {Concurrency: 50, MBPS: 5},
			},
		},
	}

	mgr, err := NewQosMgr(conf)
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("get read QoS", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.ReadIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)
		require.NotNil(t, qos)

		currConf := mgr.GetConfig()
		require.Equal(t, conf.FlowConf.DiskBandwidthMB, currConf.FlowConf.DiskBandwidthMB)
	})

	t.Run("get write QoS", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.WriteIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)
		require.NotNil(t, qos)
	})

	t.Run("get delete QoS", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.DeleteIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)
		require.NotNil(t, qos)
	})

	t.Run("get background QoS", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.BackgroundIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)
		require.NotNil(t, qos)
	})

	t.Run("get QoS with invalid IO type", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.IOTypeMax)
		qos, ok := mgr.GetQueueQos(ctx)
		require.False(t, ok)
		require.Nil(t, qos)
	})
}

func TestQosMgr_GetQosBpsLimiter(t *testing.T) {
	ioStat, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	for i := range bnapi.GetAllIOType() {
		iom[i] = ioStat
	}
	conf := Config{
		StatGetter: iom,
		FlowConf: FlowConfig{
			CommonDiskConfig: CommonDiskConfig{
				DiskBandwidthMB:  100,
				UpdateIntervalMs: 200,
				DiskIdleFactor:   0.5,
			},
			Level: map[string]LevelFlowConfig{
				bnapi.ReadIO.String():       {Concurrency: 500, MBPS: 50},
				bnapi.WriteIO.String():      {Concurrency: 300, MBPS: 30},
				bnapi.DeleteIO.String():     {Concurrency: 100, MBPS: 10},
				bnapi.BackgroundIO.String(): {Concurrency: 50, MBPS: 5},
			},
		},
	}

	mgr, err := NewQosMgr(conf)
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("get read IO bps limiter", func(t *testing.T) {
		limiter := getQosBpsLimiter(mgr, bnapi.ReadIO)
		require.NotNil(t, limiter)
		require.Greater(t, limiter.Limit(), float64(0))
	})

	t.Run("get write IO bps limiter", func(t *testing.T) {
		limiter := getQosBpsLimiter(mgr, bnapi.WriteIO)
		require.NotNil(t, limiter)
		require.Greater(t, limiter.Limit(), float64(0))
	})

	t.Run("get non-existent IO type limiter", func(t *testing.T) {
		limiter := getQosBpsLimiter(mgr, bnapi.IOTypeMax)
		require.NotNil(t, limiter)
		require.Equal(t, float64(0), float64(limiter.Limit()))
	})
}

func TestQueueQos_TryAcquire(t *testing.T) {
	ioStat, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	for i := range bnapi.GetAllIOType() {
		iom[i] = ioStat
	}
	conf := Config{
		StatGetter: iom,
		FlowConf: FlowConfig{
			CommonDiskConfig: CommonDiskConfig{
				DiskBandwidthMB:  100,
				UpdateIntervalMs: 200,
				DiskIdleFactor:   0.5,
			},
			Level: map[string]LevelFlowConfig{
				bnapi.ReadIO.String():       {Concurrency: 5, MBPS: 50},
				bnapi.WriteIO.String():      {Concurrency: 4, MBPS: 30},
				bnapi.DeleteIO.String():     {Concurrency: 3, MBPS: 10},
				bnapi.BackgroundIO.String(): {Concurrency: 2, MBPS: 5},
			},
		},
	}

	mgr, err := NewQosMgr(conf)
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("try acquire read IO", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.ReadIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)

		// Should be able to acquire
		maxCnt := conf.FlowConf.Level[bnapi.ReadIO.String()].Concurrency
		for i := 0; i < int(maxCnt); i++ {
			require.NoError(t, qos.Acquire())
		}

		// Queue should be full now
		require.Error(t, qos.Acquire())
		qos.Release()
		require.NoError(t, qos.Acquire())
	})

	// t.Run("try acquire with cancelled context", func(t *testing.T) {
	//	ctx, cancel := context.WithCancel(context.Background())
	//	ctx = bnapi.SetIoType(ctx, bnapi.WriteIO)
	//	cancel()
	//
	//	qos, ok := mgr.GetQueueQos(ctx)
	//	require.True(t, ok)
	//
	//	// Should not be able to acquire with cancelled context
	//	require.Error(t, qos.Acquire())
	// })

	t.Run("try acquire delete IO", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.DeleteIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)

		// Should be able to acquire
		maxCnt := conf.FlowConf.Level[bnapi.DeleteIO.String()].Concurrency
		for i := 0; i < int(maxCnt); i++ {
			require.NoError(t, qos.AcquireBid(uint64(i+1)))
			require.NoError(t, qos.Acquire())
		}

		// Queue should be full now
		// require.Error(t, qos.AcquireBid( 1)) // blocking
		require.Error(t, qos.Acquire())

		qos.ReleaseBid(1)
		require.NoError(t, qos.AcquireBid(1))
		qos.Release()
		require.NoError(t, qos.Acquire())
	})
}

func TestQueueQos_IO_Wrappers(t *testing.T) {
	ioStat, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	for i := range bnapi.GetAllIOType() {
		iom[i] = ioStat
	}
	conf := Config{
		StatGetter: iom,
		FlowConf: FlowConfig{
			CommonDiskConfig: CommonDiskConfig{
				DiskBandwidthMB:  100,
				UpdateIntervalMs: 200,
				DiskIdleFactor:   0.5,
			},
			Level: map[string]LevelFlowConfig{
				bnapi.ReadIO.String():       {Concurrency: 100, MBPS: 50},
				bnapi.WriteIO.String():      {Concurrency: 100, MBPS: 30},
				bnapi.DeleteIO.String():     {Concurrency: 100, MBPS: 10},
				bnapi.BackgroundIO.String(): {Concurrency: 100, MBPS: 5},
			},
		},
	}

	mgr, err := NewQosMgr(conf)
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("Reader wrapper", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.ReadIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)

		// Create a mock reader
		originalReader := strings.NewReader("test data")
		wrappedReader := qos.Reader(ctx, originalReader)

		require.NotNil(t, wrappedReader)
		require.NotEqual(t, originalReader, wrappedReader)

		// Test reading
		buf := make([]byte, 4)
		n, err := wrappedReader.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, "test", string(buf))
	})

	t.Run("Writer wrapper", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.WriteIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)

		// Create a mock writer
		var buf bytes.Buffer
		wrappedWriter := qos.Writer(ctx, &buf)

		require.NotNil(t, wrappedWriter)
		require.NotEqual(t, &buf, wrappedWriter)

		// Test writing
		data := []byte("test data")
		n, err := wrappedWriter.Write(data)
		require.NoError(t, err)
		require.Equal(t, len(data), n)
		require.Equal(t, "test data", buf.String())
	})

	t.Run("ReaderAt wrapper", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.ReadIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)

		// Create a mock ReaderAt
		originalReaderAt := strings.NewReader("test data for read at")
		wrappedReaderAt := qos.ReaderAt(ctx, originalReaderAt)

		require.NotNil(t, wrappedReaderAt)
		require.NotEqual(t, originalReaderAt, wrappedReaderAt)

		// Test reading at specific offset
		buf := make([]byte, 4)
		n, err := wrappedReaderAt.ReadAt(buf, 5)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, "data", string(buf))
	})

	t.Run("WriterAt wrapper", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.WriteIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)

		// Create a mock WriterAt
		buf := make([]byte, 20)
		wrappedWriterAt := qos.WriterAt(ctx, &mockWriterAt{buf: buf})

		require.NotNil(t, wrappedWriterAt)
		require.NotEqual(t, &mockWriterAt{buf: buf}, wrappedWriterAt)

		// Test writing at specific offset
		data := []byte("test")
		n, err := wrappedWriterAt.WriteAt(data, 5)
		require.NoError(t, err)
		require.Equal(t, len(data), n)
		require.Equal(t, "test", string(buf[5:9]))
	})
}

// Mock WriterAt implementation
type mockWriterAt struct {
	buf []byte
}

func (m *mockWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	copy(m.buf[off:], p)
	return len(p), nil
}

func TestQueueQos_ResetQosLimit(t *testing.T) {
	ioStat, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	for i := range bnapi.GetAllIOType() {
		iom[i] = ioStat
	}
	conf := Config{
		StatGetter: iom,
		FlowConf: FlowConfig{
			CommonDiskConfig: CommonDiskConfig{
				DiskBandwidthMB:  100,
				UpdateIntervalMs: 200,
				DiskIdleFactor:   0.5,
			},
			Level: map[string]LevelFlowConfig{
				bnapi.ReadIO.String():       {Concurrency: 500, MBPS: 50},
				bnapi.WriteIO.String():      {Concurrency: 300, MBPS: 30},
				bnapi.DeleteIO.String():     {Concurrency: 100, MBPS: 10},
				bnapi.BackgroundIO.String(): {Concurrency: 50, MBPS: 5},
			},
		},
	}

	mgr, err := NewQosMgr(conf)
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("reset QoS limit", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.WriteIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)

		// Get initial bandwidth limit
		initialLimit := getQosBpsLimiter(mgr, bnapi.WriteIO).Limit()

		// Reset with new config
		newConf := LevelFlowConfig{Concurrency: 600, MBPS: 100}

		qos.ResetLevelLimit(newConf)

		// Check that limit has changed
		newLimit := getQosBpsLimiter(mgr, bnapi.WriteIO).Limit()
		require.NotEqual(t, initialLimit, newLimit)
	})

	t.Run("reset QoS limit only one field", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.ReadIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)

		// Get initial bandwidth limit
		initialLimit := getQosBpsLimiter(mgr, bnapi.ReadIO).Limit()

		// Reset with new config
		newConf := Config{}
		err = FixQosConfigHotReset(&newConf)
		require.NoError(t, err)
		levelConf := newConf.FlowConf.Level[bnapi.ReadIO.String()]
		levelConf.MBPS = 5
		qos.ResetLevelLimit(levelConf)

		// Check that limit has changed
		newLimit := getQosBpsLimiter(mgr, bnapi.ReadIO).Limit()
		require.NotEqual(t, initialLimit, newLimit)
		require.Equal(t, 5*humanize.MiByte, int(newLimit))
		require.Equal(t, int64(500), qos.(*queueQos).conf.Concurrency)

		// only concurrency test
		levelConf = LevelFlowConfig{Concurrency: 50}
		qos.ResetLevelLimit(levelConf)
		require.Equal(t, 5*humanize.MiByte, int(getQosBpsLimiter(mgr, bnapi.ReadIO).Limit()))
		require.Equal(t, int64(50), qos.(*queueQos).conf.Concurrency)

		diskConf := CommonDiskConfig{DiskBandwidthMB: 1, DiskIdleFactor: 0.4}
		qos.ResetDiskLimit(diskConf)

		cur := qos.(*queueQos).GetQosConf()
		require.Equal(t, diskConf.DiskBandwidthMB, cur.DiskBandwidthMB)
		require.Equal(t, diskConf.DiskIdleFactor, cur.DiskIdleFactor)
	})
}

func TestQueueQos_Concurrency(t *testing.T) {
	ioStat, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	for i := range bnapi.GetAllIOType() {
		iom[i] = ioStat
	}
	conf := Config{
		StatGetter: iom,
		FlowConf: FlowConfig{
			CommonDiskConfig: CommonDiskConfig{
				DiskBandwidthMB:  100,
				UpdateIntervalMs: 200,
				DiskIdleFactor:   0.5,
			},
			Level: map[string]LevelFlowConfig{
				bnapi.ReadIO.String():       {Concurrency: 50, MBPS: 50},
				bnapi.WriteIO.String():      {Concurrency: 50, MBPS: 30},
				bnapi.DeleteIO.String():     {Concurrency: 50, MBPS: 10},
				bnapi.BackgroundIO.String(): {Concurrency: 50, MBPS: 5},
			},
		},
	}

	mgr, err := NewQosMgr(conf)
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("concurrent Acquire", func(t *testing.T) {
		ctx := bnapi.SetIoType(context.Background(), bnapi.ReadIO)
		qos, ok := mgr.GetQueueQos(ctx)
		require.True(t, ok)

		const numGoroutines = 10
		const attemptsPerGoroutine = 10
		var successCount int32
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var err1 error
				for j := 0; j < attemptsPerGoroutine; j++ {
					if err1 = qos.Acquire(); err1 != nil {
						atomic.AddInt32(&successCount, 1)
						time.Sleep(1 * time.Millisecond) // Simulate work
					}
				}
			}()
		}

		wg.Wait()

		// Should have some successful acquisitions but not all
		require.Greater(t, successCount, int32(0))
		require.LessOrEqual(t, successCount, int32(numGoroutines*attemptsPerGoroutine))
	})
}

func TestThreshold_Reset(t *testing.T) {
	t.Run("reset perIOQosConfig", func(t *testing.T) {
		th := &perIOQosConfig{
			CommonDiskConfig: CommonDiskConfig{
				DiskBandwidthMB:  100,
				UpdateIntervalMs: 200,
				DiskIdleFactor:   0.5,
			},
			LevelFlowConfig: LevelFlowConfig{
				Concurrency: 500,
				MBPS:        50,
				Factor:      0.8,
			},
		}

		newConf := LevelFlowConfig{Concurrency: 600, MBPS: 75, Factor: 0.9, BidConcurrency: 2}

		th.resetLevel(newConf)

		// Check that values were updated
		require.Equal(t, newConf, th.LevelFlowConfig)

		newDiskConf := CommonDiskConfig{
			DiskIops:         10,
			DiskBandwidthMB:  20,
			UpdateIntervalMs: 30,
			DiskIdleFactor:   0.4,
		}
		th.resetDisk(newDiskConf)
		require.Equal(t, newDiskConf.DiskBandwidthMB, th.DiskBandwidthMB)
		require.Equal(t, newDiskConf.DiskIdleFactor, th.DiskIdleFactor)
	})
}

func TestUtilityFunctions(t *testing.T) {
	t.Run("initMbpsLimiter", func(t *testing.T) {
		limiter := initLimiter(100)
		require.NotNil(t, limiter)
		require.Greater(t, limiter.Limit(), float64(0))

		// Test with zero bandwidth
		limiter = initLimiter(0)
		require.Nil(t, limiter)
	})

	t.Run("resetIOpsLimiter", func(t *testing.T) {
		limiter := initLimiter(1000)
		require.NotNil(t, limiter)
		require.Greater(t, limiter.Limit(), float64(0))

		// Test with zero IOps
		limiter = initLimiter(0)
		require.Nil(t, limiter)
	})

	t.Run("resetBpsLimiter", func(t *testing.T) {
		limiter := initLimiter(100)
		initialLimit := limiter.Limit()

		resetLimiter(limiter, 200)
		newLimit := limiter.Limit()

		require.NotEqual(t, initialLimit, newLimit)
	})
}

func TestStringToQosType(t *testing.T) {
	tests := []struct {
		input    string
		expected bnapi.IOType
	}{
		{bnapi.ReadIO.String(), bnapi.ReadIO},
		{bnapi.WriteIO.String(), bnapi.WriteIO},
		{bnapi.DeleteIO.String(), bnapi.DeleteIO},
		{bnapi.BackgroundIO.String(), bnapi.BackgroundIO},
		{"invalid", bnapi.IOTypeMax},
		{"", bnapi.IOTypeMax},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := bnapi.StringToIOType(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}

	// testGetQosTypeMap
	typeMap := bnapi.GetAllIOType()
	require.NotNil(t, typeMap)
	require.Len(t, typeMap, 4)

	require.Equal(t, bnapi.ReadIO.String(), typeMap[bnapi.ReadIO])
	require.Equal(t, bnapi.WriteIO.String(), typeMap[bnapi.WriteIO])
	require.Equal(t, bnapi.DeleteIO.String(), typeMap[bnapi.DeleteIO])
	require.Equal(t, bnapi.BackgroundIO.String(), typeMap[bnapi.BackgroundIO])
}

func BenchmarkQosMgr_TryAcquire(b *testing.B) {
	ioStat, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	for i := range bnapi.GetAllIOType() {
		iom[i] = ioStat
	}
	conf := Config{
		StatGetter: iom,
		FlowConf: FlowConfig{
			CommonDiskConfig: CommonDiskConfig{
				DiskBandwidthMB:  100,
				UpdateIntervalMs: 200,
				DiskIdleFactor:   0.5,
			},
			Level: map[string]LevelFlowConfig{
				bnapi.ReadIO.String():       {Concurrency: 500, MBPS: 50},
				bnapi.WriteIO.String():      {Concurrency: 500, MBPS: 30},
				bnapi.DeleteIO.String():     {Concurrency: 500, MBPS: 10},
				bnapi.BackgroundIO.String(): {Concurrency: 500, MBPS: 5},
			},
		},
	}

	mgr, err := NewQosMgr(conf)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.Close()

	ctx := bnapi.SetIoType(context.Background(), bnapi.ReadIO)
	qos, _ := mgr.GetQueueQos(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			qos.Acquire()
		}
	})
}
