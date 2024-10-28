package qos

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
)

func TestNewQosManager(t *testing.T) {
	ctx := context.Background()
	ctx = bnapi.SetIoType(ctx, bnapi.NormalIO)

	{
		conf := Config{ReadQueueDepth: 1}
		InitAndFixQosConfig(&conf) // default value
		qos, err := NewIoQueueQos(conf)
		require.NoError(t, err)
		defer qos.Close()
		qos.ResetQosLimit(Config{})
		ioQos := qos.(*IoQueueQos)
		require.Equal(t, conf.ReadMBPS, ioQos.conf.ReadMBPS) // default value
		require.Equal(t, conf.WriteMBPS, ioQos.conf.WriteMBPS)
		require.Equal(t, conf.BackgroundMBPS, ioQos.conf.BackgroundMBPS)
		require.Equal(t, int(LimitTypeMax), len(ioQos.bpsLimiters))
		require.Equal(t, int(2*conf.WriteMBPS*humanize.MiByte), ioQos.bpsLimiters[LimitTypeWrite].Burst())
		require.Equal(t, int(2*conf.BackgroundMBPS*humanize.MiByte), ioQos.bpsLimiters[LimitTypeBack].Burst())
		require.Equal(t, int(2*conf.ReadMBPS*humanize.MiByte), ioQos.bpsLimiters[LimitTypeRead].Burst())

		conf.BackgroundMBPS = 4
		InitAndFixQosConfig(&conf)
		require.Equal(t, int64(4), conf.BackgroundMBPS)

		conf.WriteMBPS = 2
		InitAndFixQosConfig(&conf)
		require.Equal(t, int64(defaultReadBandwidthMBPS), conf.ReadMBPS)
		require.Equal(t, int64(2), conf.WriteMBPS)
		require.Equal(t, int64(2), conf.BackgroundMBPS)

		conf.WriteMBPS = 0
		qos.ResetQosLimit(conf)
		lmts := qos.(*IoQueueQos).bpsLimiters
		require.NotEqual(t, int64(0), int64(lmts[LimitTypeWrite].Limit()))
		require.Equal(t, conf.BackgroundMBPS*humanize.MiByte, int64(lmts[LimitTypeBack].Limit()))
	}

	// statGet, _ := flow.NewIOFlowStat("110", true)
	ioStat, _ := iostat.StatInit("", 0, true)
	iostat1, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	iom[0] = ioStat
	iom[1] = iostat1
	diskView := flow.NewDiskViewer(iom)
	conf := Config{
		ReadMBPS:        150,
		WriteMBPS:       120,
		BackgroundMBPS:  1,
		DiskViewer:      diskView,
		StatGetter:      iom,
		ReadQueueDepth:  100,
		WriteQueueDepth: 100,
		WriteChanQueCnt: 2,
		// MaxWaitCount:    2 * 100,
	}
	qos, err := NewIoQueueQos(conf)
	require.NoError(t, err)
	defer qos.Close()

	f, err := os.CreateTemp(os.TempDir(), "TestQos")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	defer f.Close()
	ss := "test qos"

	q, ok := qos.(*IoQueueQos)
	require.True(t, ok)

	{
		// write
		ok = q.TryAcquireIO(ctx, 1, IOTypeWrite)
		require.True(t, ok)
		defer q.ReleaseIO(1, IOTypeWrite)
		reader := strings.NewReader(ss)
		writer := qos.Writer(ctx, bnapi.NormalIO, f)
		n, err := io.Copy(writer, reader)
		require.Equal(t, int64(len(ss)), n)
		require.NoError(t, err)

		reader2 := strings.NewReader(ss)
		writer = qos.Writer(ctx, bnapi.BackgroundIO, f)
		n, err = io.Copy(writer, reader2)
		require.Equal(t, int64(len(ss)), n)
		require.NoError(t, err)
	}

	{
		// read at
		ok = q.TryAcquireIO(ctx, 1, IOTypeRead)
		require.True(t, ok)
		defer q.ReleaseIO(1, IOTypeRead)
		rt := qos.ReaderAt(ctx, bnapi.NormalIO, f)
		buf := make([]byte, len(ss)) // size 8
		_, err = rt.ReadAt(buf, 0)
		require.NoError(t, err)

		rt = qos.ReaderAt(ctx, bnapi.BackgroundIO, f)
		_, err = rt.ReadAt(buf, int64(len(ss)))
		require.NoError(t, err)

		ctx = bnapi.SetIoType(ctx, bnapi.IOTypeMax)
		require.Panics(t, func() {
			rt = qos.ReaderAt(ctx, bnapi.IOTypeMax, f)
		})
		ctx = bnapi.SetIoType(ctx, bnapi.NormalIO)
	}

	{
		// read
		wf, err := os.CreateTemp(os.TempDir(), "TestQosReader")
		require.NoError(t, err)
		defer os.Remove(wf.Name())
		defer wf.Close()

		r := qos.Reader(ctx, bnapi.NormalIO, f)
		_, err = io.Copy(wf, r)
		require.NoError(t, err)

		r = qos.Reader(ctx, bnapi.NormalIO, f)
		_, err = io.Copy(wf, r)
		require.NoError(t, err)
	}

	{
		// write at
		fi, err := f.Stat()
		require.NoError(t, err)
		oldSize := fi.Size()

		wt := qos.WriterAt(ctx, bnapi.NormalIO, f)
		data := []byte("hello")
		_, err = wt.WriteAt(data, oldSize)
		require.NoError(t, err)
		f.Sync()
		fi, err = f.Stat()
		require.NoError(t, err)
		require.Equal(t, oldSize+int64(len(data)), fi.Size())

		require.Panics(t, func() {
			wt = qos.WriterAt(ctx, bnapi.IOTypeMax, f)
		})
	}

	{
		// reset qos limiter
		require.Equal(t, int64(1), q.conf.BackgroundMBPS)
		conf.BackgroundMBPS = defaultBackgroundBandwidthMBPS
		conf.ReadMBPS = defaultReadBandwidthMBPS
		conf.WriteMBPS = defaultWriteBandwidthMBPS
		qos.ResetQosLimit(conf)
		lmts := qos.(*IoQueueQos).bpsLimiters
		require.Equal(t, int64(defaultWriteBandwidthMBPS*humanize.MiByte), int64(lmts[LimitTypeWrite].Limit()))
		require.Equal(t, int64(defaultBackgroundBandwidthMBPS*humanize.MiByte), int64(lmts[LimitTypeBack].Limit()))
		require.Equal(t, int64(defaultReadBandwidthMBPS*humanize.MiByte), int64(lmts[LimitTypeRead].Limit()))
	}

	{
		// reset discard
		conf.ReadDiscard = 70
		conf.WriteDiscard = 60
		qos.ResetQosLimit(conf)
		ioQos := qos.(*IoQueueQos)
		require.Equal(t, conf.ReadDiscard, ioQos.readDiscard.discardRatio)
		for _, dis := range ioQos.writeDiscard {
			require.Equal(t, conf.WriteDiscard, dis.discardRatio)
		}

	}
}

func TestQosTryAcquire(t *testing.T) {
	ctx := context.Background()
	statGet, _ := flow.NewIOFlowStat("110", true)
	diskView := flow.NewDiskViewer(statGet)
	conf := Config{
		ReadMBPS:        50,
		WriteMBPS:       40,
		BackgroundMBPS:  10,
		DiskViewer:      diskView,
		StatGetter:      statGet,
		ReadQueueDepth:  8,
		WriteQueueDepth: 4, // max num, total count of all write chan
		WriteChanQueCnt: 2,
	}
	qos, err := NewIoQueueQos(conf)
	require.NoError(t, err)
	defer qos.Close()

	f, err := os.CreateTemp(os.TempDir(), "TestQos")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	defer f.Close()
	q, ok := qos.(*IoQueueQos)
	require.True(t, ok)
	require.Equal(t, conf.ReadQueueDepth, q.maxWaitCnt[IOTypeRead])
	require.Equal(t, conf.WriteQueueDepth*conf.WriteChanQueCnt, q.maxWaitCnt[IOTypeWrite])
	require.Equal(t, int32(0), q.ioCnt[IOTypeRead])
	require.Equal(t, int32(0), q.ioCnt[IOTypeWrite])
	require.Equal(t, int(LimitTypeMax), len(q.bpsLimiters))
	require.Equal(t, int(conf.WriteChanQueCnt), len(q.writeDiscard))

	{
		// TryAcquireIO read
		for i := int32(0); i < q.conf.ReadQueueDepth; i++ {
			ok = q.TryAcquireIO(ctx, 1, IOTypeRead)
			require.True(t, ok)
		}

		ok = q.TryAcquireIO(ctx, 1, IOTypeRead)
		require.False(t, ok)

		// TryAcquireIO write high-level
		for i := int32(0); i < q.conf.WriteQueueDepth; i++ {
			ok = q.TryAcquireIO(ctx, 1, IOTypeWrite)
			require.True(t, ok)
			ok = q.TryAcquireIO(ctx, 2, IOTypeWrite)
			require.True(t, ok)
		}

		ok = q.TryAcquireIO(ctx, 1, IOTypeWrite)
		require.False(t, ok)
		ok = q.TryAcquireIO(ctx, 2, IOTypeWrite)
		require.False(t, ok)

		require.Equal(t, int32(8), q.ioCnt[IOTypeRead])
		require.Equal(t, int32(8), q.ioCnt[IOTypeWrite])
		require.Equal(t, int32(8), q.readDiscard.currentCnt)
		require.Equal(t, int32(4), q.writeDiscard[0].currentCnt)
		require.Equal(t, int32(4), q.writeDiscard[1].currentCnt)
		require.Equal(t, int32(4), q.writeDiscard[0].queueDepth)
		require.Equal(t, int32(4), q.writeDiscard[1].queueDepth)

		for i := int32(0); i < q.conf.WriteQueueDepth; i++ {
			q.ReleaseIO(1, IOTypeWrite)
			q.ReleaseIO(2, IOTypeWrite)
		}
		for i := int32(0); i < q.conf.ReadQueueDepth; i++ {
			q.ReleaseIO(1, IOTypeRead)
		}
		require.Equal(t, int32(0), q.ioCnt[IOTypeRead])
		require.Equal(t, int32(0), q.ioCnt[IOTypeWrite])
	}

	{
		// TryAcquireIO write low-level
		ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)

		for i := int32(0); i < q.conf.WriteQueueDepth/2; i++ {
			ok = q.TryAcquireIO(ctx, 1, IOTypeWrite)
			require.True(t, ok)
		}

		// discard ratio
		success := 0
		for i := 0; i < 100; i++ {
			ok = q.TryAcquireIO(ctx, 1, IOTypeWrite)
			if ok {
				success++
			}
		}

		require.Equal(t, int32(2+success), q.ioCnt[IOTypeWrite])
		require.Equal(t, int32(0), q.writeDiscard[0].currentCnt)
		require.Equal(t, int32(2+success), q.writeDiscard[1].currentCnt)
		ratio := float64(success) / 100
		t.Logf("success:%d, ration:%f", success, ratio)
		require.Less(t, ratio, 1.0)
		require.Less(t, 0.0, ratio)
	}
}
