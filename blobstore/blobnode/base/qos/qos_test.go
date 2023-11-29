package qos

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

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
		InitAndFixQosConfig(&conf)
		qos, err := NewIoQueueQos(conf)
		require.NoError(t, err)
		defer qos.Close()
		qos.ResetQosLimit(Config{})
		require.Equal(t, conf.NormalMBPS*humanize.MiByte, qos.(*IoQueueQos).conf.NormalMBPS)
		require.Equal(t, conf.BackgroundMBPS*humanize.MiByte, qos.(*IoQueueQos).conf.BackgroundMBPS)

		conf.BackgroundMBPS = 4
		InitAndFixQosConfig(&conf)
		conf.NormalMBPS = 0
		qos.ResetQosLimit(conf)
		require.NotEqual(t, int64(0), qos.(*IoQueueQos).conf.NormalMBPS)
		require.Equal(t, conf.BackgroundMBPS*humanize.MiByte, qos.(*IoQueueQos).conf.BackgroundMBPS)
	}

	// statGet, _ := flow.NewIOFlowStat("110", true)
	ioStat, _ := iostat.StatInit("", 0, true)
	iostat1, _ := iostat.StatInit("", 0, true)
	iom := &flow.IOFlowStat{}
	iom[0] = ioStat
	iom[1] = iostat1
	diskView := flow.NewDiskViewer(iom)
	conf := Config{
		NormalMBPS:      200,
		BackgroundMBPS:  1,
		DiskViewer:      diskView,
		StatGetter:      iom,
		ReadQueueDepth:  100,
		WriteQueueDepth: 100,
		WriteChanQueCnt: 2,
		MaxWaitCount:    2 * 100,
	}
	qos, err := NewIoQueueQos(conf)
	require.NoError(t, err)
	defer qos.Close()

	f, err := ioutil.TempFile(os.TempDir(), "TestQos")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	defer f.Close()

	q, ok := qos.(*IoQueueQos)
	require.True(t, ok)

	{
		ok = q.TryAcquireIO(ctx, 1, WriteType)
		require.True(t, ok)
		defer q.ReleaseIO(1, WriteType)
		reader := strings.NewReader("test qos")
		writer := qos.Writer(ctx, bnapi.NormalIO, f)
		n, err := io.Copy(writer, reader)
		require.Equal(t, int64(8), n)
		require.NoError(t, err)
	}

	{
		ok = q.TryAcquireIO(ctx, 1, ReadType)
		require.True(t, ok)
		defer q.ReleaseIO(1, ReadType)
		rt := qos.ReaderAt(ctx, bnapi.NormalIO, f)
		buf := make([]byte, 8)
		_, err = rt.ReadAt(buf, 0)
		require.NoError(t, err)

		ctx = bnapi.SetIoType(ctx, bnapi.IOTypeMax)
		require.Panics(t, func() {
			rt = qos.ReaderAt(ctx, bnapi.IOTypeMax, f)
		})
		ctx = bnapi.SetIoType(ctx, bnapi.NormalIO)
	}

	{
		wf, err := ioutil.TempFile(os.TempDir(), "TestQosReader")
		require.NoError(t, err)
		defer os.Remove(wf.Name())
		r := qos.Reader(ctx, bnapi.NormalIO, f)
		_, err = io.Copy(wf, r)
		require.NoError(t, err)
		wf.Close()
	}

	{
		wt := qos.WriterAt(ctx, bnapi.NormalIO, f)
		_, err = wt.WriteAt([]byte("hello"), 10)
		require.NoError(t, err)

		require.Panics(t, func() {
			wt = qos.WriterAt(ctx, bnapi.IOTypeMax, f)
		})
	}

	{
		require.Equal(t, int64(1*humanize.MiByte), q.conf.BackgroundMBPS)
		conf.BackgroundMBPS = defaultBackgroundBandwidthMBPS
		conf.NormalMBPS = defaultMaxBandwidthMBPS
		qos.ResetQosLimit(conf)
		require.Equal(t, int64(defaultMaxBandwidthMBPS*humanize.MiByte), q.conf.NormalMBPS)
		require.Equal(t, int64(defaultBackgroundBandwidthMBPS*humanize.MiByte), q.conf.BackgroundMBPS)
	}
}

func TestQosTryAcquire(t *testing.T) {
	ctx := context.Background()
	statGet, _ := flow.NewIOFlowStat("110", true)
	diskView := flow.NewDiskViewer(statGet)
	conf := Config{
		NormalMBPS:      100,
		BackgroundMBPS:  10,
		DiskViewer:      diskView,
		StatGetter:      statGet,
		ReadQueueDepth:  2,
		WriteQueueDepth: 2,
		WriteChanQueCnt: 2,
		MaxWaitCount:    2 * 2,
	}
	qos, err := NewIoQueueQos(conf)
	require.NoError(t, err)
	defer qos.Close()

	f, err := ioutil.TempFile(os.TempDir(), "TestQos")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	defer f.Close()
	q, ok := qos.(*IoQueueQos)
	require.True(t, ok)

	{
		for i := 0; i < q.conf.MaxWaitCount; i++ {
			ok = q.TryAcquireIO(ctx, 1, WriteType)
			require.True(t, ok)
		}

		ok = q.TryAcquireIO(ctx, 1, WriteType)
		require.False(t, ok)

		for i := 0; i < q.conf.MaxWaitCount; i++ {
			q.ReleaseIO(1, WriteType)
		}
	}

	{
		ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)

		for i := 0; i < q.conf.WriteQueueDepth; i++ {
			ok = q.TryAcquireIO(ctx, 1, WriteType)
			require.True(t, ok)
		}

		// discard ratio
		success := 0
		for i := 0; i < 1000; i++ {
			ok = q.TryAcquireIO(ctx, 1, WriteType)
			if ok {
				success++
			}
		}
		ratio := float64(success) / 1000
		t.Logf("success:%d, ration:%f", success, ratio)
		require.Less(t, ratio, 1.0)
		// require.Less(t, 0.0, ratio)
	}
}

func TestDynamicDiscard(t *testing.T) {
	rdQueDepth := 128
	qos := newIoQosDiscard(rdQueDepth)
	defer qos.Close()
	require.Equal(t, int32(ratioDiscardLow), qos.discardRatio)

	time.Sleep(time.Millisecond * 100)
	qos.currentCnt = 128 * 2
	time.Sleep(time.Millisecond * 1200)
	require.Equal(t, int32(ratioDiscardMid), qos.discardRatio)
}
