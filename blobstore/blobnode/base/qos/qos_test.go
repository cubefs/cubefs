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
)

func TestNewQosManager(t *testing.T) {
	ctx := context.Background()
	conf := Config{
		DiskBandwidthMBPS: 1000,
		DiskIOPS:          1000,
		LevelConfigs: map[string]ParaConfig{
			"level1": {
				Iops:      100,
				Bandwidth: 100,
				Factor:    0.8,
			},
			"level2": {
				Iops:      100,
				Bandwidth: 100,
				Factor:    0.8,
			},
		},
		DiskViewer: nil,
		StatGetter: nil,
	}
	qos, err := NewQosManager(conf)
	require.NoError(t, err)

	f, err := os.CreateTemp(os.TempDir(), "TestQos")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	reader := strings.NewReader("test qos")
	writer := qos.Writer(ctx, bnapi.NormalIO, f)
	n, err := io.Copy(writer, reader)
	require.Equal(t, int64(8), n)
	require.NoError(t, err)

	rt := qos.ReaderAt(ctx, bnapi.NormalIO, f)
	buf := make([]byte, 8)
	_, err = rt.ReadAt(buf, 0)
	require.NoError(t, err)

	wf, err := os.CreateTemp(os.TempDir(), "TestQosReader")
	require.NoError(t, err)
	defer os.Remove(wf.Name())
	r := qos.Reader(ctx, bnapi.NormalIO, f)
	_, err = io.Copy(wf, r)
	require.NoError(t, err)
	wf.Close()

	wt := qos.WriterAt(ctx, bnapi.NormalIO, f)
	_, err = wt.WriteAt([]byte("hello"), 10)
	require.NoError(t, err)

	f.Close()
}

func TestThresholdReset(t *testing.T) {
	conf := Config{
		DiskBandwidthMBPS: 1000,
		DiskIOPS:          1000,
		LevelConfigs: map[string]ParaConfig{
			"level1": {
				Iops:      100,
				Bandwidth: 100,
				Factor:    0.8,
			},
			"level2": {
				Iops:      100,
				Bandwidth: 100,
				Factor:    0.8,
			},
		},
		DiskViewer: nil,
		StatGetter: nil,
	}

	threshold := &Threshold{
		ParaConfig: ParaConfig{
			Iops:      5,
			Bandwidth: 5,
			Factor:    0.1,
		},
		DiskIOPS:      conf.DiskIOPS,
		DiskBandwidth: conf.DiskBandwidthMBPS,
	}

	threshold.reset("level1", conf)
	require.Equal(t, int64(100), threshold.Iops)
	require.Equal(t, int64(100*humanize.MiByte), threshold.Bandwidth)
	require.Equal(t, 0.8, threshold.Factor)
}
