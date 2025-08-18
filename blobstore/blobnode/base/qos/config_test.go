package qos

import (
	"testing"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
)

func TestInitAndFixQosConfig_EmptyConfig(t *testing.T) {
	// all config is empty
	conf := &Config{}
	err := FixQosConfigOnInit(conf)
	require.NoError(t, err)
	require.Equal(t, int64(defaultMaxMBps), conf.DiskBandwidthMB)
	require.Equal(t, int64(defaultIntervalMs), conf.UpdateIntervalMs)
	require.Equal(t, float64(defaultIdleFactor), conf.DiskIdleFactor)
	require.Len(t, conf.Level, 4)

	readConfig := conf.Level[bnapi.ReadIO.String()]
	require.Equal(t, defaultConfs[bnapi.ReadIO.String()], readConfig)

	writeConfig := conf.Level[bnapi.WriteIO.String()]
	require.Equal(t, defaultConfs[bnapi.WriteIO.String()], writeConfig)

	deleteConfig := conf.Level[bnapi.DeleteIO.String()]
	require.Equal(t, defaultConfs[bnapi.DeleteIO.String()], deleteConfig)

	backgroundConfig := conf.Level[bnapi.BackgroundIO.String()]
	require.Equal(t, defaultConfs[bnapi.BackgroundIO.String()], backgroundConfig)
}

func TestInitAndFixQosConfig_PartialConfig(t *testing.T) {
	// some config, some empty
	conf := &Config{
		FlowConfig: FlowConfig{
			Level: LevelConfigMap{
				bnapi.ReadIO.String(): {
					Concurrency: 1024,
					MBPS:        500,
					BusyFactor:  0.9,
				},
				bnapi.WriteIO.String(): {
					Concurrency: 256,
					MBPS:        200,
					BusyFactor:  0.8,
				},
				// DeleteIO and BackgroundIO not configure, should use default value
			},
		},
	}

	err := FixQosConfigOnInit(conf)
	require.NoError(t, err)

	readConfig := conf.Level[bnapi.ReadIO.String()]
	require.Equal(t, int64(1024), readConfig.Concurrency)
	require.Equal(t, int64(500), readConfig.MBPS)
	require.Equal(t, 0.9, readConfig.BusyFactor)

	writeConfig := conf.Level[bnapi.WriteIO.String()]
	require.Equal(t, int64(256), writeConfig.Concurrency)
	require.Equal(t, int64(200), writeConfig.MBPS)
	require.Equal(t, 0.8, writeConfig.BusyFactor)

	deleteConfig := conf.Level[bnapi.DeleteIO.String()]
	require.Equal(t, defaultConfs[bnapi.DeleteIO.String()], deleteConfig)

	backgroundConfig := conf.Level[bnapi.BackgroundIO.String()]
	require.Equal(t, defaultConfs[bnapi.BackgroundIO.String()], backgroundConfig)
}

func TestInitAndFixQosConfig_FullUserConfig(t *testing.T) {
	// all level qos config are set
	conf := &Config{
		FlowConfig: FlowConfig{
			CommonDiskConfig: CommonDiskConfig{
				DiskBandwidthMB:  2048,
				UpdateIntervalMs: 1000,
				DiskIdleFactor:   0.3,
			},
			Level: LevelConfigMap{
				bnapi.ReadIO.String(): {
					Concurrency: 2048,
					MBPS:        800,
					BusyFactor:  0.95,
				},
				bnapi.WriteIO.String(): {
					Concurrency: 512,
					MBPS:        400,
					BusyFactor:  0.85,
				},
				bnapi.DeleteIO.String(): {
					Concurrency: 256,
					MBPS:        100,
					BusyFactor:  0.75,
				},
				bnapi.BackgroundIO.String(): {
					Concurrency: 128,
					MBPS:        50,
					BusyFactor:  0.65,
				},
			},
		},
	}

	err := FixQosConfigOnInit(conf)
	require.NoError(t, err)

	require.Equal(t, int64(2048), conf.DiskBandwidthMB)
	require.Equal(t, int64(1000), conf.UpdateIntervalMs)
	require.Equal(t, 0.3, conf.DiskIdleFactor)

	readConfig := conf.Level[bnapi.ReadIO.String()]
	require.Equal(t, int64(2048), readConfig.Concurrency)
	require.Equal(t, int64(800), readConfig.MBPS)
	require.Equal(t, 0.95, readConfig.BusyFactor)

	writeConfig := conf.Level[bnapi.WriteIO.String()]
	require.Equal(t, int64(512), writeConfig.Concurrency)
	require.Equal(t, int64(400), writeConfig.MBPS)
	require.Equal(t, 0.85, writeConfig.BusyFactor)

	deleteConfig := conf.Level[bnapi.DeleteIO.String()]
	require.Equal(t, int64(256), deleteConfig.Concurrency)
	require.Equal(t, int64(100), deleteConfig.MBPS)
	require.Equal(t, 0.75, deleteConfig.BusyFactor)

	backgroundConfig := conf.Level[bnapi.BackgroundIO.String()]
	require.Equal(t, int64(128), backgroundConfig.Concurrency)
	require.Equal(t, int64(50), backgroundConfig.MBPS)
	require.Equal(t, 0.65, backgroundConfig.BusyFactor)
}

func TestInitAndFixQosConfig_InvalidConfig(t *testing.T) {
	// test invalid config
	conf := &Config{
		FlowConfig: FlowConfig{
			Level: LevelConfigMap{
				"invalid_io_type": {
					Concurrency: 100,
					MBPS:        50,
					BusyFactor:  0.5,
				},
			},
		},
	}

	err := FixQosConfigOnInit(conf)
	require.Error(t, err)
	require.Equal(t, ErrQosWrongConfig, err)
}

func TestInitAndFixQosConfig_ZeroValuesFixed(t *testing.T) {
	// test zero values fixed
	conf := &Config{
		FlowConfig: FlowConfig{
			Level: LevelConfigMap{
				bnapi.ReadIO.String(): {
					Concurrency: 1,
					MBPS:        0,
					BusyFactor:  0,
				},
			},
		},
	}

	err := FixQosConfigOnInit(conf)
	require.NoError(t, err)

	readConfig := conf.Level[bnapi.ReadIO.String()]
	require.Equal(t, defaultConfs[bnapi.ReadIO.String()].BidConcurrency, readConfig.BidConcurrency)
	require.Equal(t, int64(1), readConfig.Concurrency)
	require.Equal(t, defaultConfs[bnapi.ReadIO.String()].MBPS, readConfig.MBPS)
	require.Equal(t, defaultConfs[bnapi.ReadIO.String()].BusyFactor, readConfig.BusyFactor)
}

func TestInitAndFixQosConfig_NegativeValues(t *testing.T) {
	conf := &Config{
		FlowConfig: FlowConfig{
			Level: LevelConfigMap{
				bnapi.ReadIO.String(): {
					Concurrency: -100,
					MBPS:        -1,
					BusyFactor:  0,
				},
			},
		},
	}

	err := FixQosConfigOnInit(conf)
	require.NoError(t, err)
	// require.Equal(t, ErrQosWrongConfig, err)

	readConfig := conf.Level[bnapi.ReadIO.String()]
	require.Equal(t, defaultConfs[bnapi.ReadIO.String()], readConfig)
}
