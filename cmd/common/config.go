package common

import (
	"path"
	"syscall"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cubefs/cubefs/util/config"
)

const (
	configKeyLogDir           = "logDir"
	configKeyLogMaxSize       = "logMaxSize"
	configKeyLogMaxAge        = "logMaxAge"
	configKeyLogMaxBackups    = "logMaxBackups"
	configKeyLogReservedRatio = "logReservedRatio" // convert to ReservedSize
	configKeyLogReservedSize  = "logReservedSize"
	configKeyLogLocalTime     = "logLocalTime"
	configKeyLogCompress      = "logCompress"
)

func LoadLogger(module string, cfg *config.Config) *lumberjack.Logger {
	intGetter := func(key string, df int) int {
		val := cfg.GetInt(configKeyLogMaxSize)
		switch {
		case val < 0:
			return 0
		case val > 0:
			return val
		default:
			return df
		}
	}

	logDir := cfg.GetString(configKeyLogDir)

	reservedSize := cfg.GetInt(configKeyLogReservedSize)
	if reservedSize == 0 && logDir != "" {
		var fs syscall.Statfs_t
		if err := syscall.Statfs(logDir, &fs); err == nil {
			if ratio := cfg.GetFloat(configKeyLogReservedRatio); ratio > 0 {
				reservedSize = int(float64(fs.Blocks*uint64(fs.Bsize)) * ratio / (1 >> 20))
			}
		}
		if reservedSize <= 0 {
			reservedSize = 4 * 1024 // 4 GB
		}
	}

	localTime, setting := cfg.CheckAndGetBool(configKeyLogLocalTime)
	if !setting {
		localTime = true
	}
	return &lumberjack.Logger{
		Filename:     path.Join(logDir, module, module+".log"),
		MaxSize:      intGetter(configKeyLogMaxSize, 1024), // 1 GB
		MaxAge:       intGetter(configKeyLogMaxAge, 0),
		MaxBackups:   intGetter(configKeyLogMaxBackups, 0),
		ReservedSize: reservedSize,
		LocalTime:    localTime,
		Compress:     cfg.GetBool(configKeyLogCompress),
	}
}

func NewLogger(logDir string, module string,
	maxSize, maxAge, maxBackups, reservedSize int, localTime, compress bool,
) *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:     path.Join(logDir, module, module+".log"),
		MaxSize:      maxSize,
		MaxAge:       maxAge,
		MaxBackups:   maxBackups,
		ReservedSize: reservedSize,
		LocalTime:    localTime,
		Compress:     compress,
	}
}
