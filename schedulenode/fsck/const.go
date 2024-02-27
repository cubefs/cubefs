package fsck

import (
	"time"
)

const (
	InodeCheckOpt int = 1 << iota
	DentryCheckOpt
)

const (
	DefaultCheckInterval     = time.Minute * 1440
	DefaultSafeCleanInterval = time.Minute * 1440
	DefaultTaskConcurrency   = 1
)