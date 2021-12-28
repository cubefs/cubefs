package bcache

import (
	"os"
	"syscall"
)

func AccessTime(info os.FileInfo) int64 {
	linuxFileAttr := info.Sys().(*syscall.Stat_t)
	return linuxFileAttr.Atim.Sec
}
