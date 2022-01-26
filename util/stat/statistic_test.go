package stat

import (
	"github.com/cubefs/cubefs/util/errors"
	"testing"
	"time"
)

func TestStatistic(t *testing.T) {
	statLogPath := "./"
	statLogSize := 20000000
	timeOutUs := [MaxTimeoutLevel]uint32{100000, 500000, 1000000}

	NewStatistic(statLogPath, "TestStatistic", int64(statLogSize), timeOutUs, true)
	bgTime := BeginStat()
	EndStat("test1", nil, bgTime, 1)
	time.Sleep(10 * time.Second)
	err := errors.New("EIO")
	EndStat("test2", err, bgTime, 100)
	time.Sleep(10 * time.Second)
	time.Sleep(50 * time.Second)
}
