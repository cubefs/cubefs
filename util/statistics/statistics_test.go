package statistics_test

import (
	"github.com/chubaofs/chubaofs/util/statistics"
	"golang.org/x/net/context"
	"math/rand"
	"testing"
	"time"
)

func TestReport(t *testing.T) {
	actions := []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"}
	volNum := 3
	partitionNum := 20
	useTime := int(1e10)

	statistics.TickerTime = 3 * time.Second

	ctx, cancel := context.WithCancel(context.Background())

	go statistics.StartReportJob(ctx, "127.0.0.1", "test")

	for i := 0; i < 100; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					statistics.Report(actions[rand.Intn(len(actions))], uint64(rand.Intn(volNum)), uint64(rand.Intn(partitionNum)), time.Duration(int64(rand.Intn(useTime))))
				}

			}
		}()
	}

	time.Sleep(60 * time.Second)
	cancel()
}
