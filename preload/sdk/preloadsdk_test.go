package sdk

import (
	"github.com/cubefs/cubefs/util/log"
	"testing"
)

func TestConvertDebugLevel(t *testing.T) {
	t.Run("warn", func(t *testing.T) {
		if convertLogLevel("warn") != log.WarnLevel {
			t.Fatalf("expected WarnLevel")
		}
	})
	t.Run("warn2", func(t *testing.T) {
		if convertLogLevel("warn2") != log.DebugLevel {
			t.Fatalf("expected DebugLevel")
		}
	})
}
