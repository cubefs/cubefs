package compact

import (
	"strings"
	"testing"
)

var volume *CompactVolumeInfo

func TestNewCompVolume(t *testing.T) {
	var err error
	mcc := &metaNodeControlConfig{100, 10, 2, 1, 32}
	if volume, err = NewCompVolume(ltptestVolume, clusterName, strings.Split(ltptestMaster, ","), mcc); err != nil {
		t.Fatalf("NewCompVolume should not hava err, but err:%v", err)
	}
}

func TestReleaseResource(t *testing.T) {
	TestNewCompVolume(t)
	volume.ReleaseResource()
}

func TestIsRunning(t *testing.T) {
	TestNewCompVolume(t)
	running := volume.isRunning()
	if !running {
		t.Fatalf("volume isRunning expect:%v, actual:%v", running, !running)
	}
}
