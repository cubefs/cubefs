package version

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestVersionID_LessThan(t *testing.T) {
	tests := []struct {
		name		string
		version1	VersionID
		version2    VersionID
		less        bool
	}{
		{
			name:     "test01",
			version1: "3.9.9",
			version2: "4.0.0",
			less:     true,
		},
		{
			name:     "test02",
			version1: "4.0.1",
			version2: "4.0.0",
			less:     false,
		},
		{
			name:     "test03",
			version1: "3.9.9",
			version2: "3.9.9",
			less:     false,
		},
		{
			name:     "test04",
			version1: "4.9.9.8",
			version2: "5.0.0",
			less:     true,
		},
		{
			name:     "test05",
			version1: "5.0.0",
			version2: "5.0.0.1",
			less:     false,
		},
		{
			name:     "test06",
			version1: "5.0.0.1",
			version2: "4.2.0",
			less:     false,
		},
		{
			name:     "test07",
			version1: "4.0.0",
			version2: "4.0.1",
			less:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			less, err := tt.version1.LessThan(tt.version2)
			assert.Equal(t, nil, err, "compare err")
			assert.Equal(t, tt.less, less, "compare incorrect")
		})
	}
}
