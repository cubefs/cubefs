package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFingerprint_Codec(t *testing.T) {
	var fgp1 Fingerprint
	fgp1.Append(1)
	fgp1.Append(2)
	fgp1.Append(3)
	fgp1.Append(4)
	fgp1.Append(5)
	fgp1.Append(6)
	encoded := fgp1.EncodeBinary()
	var fgp2 Fingerprint
	fgp2.DecodeBinary(encoded)
	assert.Equal(t, fgp1, fgp2)
}
