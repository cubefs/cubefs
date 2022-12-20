package bloom

import (
	"math/rand"
	"testing"

	"github.com/twmb/murmur3"
)

// We want to preserve backward compatibility
func TestHashBasic(t *testing.T) {
	max_length := 1000
	bigdata := make([]byte, max_length)
	for i := 0; i < max_length; i++ {
		bigdata[i] = byte(i)
	}
	for length := 0; length <= 1000; length++ {
		data := bigdata[:length]
		var d digest128
		h1, h2, h3, h4 := d.sum256(data)
		//
		a1 := []byte{1} // to grab another bit of data
		hasher := murmur3.New128()
		hasher.Write(data) // #nosec
		v1, v2 := hasher.Sum128()
		hasher.Write(a1) // #nosec
		v3, v4 := hasher.Sum128()
		if v1 != h1 || v2 != h2 || v3 != h3 || v4 != h4 {
			t.Errorf("Backward compatibillity break.")
		}
	}
}

func TestDocumentation(t *testing.T) {
	filter := NewWithEstimates(10000, 0.01)
	got := EstimateFalsePositiveRate(filter.m, filter.k, 10000)
	if got > 0.011 || got < 0.009 {
		t.Errorf("Bad false positive rate %v", got)
	}
}

// We want to preserve backward compatibility
func TestHashRandom(t *testing.T) {
	max_length := 1000
	bigdata := make([]byte, max_length)
	for length := 0; length <= 1000; length++ {
		data := bigdata[:length]
		for trial := 1; trial < 10; trial++ {
			rand.Read(data)
			var d digest128
			h1, h2, h3, h4 := d.sum256(data)
			//
			a1 := []byte{1} // to grab another bit of data
			hasher := murmur3.New128()
			hasher.Write(data) // #nosec
			v1, v2 := hasher.Sum128()
			hasher.Write(a1) // #nosec
			v3, v4 := hasher.Sum128()
			if v1 != h1 || v2 != h2 || v3 != h3 || v4 != h4 {
				t.Errorf("Backward compatibillity break.")
			}
		}
	}
}
