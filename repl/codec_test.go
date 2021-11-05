package repl

import (
	"reflect"
	"testing"

	"github.com/chubaofs/chubaofs/util/blackmagic"
)

func TestDecodeReplPacketArg(t *testing.T) {
	type TestSample struct {
		Raw             string
		ExpectFollowers []string
		ExpectQuorum    int
	}
	var samples = []TestSample{
		{
			Raw: "10.203.16.236:6000/10.203.29.78:6000/",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
			},
			ExpectQuorum: 0,
		},
		{
			Raw: "10.203.16.236:6000/10.203.29.78:6000/11.97.70.219:6000/11.97.70.220:6000/",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
				"11.97.70.219:6000",
				"11.97.70.220:6000",
			},
			ExpectQuorum: 0,
		},
		{
			Raw: "10.203.16.236:6000/10.203.29.78:6000/11.97.70.219:6000/11.97.70.220:6000/3",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
				"11.97.70.219:6000",
				"11.97.70.220:6000",
			},
			ExpectQuorum: 3,
		},
		{
			Raw: "10.203.16.236:6000/10.203.29.78:6000/11.97.70.219:6000/11.97.70.220:6000/5",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
				"11.97.70.219:6000",
				"11.97.70.220:6000",
			},
			ExpectQuorum: 5,
		},
		{
			Raw: "10.203.16.236:6000/10.203.29.78:6000/11.97.70.219:6000/11.97.70.220:6000/6",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
				"11.97.70.219:6000",
				"11.97.70.220:6000",
			},
			ExpectQuorum: 0,
		},
		{
			Raw: "10.203.16.236:6000/10.203.29.78:6000/11.97.70.219:6000/11.97.70.220:6000/-2",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
				"11.97.70.219:6000",
				"11.97.70.220:6000",
			},
			ExpectQuorum: 0,
		},
		{
			Raw:             "1",
			ExpectFollowers: []string{},
			ExpectQuorum:    0,
		},
	}

	for _, sample := range samples {
		decodedFollowers, decodedQuorum := DecodeReplPacketArg(blackmagic.StringToBytes(sample.Raw))
		if !reflect.DeepEqual(decodedFollowers, sample.ExpectFollowers) {
			t.Fatalf("followers validation mismatch.\n"+
				"raw    : %v\n"+
				"decoded: %v\n"+
				"actual : %v",
				sample.Raw,
				decodedFollowers,
				sample.ExpectFollowers)
		}
		if decodedQuorum != sample.ExpectQuorum {
			t.Fatalf("quorum validation mismatch.\n"+
				"raw    : %v\n"+
				"decoded: %v\n"+
				"actual : %v",
				sample.Raw,
				decodedQuorum,
				sample.ExpectQuorum)
		}
	}
}
