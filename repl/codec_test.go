package repl

import (
	"reflect"
	"testing"

	"github.com/cubefs/cubefs/util/blackmagic"
)

func TestDecodeReplPacketArg(t *testing.T) {
	type TestCase struct {
		Name            string
		Raw             string
		ExpectFollowers []string
		ExpectQuorum    int
	}
	var cases = []TestCase{
		{
			Name: "2 followers without quorum",
			Raw:  "10.203.16.236:6000/10.203.29.78:6000/",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
			},
			ExpectQuorum: 0,
		},
		{
			Name: "4 followers without quorum",
			Raw:  "10.203.16.236:6000/10.203.29.78:6000/11.97.70.219:6000/11.97.70.220:6000/",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
				"11.97.70.219:6000",
				"11.97.70.220:6000",
			},
			ExpectQuorum: 0,
		},
		{
			Name: "4 followers with 3 for valid quorum",
			Raw:  "10.203.16.236:6000/10.203.29.78:6000/11.97.70.219:6000/11.97.70.220:6000/3",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
				"11.97.70.219:6000",
				"11.97.70.220:6000",
			},
			ExpectQuorum: 3,
		},
		{
			Name: "4 followers with 5 for valid quorum",
			Raw:  "10.203.16.236:6000/10.203.29.78:6000/11.97.70.219:6000/11.97.70.220:6000/5",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
				"11.97.70.219:6000",
				"11.97.70.220:6000",
			},
			ExpectQuorum: 5,
		},
		{
			Name: "4 followers with 6 for invalid quorum",
			Raw:  "10.203.16.236:6000/10.203.29.78:6000/11.97.70.219:6000/11.97.70.220:6000/6",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
				"11.97.70.219:6000",
				"11.97.70.220:6000",
			},
			ExpectQuorum: 0,
		},
		{
			Name: "4 followers with negative value for invalid quorum",
			Raw:  "10.203.16.236:6000/10.203.29.78:6000/11.97.70.219:6000/11.97.70.220:6000/-2",
			ExpectFollowers: []string{
				"10.203.16.236:6000",
				"10.203.29.78:6000",
				"11.97.70.219:6000",
				"11.97.70.220:6000",
			},
			ExpectQuorum: 0,
		},
		{
			Name:            "illegal raw argument",
			Raw:             "1",
			ExpectFollowers: []string{},
			ExpectQuorum:    0,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.Name, func(t *testing.T) {
			decodedFollowers, decodedQuorum := DecodeReplPacketArg(blackmagic.StringToBytes(testCase.Raw))
			if !reflect.DeepEqual(decodedFollowers, testCase.ExpectFollowers) {
				t.Fatalf("followers validation mismatch.\n"+
					"raw    : %v\n"+
					"decoded: %v\n"+
					"actual : %v",
					testCase.Raw,
					decodedFollowers,
					testCase.ExpectFollowers)
			}
			if decodedQuorum != testCase.ExpectQuorum {
				t.Fatalf("quorum validation mismatch.\n"+
					"raw    : %v\n"+
					"decoded: %v\n"+
					"actual : %v",
					testCase.Raw,
					decodedQuorum,
					testCase.ExpectQuorum)
			}
		})
	}
}
