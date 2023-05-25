package repl

import (
	"fmt"
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
	dn1 := "127.0.0.1:6000"
	dn2 := "127.0.0.1:6001"
	dn3 := "127.0.0.1:6002"
	dn4 := "127.0.0.1:6003"
	var cases = []TestCase{
		{
			Name: "2 followers without quorum",
			Raw:  fmt.Sprintf("%v/%v/", dn1, dn2),
			ExpectFollowers: []string{
				dn1,
				dn2,
			},
			ExpectQuorum: 0,
		},
		{
			Name: "4 followers without quorum",
			Raw:  fmt.Sprintf("%v/%v/%v/", dn1, dn2, dn3),
			ExpectFollowers: []string{
				dn1, dn2, dn3,
			},
			ExpectQuorum: 0,
		},
		{
			Name: "4 followers with 3 for valid quorum",
			Raw:  fmt.Sprintf("%v/%v/%v/%v/3", dn1, dn2, dn3, dn4),
			ExpectFollowers: []string{
				dn1, dn2, dn3, dn4,
			},
			ExpectQuorum: 3,
		},
		{
			Name: "4 followers with 5 for valid quorum",
			Raw:  fmt.Sprintf("%v/%v/%v/%v/5", dn1, dn2, dn3, dn4),
			ExpectFollowers: []string{
				dn1, dn2, dn3, dn4,
			},
			ExpectQuorum: 5,
		},
		{
			Name: "4 followers with 6 for invalid quorum",
			Raw:  fmt.Sprintf("%v/%v/%v/%v/6", dn1, dn2, dn3, dn4),
			ExpectFollowers: []string{
				dn1, dn2, dn3, dn4,
			},
			ExpectQuorum: 0,
		},
		{
			Name: "4 followers with negative value for invalid quorum",
			Raw:  fmt.Sprintf("%v/%v/%v/%v/-2", dn1, dn2, dn3, dn4),
			ExpectFollowers: []string{
				dn1, dn2, dn3, dn4,
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
