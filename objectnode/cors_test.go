package objectnode

import (
	"fmt"
	"testing"
)

func TestCORSRule_MatchAny(t *testing.T) {
	type Input struct {
		Origin  string
		Method  string
		Headers []string
	}
	type Output struct {
		Match bool
	}
	type InputOutput struct {
		In  Input
		Out Output
	}
	type Sample struct {
		Rule  CORSRule
		Pairs []InputOutput
	}

	var samples = []Sample{
		{
			Rule: CORSRule{
				AllowedOrigin: []string{"*"},
				AllowedMethod: []string{"*"},
				AllowedHeader: []string{"*"},
			},
			Pairs: []InputOutput{
				{
					Out: Output{
						Match: true,
					},
				},
				{
					In: Input{
						Origin: "http://a.b.com",
					},
					Out: Output{
						Match: true,
					},
				},
				{
					In: Input{
						Method: "GET",
					},
					Out: Output{
						Match: true,
					},
				},
				{
					In: Input{
						Headers: []string{"Range"},
					},
					Out: Output{
						Match: true,
					},
				},
				{
					In: Input{
						Origin: "http://a.b,com",
						Method: "POST",
					},
					Out: Output{
						Match: true,
					},
				},
			},
		},
		{
			Rule: CORSRule{
				AllowedOrigin: []string{"http://a.b.com"},
				AllowedMethod: []string{"*"},
				AllowedHeader: []string{"*"},
			},
			Pairs: []InputOutput{
				{
					Out: Output{
						Match: false,
					},
				},
				{
					In: Input{
						Origin: "http://a.b.com",
					},
					Out: Output{
						Match: true,
					},
				},
			},
		},
		{
			Rule: CORSRule{
				AllowedOrigin: []string{"*"},
				AllowedMethod: []string{"GET", "POST"},
				AllowedHeader: []string{"*"},
			},
			Pairs: []InputOutput{
				{
					Out: Output{
						Match: false,
					},
				},
				{
					In: Input{
						Method: "GET",
					},
					Out: Output{
						Match: true,
					},
				},
				{
					In: Input{
						Method: "PUT",
					},
					Out: Output{
						Match: false,
					},
				},
				{
					In: Input{
						Origin: "http://a.b.com",
						Method: "GET",
					},
					Out: Output{
						Match: true,
					},
				},
			},
		},
		{
			Rule: CORSRule{
				AllowedOrigin: []string{"*"},
				AllowedMethod: []string{"GET", "POST", "POST", "PUT", "DELETE", "HEAD"},
				AllowedHeader: []string{"*"},
			},
			Pairs: []InputOutput{
				{
					Out: Output{
						Match: true,
					},
				},
			},
		},
	}

	for i, sample := range samples {
		sample.Rule.preprocess()
		for j, pair := range samples[i].Pairs {
			t.Run(fmt.Sprintf("Sample_%v_%v", i+1, j+1), func(t *testing.T) {
				if pair.Out.Match != sample.Rule.Match(pair.In.Origin, pair.In.Method, pair.In.Headers) {
					t.Fatalf("result mismatch:\n"+
						"\tCORSRule: %v\n"+
						"\tInput   : %v\n"+
						"\tExpect  : %v", sample.Rule, pair.In, pair.Out)
				}
			})

		}
	}

}
