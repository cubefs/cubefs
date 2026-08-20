package objectnode

import "testing"

func TestParseMaxKeys(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    uint64
		wantErr bool
	}{
		{name: "empty uses default", input: "", want: MaxKeys},
		{name: "small value", input: "100", want: 100},
		{name: "clamp over limit", input: "1001", want: MaxKeys},
		{name: "clamp huge value", input: "2147483647", want: MaxKeys},
		{name: "invalid string", input: "abc", wantErr: true},
		{name: "negative string", input: "-1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMaxKeys(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected max keys: got %d want %d", got, tt.want)
			}
		})
	}
}
