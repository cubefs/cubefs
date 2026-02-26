package proto

import "testing"

func TestValidateTagExamples(t *testing.T) {
	tests := []struct {
		name  string
		tag   string
		valid bool
	}{
		{
			name:  "same source and destination should fail",
			tag:   "tag1->tag1",
			valid: false,
		},
		{
			name:  "swapped source and destination should fail",
			tag:   "tag1,tag2->tag2,tag1",
			valid: false,
		},
		{
			name:  "source less than destination should fail",
			tag:   "tag1->tag2,tag3",
			valid: false,
		},
		{
			name:  "source more than destination should fail",
			tag:   "tag1,tag2->tag3",
			valid: false,
		},
		{
			name:  "single pair should pass",
			tag:   "tag1->tag2",
			valid: true,
		},
		{
			name:  "two pairs should pass",
			tag:   "tag1,tag2->tag3,tag4",
			valid: true,
		},
		{
			name:  "three pairs should pass",
			tag:   "tag1,tag2,tag3->tag4,tag5,tag6",
			valid: true,
		},
		{
			name:  "duplicate source and destination should pass",
			tag:   "tag1,tag1->tag2,tag2",
			valid: true,
		},
		{
			name:  "three duplicate source and destination should pass",
			tag:   "tag1,tag1,tag1->tag2,tag2,tag2",
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ValidateTag(tt.tag)
			if got != tt.valid {
				t.Fatalf("ValidateTag(%q) = %v, want %v", tt.tag, got, tt.valid)
			}
		})
	}
}
