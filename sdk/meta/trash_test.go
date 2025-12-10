package meta

import (
	"path"
	"strings"
	"testing"
	"time"
)

func TestHashBucket(t *testing.T) {
	trash := &Trash{}
	b1 := trash.hashBucket("/a/b", "c")
	b2 := trash.hashBucket("/a/b", "c")
	if b1 != b2 {
		t.Fatalf("hashBucket not deterministic: %s vs %s", b1, b2)
	}
	if len(b1) != BucketHashWidth {
		t.Fatalf("bucket length want %d got %d", BucketHashWidth, len(b1))
	}
}

func TestRecoverPosixPathNamePlain(t *testing.T) {
	trash := &Trash{}
	encoded := "a|__|b|__|c"
	got := trash.recoverPosixPathName(encoded, 0)
	if got != "a/b/c" {
		t.Fatalf("recoverPosixPathName want a/b/c got %s", got)
	}
}

func TestGenerateTmpFileName(t *testing.T) {
	trash := &Trash{}

	if got := trash.generateTmpFileName(""); got != ParentDirPrefix {
		t.Fatalf("root tmp name want %s got %s", ParentDirPrefix, got)
	}

	// For nested path it should end with ParentDirPrefix and encode separators.
	got := trash.generateTmpFileName("a/b")
	if !strings.HasSuffix(got, ParentDirPrefix) {
		t.Fatalf("tmp name should end with ParentDirPrefix, got %s", got)
	}
	if !strings.Contains(got, ParentDirPrefix) {
		t.Fatalf("tmp name should contain encoded separator, got %s", got)
	}
}

func TestExtractTimeStampFromName(t *testing.T) {
	trash := &Trash{}
	now := time.Now().Unix()
	name := "Expired_" + time.Unix(now, 0).Format(ExpiredTimeFormat)
	ts, err := trash.extractTimeStampFromName(name)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ts != now {
		t.Fatalf("timestamp mismatch want %d got %d", now, ts)
	}

	if _, err := trash.extractTimeStampFromName("bad_name"); err == nil {
		t.Fatalf("expect error for bad name")
	}
}

func TestTransferLongFileName(t *testing.T) {
	base := strings.Repeat("x", FileNameLengthMax+10)
	filePath := path.Join("/tmp", base)
	newName, oldName := transferLongFileName(filePath)

	if oldName != base {
		t.Fatalf("old name want %s got %s", base, oldName)
	}
	if !strings.HasPrefix(newName, "/tmp/"+LongNamePrefix) {
		t.Fatalf("new name should start with long name prefix, got %s", newName)
	}
	if !strings.Contains(newName, ParentDirPrefix) {
		t.Fatalf("new name should contain ParentDirPrefix, got %s", newName)
	}
}
