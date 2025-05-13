package metanode

import (
	"bytes"
	"io/fs"
	"reflect"
	"testing"
)

func dentryDqual(d1, d2 *Dentry) bool {
	if d1.Name != d2.Name || d1.ParentId != d2.ParentId || d1.Inode != d2.Inode || d1.Type != d2.Type {
		return false
	}

	if d1.multiSnap == nil && d2.multiSnap == nil {
		return true
	}

	if d1.multiSnap.VerSeq == d2.multiSnap.VerSeq {
		return true
	}

	if len(d1.multiSnap.dentryList) != len(d2.multiSnap.dentryList) {
		return false
	}

	for i, dd1 := range d1.multiSnap.dentryList {
		dd2 := d2.multiSnap.dentryList[i]
		if !dentryDqual(dd1, dd2) {
			return false
		}
	}

	return true
}

func TestDentryMarshalCompitable(t *testing.T) {
	snap := NewDentrySnap(1024)
	snap.dentryList = append(snap.dentryList, &Dentry{
		Name:  "old_name",
		Inode: 1035,
	})

	d := &Dentry{
		ParentId:  1,
		Name:      "test",
		Inode:     102,
		Type:      uint32(fs.ModeDir),
		multiSnap: snap,
	}

	// data is dentry d marshald byte by version 3.5.0
	data := []byte{0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 1, 116, 101, 115, 116, 0, 0, 0, 44, 0, 0, 0, 0, 0, 0, 0, 102, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 4, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	d2 := &Dentry{}
	err := d2.Unmarshal(data)
	if err != nil {
		t.Fail()
	}

	if !dentryDqual(d, d2) {
		t.Fail()
	}
}

func TestDentryMarshal(t *testing.T) {
	d := &Dentry{
		ParentId:  1,
		Name:      "test",
		Type:      uint32(fs.ModeDir),
		Inode:     1024,
		multiSnap: NewDentrySnap(1024),
	}

	buf1 := GetDentryBuf()
	defer PutDentryBuf(buf1)

	err := d.MarshalV2(buf1)
	if err != nil {
		panic(err)
	}

	data2, err := d.Marshal()
	if err != nil || !bytes.Equal(buf1.Bytes(), data2) {
		t.Fail()
	}

	d3 := &Dentry{}
	err = d3.Unmarshal(buf1.Bytes())
	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(d, d3) {
		t.Fail()
	}
}

func TestDentryMarshalValue(t *testing.T) {
	d := &Dentry{
		ParentId:  1,
		Name:      "test",
		Type:      uint32(fs.ModeDir),
		Inode:     1024,
		multiSnap: NewDentrySnap(1024),
	}

	buf1 := GetDentryBuf()
	defer PutDentryBuf(buf1)

	// marshalValue & marshalValueV2
	d.MarshalValueV2(buf1)
	data1 := buf1.Bytes()

	data2 := d.MarshalValue()
	if !bytes.Equal(data1, data2) {
		t.Fail()
	}

	// marshalKey & marshalKeyV2
	buf2 := GetDentryBuf()
	defer PutDentryBuf(buf2)

	d.MarshalKeyV2(buf2)
	data1 = buf2.Bytes()

	data2 = d.MarshalKey()
	if !bytes.Equal(data1, data2) {
		t.Fail()
	}
}

func BenchmarkDentryMarshal(b *testing.B) {
	d := &Dentry{
		ParentId: 1,
		Name:     "test",
		Type:     uint32(fs.ModeDir),
		Inode:    1024,
	}

	buf := GetDentryBuf()
	defer PutDentryBuf(buf)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.MarshalValueV2(buf)
	}
}

func BenchmarkDentryUnMarshal(b *testing.B) {
	d := &Dentry{
		ParentId: 1,
		Name:     "test",
		Type:     uint32(fs.ModeDir),
		Inode:    1024,
	}

	buf := GetDentryBuf()
	d.MarshalValueV2(buf)
	PutDentryBuf(buf)

	data := buf.Bytes()
	d2 := &Dentry{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d2.Unmarshal(data)
	}
}
