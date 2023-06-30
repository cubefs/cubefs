package metanode

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestDentry_V1Marshal(t *testing.T){
	d := &Dentry{
		ParentId: 123,
		Name:     "dentry_test",
		Inode:    16797219,
		Type:    2147484141,
	}
	raw,_ :=d.Marshal()
	dentryRestore := &Dentry{}
	dentryRestore.UnmarshalV2(raw)
	dentrgKV := &Dentry{}
	dentrgKV.UnmarshalV2WithKeyAndValue(d.MarshalKey(), d.MarshalValue())
	if reflect.DeepEqual(d, dentryRestore) && reflect.DeepEqual(d, dentrgKV){
		t.Logf("dentryMarshal---->dentryUnmarshalV2: success")
	}else{
		t.Errorf("error: len:%d src=[%v] res=[%v] kv=[%v]\n",len(raw), d, dentryRestore, dentrgKV)
	}

}

func TestDentry_V2Marshal(t *testing.T){
	d := &Dentry{
		ParentId: 123,
		Name:     "dentry_test",
		Inode:    16797219,
		Type:     2147484141,
	}
	raw,_ :=d.MarshalV2()
	dentryRestore := &Dentry{}
	dentryRestore.Unmarshal(raw)
	dentrgKV := &Dentry{}
	dentrgKV.UnmarshalKey(raw[DentryKeyOffset : DentryKeyOffset + d.DentryKeyLen()])
	dentrgKV.UnmarshalValue(raw[len(raw) - DentryValueLen : ])
	if reflect.DeepEqual(d, dentryRestore) && reflect.DeepEqual(d, dentrgKV){
		t.Logf("dentryMarshalV2---->dentryUnmarshalV1: success")
	}else{
		t.Errorf("error: len:%d src=[%v] res=[%v] kv=[%v]\n",len(raw), d, dentryRestore, dentrgKV)
	}
}

func TestDentry_EncodeBinary(t *testing.T) {
	expectDentry := &Dentry{
		ParentId: 123,
		Name:     "dentry_test",
		Inode:    16797219,
		Type:     2147484141,
	}
	data := make([]byte, expectDentry.BinaryDataLen())
	_, _ = expectDentry.EncodeBinary(data)

	dentry := new(Dentry)
	if err := dentry.Unmarshal(data); err != nil {
		t.Errorf("unmarshal failed:%v", err)
		return
	}
	assert.Equal(t, expectDentry, dentry)
}