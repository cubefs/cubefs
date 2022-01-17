package metanode

import (
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
	dentrgKV.UnmarshalWithKeyAndValue(d.MarshalKey(), d.MarshalValue())
	if reflect.DeepEqual(d, dentryRestore) && reflect.DeepEqual(d, dentrgKV){
		t.Logf("dentryMaral---->dentryUnmarshalV2: success")
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
		t.Logf("dentryMaralV2---->dentryUnmarshalV1: success")
	}else{
		t.Errorf("error: len:%d src=[%v] res=[%v] kv=[%v]\n",len(raw), d, dentryRestore, dentrgKV)
	}
}
