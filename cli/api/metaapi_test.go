package api

import (
	"encoding/json"
	"github.com/chubaofs/chubaofs/metanode"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestMetaHttpClient_GetAllInodes(t *testing.T) {
	inodeMap := make(map[uint64]*metanode.Inode, 0)

	reader := strings.NewReader(`
[{"Inode":33554435,"Type":420,"Uid":0,"Gid":0,"Size":4,"Generation":2,"CreateTime":1606102532,"AccessTime":1606102792,"ModifyTime":1606102532,"LinkTarget":null,"NLink":1,"Flag":0,"Reserved":0,"Extents":{}},
{"Inode":33554436,"Type":420,"Uid":0,"Gid":0,"Size":4,"Generation":2,"CreateTime":1606102537,"AccessTime":1606102792,"ModifyTime":1606102537,"LinkTarget":null,"NLink":1,"Flag":0,"Reserved":0,"Extents":{}},
{"Inode":33554437,"Type":420,"Uid":0,"Gid":0,"Size":4,"Generation":2,"CreateTime":1606102538,"AccessTime":1606102792,"ModifyTime":1606102538,"LinkTarget":null,"NLink":1,"Flag":0,"Reserved":0,"Extents":{}}]
`)

	dec := json.NewDecoder(reader)
	dec.UseNumber()

	// It's the "items". We expect it to be an array
	if err := parseToken(dec, '['); err != nil {
		t.Fatal(err)
	}

	// Read items (large objects)
	for dec.More() {
		// Read next item (large object)
		in := &metanode.Inode{}
		if err := dec.Decode(in); err != nil {
			t.Fatal(err)
		}
		inodeMap[in.Inode] = in
	}
	// Array closing delimiter
	if err := parseToken(dec, ']'); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(inodeMap), 3)
}
