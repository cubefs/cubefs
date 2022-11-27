package batch

import "encoding/json"

// Index is a "key" mapper for batch requests.  Its key is not special and
// should be treated the same as an index in a slice.
type Index struct {
	key int
}

// NewIndex creates an index.  This should only be used by
// generated code or the creator of batches,  The "Key" has no purpose and should
// not be used elsewhere.
func NewIndex(key int) Index {
	// TODO generate a hash to really mess with people trying to recreate batches?
	return Index{key: key}
}

// Create another type for Index so that MarshalText and UnmarshalText
// don't run into recursion issues.
type index Index

func (i Index) MarshalText() (text []byte, err error) {
	return json.Marshal(index(i))
}

func (i *Index) UnmarshalText(text []byte) error {
	return json.Unmarshal(text, (*index)(i))
}