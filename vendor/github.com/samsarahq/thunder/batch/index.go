package batch

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
