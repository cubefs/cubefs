// Package diff computes diffs for updating JSON objects. It uses a lightweight
// field-by-field diffing algorithm suitable for comparing graphql result
// objects, but perhaps not for other JSON objects.
//
// For each field, a diff represents an update as
// - a recursive diff, stored as an object.
// - a complex replacement of the original field's value, stored as the new value in a 1-element array.
// - a scalar replacement, stored as the raw new value.
// - a deletion of the old field, stored as a 0-element array.
//
// For example, consider the following objects old, new, and the resulting
// diff:
//
//    old = {
//      "name": "bob",
//      "address": {"state": "ca", "city": "sf"},
//      "age": 30
//    }
//    new = {
//      "name": "alice",
//      "address": {"state": "ca", "city": "oakland"},
//      "friends": ["bob", "charlie]
//    }
//    diff = {
//      "name": "alice",
//      "address": {"city": "oakland"},
//      "age": [],
//      "friends": [["bob", "charlie]]
//    }
//
// The diff updates the name field to "alice", stored as a scalar. The diff
// updates the address recursively, keeping the state, but changing the city
// to "oakland". The diff deletes the age field with a 0-element array, and
// add a new complex friends field, stored in 1-element array.
//
// Array diffs are represented as object diffs with an optional reordering
// field stored in the "$" property. This reordering field holds a compressed
// of indices, indicating for each value in the new array the location of the
// element in the previous array. This makes for small diffs of long arrays
// that contain some permuted objects.
//
// For example, consider the following arrays old, new, and the resulting
// reordering:
//
//   old = [0, 1, 2, 3]
//   new = [1, 2, 3, 4]
//   reordering = [1, 2, 3, -1]
//
// The reordering indicates that the first 3 elements of the new array can
// be found at positions 1, 2, 3 and of the old array. The fourth item in
// the new array was not found in the old array.
//
// To compress this reodering array, we replace runs of adjacent indices
// as a tuple [start, length], so that the compressed reordering becomes
//
//   reordering = [1, 2, 3, -1]
//   compressed = [[1, 3], -1]
//
// Finally, to identify complex objects in arrays, package diffs uses
// a special "__key" field in objects. This must be a comparable value
// that allows the algorithm to line up values in arrays. For example,
//
//   old = [
//     {"__key": 10, "name": "bob", "age": 20"},
//     {"__key": 13, "name": "alice"}
//   ]
//   new = [
//     {"__key": 13, "name": "alice"}
//     {"__key": 10, "name": "bob", "age": 23},
//   ]
//   diff = {
//		"$": [1, 0],
//      "1": {"age": 23}
//   }
//
// Here, the diff first switches the order of the elements in the array,
// using the __key field to identify the two objects, and then updates
// the "age" field in the second element of the array to 23.
package diff

import (
	"bytes"
	"fmt"
	"reflect"
)

var emptyArray = []interface{}{}

// markRemoved returns a 0-element JSON array to indicate a removed field.
func markRemoved() interface{} {
	return emptyArray
}

// StripKey recursively creates a new JSON object with all __key fields
// removed.
func StripKey(i interface{}) interface{} {
	switch i := i.(type) {
	case map[string]interface{}:
		r := make(map[string]interface{})
		for k, v := range i {
			if k == "__key" {
				continue
			}
			r[k] = StripKey(v)
		}
		return r
	case []interface{}:
		r := make([]interface{}, 0, len(i))
		for _, v := range i {
			r = append(r, StripKey(v))
		}
		return r
	default:
		return i
	}
}

// markReplaced returns either a scalar value or a 1-element array
// wrapping a complex value to indicate an updated field.
//
// markReplaced strips __key fields from any complex value.
func markReplaced(i interface{}) interface{} {
	switch i := i.(type) {
	case bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, string:
		// Pass through i that don't look like deltas.
		return i

	default:
		// Wrap all other values.
		return []interface{}{StripKey(i)}
	}
}

// diffMap computes a diff between two maps by comparing fields key-by-key.
func diffMap(old map[string]interface{}, newAny interface{}) interface{} {
	// Verify the type of new.
	new, ok := newAny.(map[string]interface{})
	if !ok {
		return markReplaced(newAny)
	}

	// Check if two map are identical by comparing their pointers, and
	// short-circuit if so.
	if reflect.ValueOf(old).Pointer() == reflect.ValueOf(new).Pointer() {
		return nil
	}

	// Assert that the __key fields, if present, are equal.
	if old["__key"] != new["__key"] {
		return markReplaced(new)
	}

	// Build the diff.
	d := make(map[string]interface{})

	// Handle deleted fields.
	for k := range old {
		if _, ok := new[k]; !ok {
			d[k] = markRemoved()
		}
	}

	// Handle changed fields.
	for k, newV := range new {
		if oldV, ok := old[k]; ok {
			if innerD := Diff(oldV, newV); innerD != nil {
				d[k] = innerD
			}
		} else {
			d[k] = newV
		}
	}

	// Check if the diff is empty.
	if len(d) == 0 {
		return nil
	}
	return d
}

// reoderKey returns the key to use for a
func reorderKey(i interface{}) interface{} {
	if i == nil {
		return i
	}
	if object, ok := i.(map[string]interface{}); ok {
		if key, ok := object["__key"]; ok {
			return key
		}
	}

	if reflect.TypeOf(i).Comparable() {
		return i
	}

	return nil
}

// computeReorderIndices returns an array containing the index in old of each
// item in new
//
// If an item in new is not present in old, the index is -1. Objects are
// identified using the __key field, if present. Otherwise, the values are used
// as map keys if they are comparable.
func computeReorderIndices(old, new []interface{}) []int {
	oldIndices := make(map[interface{}][]int)
	for i, item := range old {
		key := reorderKey(item)
		oldIndices[key] = append(oldIndices[key], i)
	}

	indices := make([]int, len(new))
	for i, item := range new {
		key := reorderKey(item)
		if index := oldIndices[key]; len(index) > 0 {
			indices[i] = index[0]
			oldIndices[key] = index[1:]
		} else {
			indices[i] = -1
		}
	}

	return indices
}

// compressReorderIndices compresses a set of indices
//
// Runs of incrementing non-negative consecutive values are represented by a
// two-element array [first, count].
func compressReorderIndices(indices []int) []interface{} {
	compressed := make([]interface{}, 0)
	i := 0
	for i < len(indices) {
		// j represents the end of the current run.
		j := i
		for j < len(indices) && indices[j] != -1 && indices[j]-indices[i] == j-i {
			// Increment j while the run continues.
			j++
		}

		if i == j {
			compressed = append(compressed, -1)
			i = j + 1
		} else if j-i == 1 {
			compressed = append(compressed, indices[i])
			i = j
		} else {
			compressed = append(compressed, [2]int{indices[i], j - i})
			i = j
		}
	}

	return compressed
}

// diffArray computes a diff between two arrays by first reordering the
// elements and then comparing elements one-by-one.
func diffArray(old []interface{}, newAny interface{}) interface{} {
	// Verify the type of new.
	new, ok := newAny.([]interface{})
	if !ok {
		return markReplaced(newAny)
	}

	// Check if two arrays are identical by comparing their pointers and length,
	// and short-circuit if so.
	if len(old) == len(new) && reflect.ValueOf(old).Pointer() == reflect.ValueOf(new).Pointer() {
		return nil
	}

	d := make(map[string]interface{})

	// Compute reorder indices.
	indices := computeReorderIndices(old, new)

	// Check if the reorder indices can be omitted.
	orderChanged := len(old) != len(indices)
	for i := range indices {
		if indices[i] != i {
			orderChanged = true
		}
	}
	if orderChanged {
		d["$"] = compressReorderIndices(indices)
	}

	// Compare the array elements.
	for i, newI := range new {
		var oldI interface{}
		if j := indices[i]; j != -1 {
			oldI = old[j]
		}
		if innerD := Diff(oldI, newI); innerD != nil {
			d[fmt.Sprint(i)] = innerD
		}
	}

	// Check if the diff is empty.
	if len(d) == 0 {
		return nil
	}
	return d
}

// Diff computes a diff between two JSON objects. See the package comment for
// details of the algorithm and the diff format.
//
// A nil diff indicates that the old and new objects are equal.
func Diff(old interface{}, new interface{}) interface{} {
	switch old := old.(type) {
	case map[string]interface{}:
		return diffMap(old, new)
	case []interface{}:
		return diffArray(old, new)
	case []uint8:
		if new, ok := new.([]uint8); ok && bytes.Equal(old, new) {
			return nil
		}
		return markReplaced(new)
	default:
		if old != new {
			return markReplaced(new)
		}
		return nil
	}
}
