package internal

import "reflect"

func IsScalarType(t reflect.Type) bool {
	switch t.Kind() {
	case
		reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.String:
		return true

	default:
		return false
	}
}

func TypesIdenticalOrScalarAliases(a, b reflect.Type) bool {
	return a == b || (a.Kind() == b.Kind() && IsScalarType(a))
}

// interfaceTyp is the reflect.Type of interface{}
var interfaceTyp reflect.Type

func init() {
	var x interface{}
	interfaceTyp = reflect.TypeOf(&x).Elem()
}

// MakeHashable converts a []interface{} slice into an equivalent fixed-length array
// [...]interface{} for use as a comparable map key
func MakeHashable(s []interface{}) interface{} {
	d := make([]interface{}, len(s))
	// Convert byte slices into strings as they are otherwise not comparable/hashable.
	for i, elem := range s {
		if b, ok := elem.([]byte); ok {
			d[i] = string(b)
		} else {
			d[i] = elem
		}
	}

	// Return arrays as they are comparable/hashable.
	switch len(d) {
	// fast code paths for short arrays:
	case 0:
		return [...]interface{}{}
	case 1:
		return [...]interface{}{d[0]}
	case 2:
		return [...]interface{}{d[0], d[1]}
	case 3:
		return [...]interface{}{d[0], d[1], d[2]}
	case 4:
		return [...]interface{}{d[0], d[1], d[2], d[3]}
	case 5:
		return [...]interface{}{d[0], d[1], d[2], d[3], d[4]}
	case 6:
		return [...]interface{}{d[0], d[1], d[2], d[3], d[4], d[5]}
	case 7:
		return [...]interface{}{d[0], d[1], d[2], d[3], d[4], d[5], d[6]}
	case 8:
		return [...]interface{}{d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7]}
	default:
		// slow catch-all:
		array := reflect.New(reflect.ArrayOf(len(d), interfaceTyp)).Elem()
		for i, elem := range d {
			array.Index(i).Set(reflect.ValueOf(elem))
		}
		return array.Interface()
	}
}
