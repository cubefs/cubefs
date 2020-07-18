package schemabuilder

import (
	"encoding"
	"fmt"
	"reflect"
	"time"

	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/internal"
)

// schemaBuilder is a struct for holding all the graph information for types as
// we build out graphql types for our graphql schema.  Resolved graphQL "types"
// are stored in the type map which we can use to see sections of the graph.
type schemaBuilder struct {
	types        map[reflect.Type]graphql.Type
	typeNames    map[string]reflect.Type
	objects      map[reflect.Type]*Object
	enumMappings map[reflect.Type]*EnumMapping
	typeCache    map[reflect.Type]cachedType // typeCache maps Go types to GraphQL datatypes
}

// EnumMapping is a representation of an enum that includes both the mapping and
// reverse mapping.
type EnumMapping struct {
	Map        map[string]interface{}
	ReverseMap map[interface{}]string
}

// cachedType is a container for GraphQL datatype and the list of its fields
type cachedType struct {
	argType *graphql.InputObject
	fields  map[string]argField
}

// getType is the "core" function of the GraphQL schema builder.  It takes in a
// reflect type and builds the appropriate graphQL "type".  This includes going
// through struct fields and attached object methods to generate the entire
// graphql graph of possible queries.  This function will be called recursively
// for types as we go through the graph.
func (sb *schemaBuilder) getType(nodeType reflect.Type) (graphql.Type, error) {
	// Support scalars and optional scalars. Scalars have precedence over structs
	// to have eg. time.Time function as a scalar.
	if typeName, values, ok := sb.getEnum(nodeType); ok {
		return &graphql.NonNull{Type: &graphql.Enum{Type: typeName, Values: values, ReverseMap: sb.enumMappings[nodeType].ReverseMap}}, nil
	}

	if typeName, ok := getScalar(nodeType); ok {
		return &graphql.NonNull{Type: &graphql.Scalar{Type: typeName}}, nil
	}
	if nodeType.Kind() == reflect.Ptr {
		if typeName, ok := getScalar(nodeType.Elem()); ok {
			return &graphql.Scalar{Type: typeName}, nil // XXX: prefix typ with "*"
		}
	}

	if nodeType.Implements(textMarshalerType) {
		return sb.getTextMarshalerType(nodeType)
	}

	// Structs
	if nodeType.Kind() == reflect.Struct {
		if err := sb.buildStruct(nodeType); err != nil {
			return nil, err
		}
		return &graphql.NonNull{Type: sb.types[nodeType]}, nil
	}
	if nodeType.Kind() == reflect.Ptr && nodeType.Elem().Kind() == reflect.Struct {
		if err := sb.buildStruct(nodeType.Elem()); err != nil {
			return nil, err
		}
		return sb.types[nodeType.Elem()], nil
	}

	switch nodeType.Kind() {
	case reflect.Slice:
		elementType, err := sb.getType(nodeType.Elem())
		if err != nil {
			return nil, err
		}

		// Wrap all slice elements in NonNull.
		if _, ok := elementType.(*graphql.NonNull); !ok {
			elementType = &graphql.NonNull{Type: elementType}
		}

		return &graphql.NonNull{Type: &graphql.List{Type: elementType}}, nil

	default:
		return nil, fmt.Errorf("bad type %s: should be a scalar, slice, or struct type", nodeType)
	}
}

// getTextMarshalerType returns a graphQL type that can be used to parse a
// encoding.TextMarshaler and convert it's value into a string in the graphQL
// response.
func (sb *schemaBuilder) getTextMarshalerType(typ reflect.Type) (graphql.Type, error) {
	scalar := &graphql.Scalar{
		Type: "string",
		Unwrapper: func(source interface{}) (interface{}, error) {
			i := reflect.ValueOf(source)
			if i.Kind() == reflect.Ptr && i.IsNil() {
				return "", nil
			}
			marshalVal, ok := i.Interface().(encoding.TextMarshaler)
			if !ok {
				return nil, fmt.Errorf("cannot convert field to text")
			}
			val, err := marshalVal.MarshalText()
			if err != nil {
				return nil, err
			}
			return string(val), nil
		},
	}
	if typ.Kind() == reflect.Ptr {
		return scalar, nil
	}
	return &graphql.NonNull{Type: scalar}, nil
}

// getEnum gets the Enum type information for the passed in reflect.Type by
// looking it up in our enum mappings.
func (sb *schemaBuilder) getEnum(typ reflect.Type) (string, []string, bool) {
	if sb.enumMappings[typ] != nil {
		var values []string
		for mapping := range sb.enumMappings[typ].Map {
			values = append(values, mapping)
		}
		return typ.Name(), values, true
	}
	return "", nil, false
}

// getScalar grabs the appropriate scalar graphql field type name for the passed
// in variable reflect type.
func getScalar(typ reflect.Type) (string, bool) {
	for match, name := range scalars {
		if internal.TypesIdenticalOrScalarAliases(match, typ) {
			return name, true
		}
	}
	return "", false
}

var scalars = map[reflect.Type]string{
	reflect.TypeOf(bool(false)): "bool",
	reflect.TypeOf(int(0)):      "int",
	reflect.TypeOf(int8(0)):     "int8",
	reflect.TypeOf(int16(0)):    "int16",
	reflect.TypeOf(int32(0)):    "int32",
	reflect.TypeOf(int64(0)):    "int64",
	reflect.TypeOf(uint(0)):     "uint",
	reflect.TypeOf(uint8(0)):    "uint8",
	reflect.TypeOf(uint16(0)):   "uint16",
	reflect.TypeOf(uint32(0)):   "uint32",
	reflect.TypeOf(uint64(0)):   "uint64",
	reflect.TypeOf(float32(0)):  "float32",
	reflect.TypeOf(float64(0)):  "float64",
	reflect.TypeOf(string("")):  "string",
	reflect.TypeOf(time.Time{}): "Time",
	reflect.TypeOf([]byte{}):    "bytes",
}
