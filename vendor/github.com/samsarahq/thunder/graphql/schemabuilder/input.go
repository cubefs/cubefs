package schemabuilder

import (
	"encoding"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/internal"
)

type dualArgParser struct {
	argParser         func(interface{}) (interface{}, error)
	fallbackArgParser func(interface{}) (interface{}, error)
}

type dualArgResponses struct {
	argValue         interface{}
	fallbackArgValue interface{}
}

func (p *dualArgParser) Parse(args interface{}) (interface{}, error) {
	v1, err := p.argParser(args)
	if err != nil {
		return nil, err
	}
	v2, err := p.fallbackArgParser(args)
	if err != nil {
		return nil, err
	}
	return dualArgResponses{
		argValue:         v1,
		fallbackArgValue: v2,
	}, nil
}

// argField is a representation of an input parameter field for a function.  It
// must be a field on a struct and will have an associated "argParser" for
// reading an input JSON and filling the struct field.
type argField struct {
	field  reflect.StructField
	parser *argParser
}

// argParser is a struct that holds information for how to deserialize a JSON
// input into a particular go variable.
type argParser struct {
	FromJSON func(interface{}, reflect.Value) error
	Type     reflect.Type
}

// Parse is a convenience function that takes in JSON args and writes them into
// a new variable type for the argParser.
func (p *argParser) Parse(args interface{}) (interface{}, error) {
	if p == nil {
		return nilParseArguments(args)
	}
	parsed := reflect.New(p.Type).Elem()
	if err := p.FromJSON(args, parsed); err != nil {
		return nil, err
	}
	return parsed.Interface(), nil
}

// nilParseArguments is a default function for parsing args.  It expects to be
// called with nothing, and will return an error if called with non-empty args.
func nilParseArguments(args interface{}) (interface{}, error) {
	if args == nil {
		return nil, nil
	}
	if args, ok := args.(map[string]interface{}); !ok || len(args) != 0 {
		return nil, graphql.NewSafeError("unexpected args")
	}
	return nil, nil
}

// makeStructParser constructs an argParser for the passed in struct type.
func (sb *schemaBuilder) makeStructParser(typ reflect.Type) (*argParser, graphql.Type, error) {
	argType, fields, err := sb.getStructObjectFields(typ)
	if err != nil {
		return nil, nil, err
	}

	return &argParser{
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asMap, ok := value.(map[string]interface{})
			if !ok {
				return errors.New("not an object")
			}

			for name, field := range fields {
				value := asMap[name]
				fieldDest := dest.FieldByIndex(field.field.Index)
				if err := field.parser.FromJSON(value, fieldDest); err != nil {
					return fmt.Errorf("%s: %s", name, err)
				}
			}

			return nil
		},
		Type: typ,
	}, argType, nil
}

// getStructObjectFields loops through a struct's fields and builds argParsers
// for all the struct's subfields.  These fields will then be used when we want
// to create an instance of the original struct from JSON.
func (sb *schemaBuilder) getStructObjectFields(typ reflect.Type) (*graphql.InputObject, map[string]argField, error) {
	// Check if the struct type is already cached
	if cached, ok := sb.typeCache[typ]; ok {
		return cached.argType, cached.fields, nil
	}

	fields := make(map[string]argField)
	argType := &graphql.InputObject{
		Name:        typ.Name(),
		InputFields: make(map[string]graphql.Type),
	}
	if argType.Name != "" {
		argType.Name += "_InputObject"
	}

	if typ.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("expected struct but received type %s", typ.Kind())
	}

	// Cache type information ahead of time to catch self-reference
	sb.typeCache[typ] = cachedType{argType, fields}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Anonymous {
			return nil, nil, fmt.Errorf("bad arg type %s: anonymous fields not supported", typ)
		}

		fieldInfo, err := parseGraphQLFieldInfo(field)
		if err != nil {
			return nil, nil, fmt.Errorf("bad type %s: %s", typ, err.Error())
		}
		if fieldInfo.Skipped {
			continue
		}

		if _, ok := fields[fieldInfo.Name]; ok {
			return nil, nil, fmt.Errorf("bad arg type %s: duplicate field %s", typ, fieldInfo.Name)
		}
		parser, fieldArgTyp, err := sb.makeArgParser(field.Type)
		if err != nil {
			return nil, nil, err
		}
		if fieldInfo.OptionalInputField {
			parser, fieldArgTyp = wrapWithZeroValue(parser, fieldArgTyp)
		}

		fields[fieldInfo.Name] = argField{
			field:  field,
			parser: parser,
		}
		argType.InputFields[fieldInfo.Name] = fieldArgTyp
	}

	return argType, fields, nil
}

// makeArgParser reads the information on a passed in variable type and returns
// an ArgParser that can be used to "fill" that type from a GraphQL JSON input.
func (sb *schemaBuilder) makeArgParser(typ reflect.Type) (*argParser, graphql.Type, error) {
	if typ.Kind() == reflect.Ptr {
		parser, argType, err := sb.makeArgParserInner(typ.Elem())
		if err != nil {
			return nil, nil, err
		}
		return wrapPtrParser(parser), argType, nil
	}

	parser, argType, err := sb.makeArgParserInner(typ)
	if err != nil {
		return nil, nil, err
	}
	return parser, &graphql.NonNull{Type: argType}, nil
}

// makeArgParserInner is a helper function for makeArgParser that doesn't need
// to worry about pointer types.
func (sb *schemaBuilder) makeArgParserInner(typ reflect.Type) (*argParser, graphql.Type, error) {
	if sb.enumMappings[typ] != nil {
		parser, argType := sb.getEnumArgParser(typ)
		return parser, argType, nil
	}

	if parser, argType, ok := getScalarArgParser(typ); ok {
		return parser, argType, nil
	}

	if reflect.PtrTo(typ).Implements(textUnmarshalerType) {
		return sb.makeTextUnmarshalerParser(typ)
	}

	switch typ.Kind() {
	case reflect.Struct:
		parser, argType, err := sb.makeStructParser(typ)
		if err != nil {
			return nil, nil, err
		}
		if argType.(*graphql.InputObject).Name == "" {
			return nil, nil, fmt.Errorf("bad type %s: should have a name", typ)
		}
		return parser, argType, nil
	case reflect.Slice:
		return sb.makeSliceParser(typ)
	default:
		return nil, nil, fmt.Errorf("bad arg type %s: should be struct, scalar, pointer, or a slice", typ)
	}
}

// wrapPtrParser wraps an ArgParser with a helper that will convert the parsed
// type into a pointer type.
func wrapPtrParser(inner *argParser) *argParser {
	return &argParser{
		FromJSON: func(value interface{}, dest reflect.Value) error {
			if value == nil {
				// optional value
				return nil
			}

			ptr := reflect.New(inner.Type)
			if err := inner.FromJSON(value, ptr.Elem()); err != nil {
				return err
			}
			dest.Set(ptr)
			return nil
		},
		Type: reflect.PtrTo(inner.Type),
	}
}

// wrapWithZeroValue wraps an ArgParser with a helper that will convert non-
// provided parameters into the argParser's zero value (basically do nothing).
func wrapWithZeroValue(inner *argParser, fieldArgTyp graphql.Type) (*argParser, graphql.Type) {
	// Make sure the "fieldArgType" we expose in graphQL is a Nullable field.
	if f, ok := fieldArgTyp.(*graphql.NonNull); ok {
		fieldArgTyp = f.Type
	}
	return &argParser{
		FromJSON: func(value interface{}, dest reflect.Value) error {
			if value == nil {
				// optional value
				return nil
			}

			return inner.FromJSON(value, dest)
		},
		Type: inner.Type,
	}, fieldArgTyp
}

// getEnumArgParser creates an arg parser for an Enum type.
func (sb *schemaBuilder) getEnumArgParser(typ reflect.Type) (*argParser, graphql.Type) {
	var values []string
	for mapping := range sb.enumMappings[typ].Map {
		values = append(values, mapping)
	}
	return &argParser{FromJSON: func(value interface{}, dest reflect.Value) error {
		asString, ok := value.(string)
		if !ok {
			return errors.New("not a string")
		}
		val, ok := sb.enumMappings[typ].Map[asString]
		if !ok {
			return fmt.Errorf("unknown enum value %v", asString)
		}
		dest.Set(reflect.ValueOf(val).Convert(dest.Type()))
		return nil
	}, Type: typ}, &graphql.Enum{Type: typ.Name(), Values: values, ReverseMap: sb.enumMappings[typ].ReverseMap}

}

// makeTextUnmarshalerParser returns an argParser that will read the passed in
// value as a string and insert it into the destination type using the
// encoding.TextUnmarshaler API.
func (sb *schemaBuilder) makeTextUnmarshalerParser(typ reflect.Type) (*argParser, graphql.Type, error) {
	return &argParser{
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asString, ok := value.(string)
			if !ok {
				return errors.New("not a string")
			}
			if !dest.CanAddr() {
				return errors.New("destination type cannot be referenced")
			}
			unmarshalable, ok := dest.Addr().Interface().(encoding.TextUnmarshaler)
			if !ok {
				return errors.New("not unmarshalable")
			}
			return unmarshalable.UnmarshalText([]byte(asString))
		},
		Type: typ,
	}, &graphql.Scalar{Type: "string"}, nil
}

// makeSliceParser creates an arg parser for a slice field.
func (sb *schemaBuilder) makeSliceParser(typ reflect.Type) (*argParser, graphql.Type, error) {
	inner, argType, err := sb.makeArgParser(typ.Elem())
	if err != nil {
		return nil, nil, err
	}

	return &argParser{
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asSlice, ok := value.([]interface{})
			if !ok {
				return errors.New("not a list")
			}

			dest.Set(reflect.MakeSlice(typ, len(asSlice), len(asSlice)))

			for i, value := range asSlice {
				if err := inner.FromJSON(value, dest.Index(i)); err != nil {
					return err
				}
			}

			return nil
		},
		Type: typ,
	}, &graphql.List{Type: argType}, nil
}

// getScalarArgParser creates an arg parser for a scalar type.
func getScalarArgParser(typ reflect.Type) (*argParser, graphql.Type, bool) {
	for match, argParser := range scalarArgParsers {
		if internal.TypesIdenticalOrScalarAliases(match, typ) {
			name, ok := getScalar(typ)
			if !ok {
				panic(typ)
			}

			if typ != argParser.Type {
				// The scalar may be a type alias here,
				// so we annotate the parser to output the
				// alias instead of the underlying type.
				newParser := *argParser
				newParser.Type = typ
				argParser = &newParser
			}

			return argParser, &graphql.Scalar{Type: name}, true
		}
	}
	return nil, nil, false
}

// scalarArgParsers are the static arg parsers that we can use for all scalar &
// static types.
var scalarArgParsers = map[reflect.Type]*argParser{
	reflect.TypeOf(bool(false)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asBool, ok := value.(bool)
			if !ok {
				return errors.New("not a bool")
			}
			dest.Set(reflect.ValueOf(asBool).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(float64(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(asFloat).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(float32(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(float32(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(int64(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(int64(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(int32(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(int32(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(int16(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(int16(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(int8(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(int8(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(int(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(int(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(uint64(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(int64(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(uint32(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(uint32(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(uint16(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(uint16(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(uint8(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(uint8(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(uint(0)): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asFloat, ok := value.(float64)
			if !ok {
				return errors.New("not a number")
			}
			dest.Set(reflect.ValueOf(uint(asFloat)).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(string("")): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asString, ok := value.(string)
			if !ok {
				return errors.New("not a string")
			}
			dest.Set(reflect.ValueOf(asString).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf([]byte{}): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asString, ok := value.(string)
			if !ok {
				return errors.New("not a string")
			}
			bytes, err := base64.StdEncoding.DecodeString(asString)
			if err != nil {
				return err
			}
			dest.Set(reflect.ValueOf(bytes).Convert(dest.Type()))
			return nil
		},
	},
	reflect.TypeOf(time.Time{}): {
		FromJSON: func(value interface{}, dest reflect.Value) error {
			asString, ok := value.(string)
			if !ok {
				return errors.New("not a string")
			}
			asTime, err := time.Parse(time.RFC3339, asString)
			if err != nil {
				return errors.New("not an iso8601 time")
			}
			dest.Set(reflect.ValueOf(asTime).Convert(dest.Type()))
			return nil
		},
	},
}

func init() {
	// When loading the scalarArgParsers we need to fill the "Type" fields
	// appropriately.
	for typ, arg := range scalarArgParsers {
		arg.Type = typ
	}
}
