package schemabuilder

import (
	"bytes"
	"context"
	"encoding"
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/samsarahq/thunder/graphql"
)

// makeGraphql converts a field name "MyField" into a graphQL field name "myField".
func makeGraphql(s string) string {
	var b bytes.Buffer
	for i, c := range s {
		if i == 0 {
			b.WriteRune(unicode.ToLower(c))
		} else {
			b.WriteRune(c)
		}
	}
	return b.String()
}

// reverseGraphqlFieldName converts a graphql field name "myField" into a
// non-graphQL field name "MyField".
func reverseGraphqlFieldName(s string) string {
	var b bytes.Buffer
	for i, c := range s {
		if i == 0 {
			b.WriteRune(unicode.ToUpper(c))
		} else {
			b.WriteRune(c)
		}
	}
	return b.String()
}

// graphQLFieldInfo contains basic struct field information related to GraphQL.
type graphQLFieldInfo struct {
	// Skipped indicates that this field should not be included in GraphQL.
	Skipped bool

	// Name is the GraphQL field name that should be exposed for this field.
	Name string

	// KeyField indicates that this field should be treated as a Object Key field.
	KeyField bool

	// OptionalInputField indicates that this field should be treated as an optional
	// field on graphQL input args.
	OptionalInputField bool
}

// parseGraphQLFieldInfo parses a struct field and returns a struct with the
// parsed information about the field (tag info, name, etc).
func parseGraphQLFieldInfo(field reflect.StructField) (*graphQLFieldInfo, error) {
	if field.PkgPath != "" {
		return &graphQLFieldInfo{Skipped: true}, nil
	}
	tags := strings.Split(field.Tag.Get("graphql"), ",")
	var name string
	if len(tags) > 0 {
		name = tags[0]
	}
	if name == "" {
		name = makeGraphql(field.Name)
	}
	if name == "-" {
		return &graphQLFieldInfo{Skipped: true}, nil
	}

	var key bool
	var optional bool

	if len(tags) > 1 {
		for _, tag := range tags[1:] {
			if tag == "key" && !key {
				key = true
			} else if tag == "optional" && !optional {
				optional = true
			} else {
				return nil, fmt.Errorf("field %s has unexpected tag %s", name, tag)
			}
		}
	}
	return &graphQLFieldInfo{Name: name, KeyField: key, OptionalInputField: optional}, nil
}

// Common Types that we will need to perform type assertions against.
var errType = reflect.TypeOf((*error)(nil)).Elem()
var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
var selectionSetType = reflect.TypeOf(&graphql.SelectionSet{})
var textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
var textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
