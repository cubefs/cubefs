package schemabuilder

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/samsarahq/thunder/graphql"
)

// buildStruct is a function for building the graphql.Type for a passed in
// struct type.  This function reads the "Object" information and Fields of the
// passed in struct to create a "graphql.Object" type that represents this type
// and can be used to resolve graphql requests.
func (sb *schemaBuilder) buildStruct(typ reflect.Type) error {
	if sb.types[typ] != nil {
		return nil
	}

	if typ == unionType {
		return fmt.Errorf("schemabuilder.Union can only be used as an embedded anonymous non-pointer struct")
	}

	if hasUnionMarkerEmbedded(typ) {
		return sb.buildUnionStruct(typ)
	}

	var name string
	var description string
	var methods Methods
	var objectKey string
	if object, ok := sb.objects[typ]; ok {
		name = object.Name
		description = object.Description
		methods = object.Methods
		objectKey = object.key
	}

	if name == "" {
		name = typ.Name()
		if name == "" {
			return fmt.Errorf("bad type %s: should have a name", typ)
		}
		if originalType, ok := sb.typeNames[name]; ok {
			return fmt.Errorf("duplicate name %s: seen both %v and %v", name, originalType, typ)
		}
	}

	object := &graphql.Object{
		Name:        name,
		Description: description,
		Fields:      make(map[string]*graphql.Field),
	}
	sb.types[typ] = object
	sb.typeNames[name] = typ

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldInfo, err := parseGraphQLFieldInfo(field)
		if err != nil {
			return fmt.Errorf("bad type %s: %s", typ, fieldInfo.Name)
		}
		if fieldInfo.Skipped {
			continue
		}

		if _, ok := object.Fields[fieldInfo.Name]; ok {
			return fmt.Errorf("bad type %s: two fields named %s", typ, fieldInfo.Name)
		}

		built, err := sb.buildField(field)
		if err != nil {
			return fmt.Errorf("bad field %s on type %s: %s", fieldInfo.Name, typ, err)
		}
		object.Fields[fieldInfo.Name] = built
		if fieldInfo.KeyField {
			if object.KeyField != nil {
				return fmt.Errorf("bad type %s: multiple key fields", typ)
			}
			if !isScalarType(built.Type) {
				return fmt.Errorf("bad type %s: key type must be scalar, got %T", typ, built.Type)
			}
			object.KeyField = built
		}
	}

	var names []string
	for name := range methods {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		method := methods[name]

		if method.Batch {
			if method.BatchArgs.FallbackFunc != nil {
				batchField, err := sb.buildBatchFunctionWithFallback(typ, method)
				if err != nil {
					return err
				}
				object.Fields[name] = batchField
				continue
			}
			batchField, err := sb.buildBatchFunction(typ, method)
			if err != nil {
				return err
			}
			object.Fields[name] = batchField
			continue
		}

		if method.Paginated {
			if method.ManualPaginationArgs.FallbackFunc != nil {
				typedField, err := sb.buildPaginatedFieldWithFallback(typ, method)
				if err != nil {
					return err
				}
				object.Fields[name] = typedField
				continue
			}
			typedField, err := sb.buildPaginatedField(typ, method)
			if err != nil {
				return err
			}
			object.Fields[name] = typedField
			continue
		}

		built, err := sb.buildFunction(typ, method)
		if err != nil {
			return fmt.Errorf("bad method %s on type %s: %s", name, typ, err)
		}
		object.Fields[name] = built
	}

	if objectKey != "" {
		keyPtr, ok := object.Fields[objectKey]
		if !ok {
			return fmt.Errorf("key field doesn't exist on object")
		}

		if !isScalarType(keyPtr.Type) {
			return fmt.Errorf("bad type %s: key type must be scalar, got %s", typ, keyPtr.Type.String())
		}
		object.KeyField = keyPtr
	}

	return nil
}

// hasUnionMarkerEmbedded determines if a struct has an embedded schemabuilder.Union
// field embedded on the type.
func hasUnionMarkerEmbedded(typ reflect.Type) bool {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Anonymous && field.Type == unionType {
			return true
		}
	}
	return false
}

// buildUnionStruct builds a graphql.Union type that has the ability to be one
// of many different graphql types.
func (sb *schemaBuilder) buildUnionStruct(typ reflect.Type) error {
	var name string
	var description string

	if name == "" {
		name = typ.Name()
		if name == "" {
			return fmt.Errorf("bad type %s: should have a name", typ)
		}
		if originalType, ok := sb.typeNames[name]; ok {
			return fmt.Errorf("duplicate name %s: seen both %v and %v", name, originalType, typ)
		}
	}

	union := &graphql.Union{
		Name:        name,
		Description: description,
		Types:       make(map[string]*graphql.Object),
	}
	sb.types[typ] = union
	sb.typeNames[name] = typ

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" || (field.Anonymous && field.Type == unionType) {
			continue
		}

		if !field.Anonymous {
			return fmt.Errorf("bad type %s: union type member types must be anonymous", name)
		}

		typ, err := sb.getType(field.Type)
		if err != nil {
			return err
		}

		obj, ok := typ.(*graphql.Object)
		if !ok {
			return fmt.Errorf("bad type %s: union type member must be a pointer to a struct, received %s", name, typ.String())
		}

		if union.Types[obj.Name] != nil {
			return fmt.Errorf("bad type %s: union type member may only appear once", name)
		}

		union.Types[obj.Name] = obj
	}
	return nil
}

// isScalarType returns whether a graphql.Type is a scalar type (or a non-null
// wrapped scalar type).
func isScalarType(typ graphql.Type) bool {
	if nonNull, ok := typ.(*graphql.NonNull); ok {
		typ = nonNull.Type
	}
	if _, ok := typ.(*graphql.Scalar); !ok {
		return false
	}
	return true
}

// buildField generates a graphQL field for a struct's field.  This field can be
// used to "resolve" a response for a graphql request.
func (sb *schemaBuilder) buildField(field reflect.StructField) (*graphql.Field, error) {
	retType, err := sb.getType(field.Type)
	if err != nil {
		return nil, err
	}

	return &graphql.Field{
		Resolve: func(ctx context.Context, source, args interface{}, selectionSet *graphql.SelectionSet) (interface{}, error) {
			value := reflect.ValueOf(source)
			if value.Kind() == reflect.Ptr {
				value = value.Elem()
			}
			return value.FieldByIndex(field.Index).Interface(), nil
		},
		Type:           retType,
		ParseArguments: nilParseArguments,
	}, nil
}
