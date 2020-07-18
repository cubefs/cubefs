package graphql

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"runtime"
)

type pathError struct {
	inner error
	path  []string
}

func nestPathErrorMulti(path []string, err error) error {
	// Don't nest SanitzedError's, as they are intended for human consumption.
	if se, ok := err.(SanitizedError); ok {
		return se
	}

	if pe, ok := err.(*pathError); ok {
		return &pathError{
			inner: pe.inner,
			path:  append(pe.path, path...),
		}
	}

	return &pathError{
		inner: err,
		path:  path,
	}
}

func nestPathError(key string, err error) error {
	// Don't nest SanitzedError's, as they are intended for human consumption.
	if se, ok := err.(SanitizedError); ok {
		return se
	}

	if pe, ok := err.(*pathError); ok {
		return &pathError{
			inner: pe.inner,
			path:  append(pe.path, key),
		}
	}

	return &pathError{
		inner: err,
		path:  []string{key},
	}
}

func ErrorCause(err error) error {
	if pe, ok := err.(*pathError); ok {
		return pe.inner
	}
	return err
}

func (pe *pathError) Unwrap() error {
	return pe.inner
}

func (pe *pathError) Error() string {
	var buffer bytes.Buffer
	writePath(pe, &buffer)
	buffer.WriteString(": ")
	buffer.WriteString(pe.inner.Error())
	return buffer.String()
}

func (pe *pathError) Reason() string {
	var buffer bytes.Buffer
	writePath(pe, &buffer)
	return buffer.String()
}

// Writes path from pe into buffer
func writePath(pe *pathError, buffer *bytes.Buffer) {
	for i := len(pe.path) - 1; i >= 0; i-- {
		if i < len(pe.path)-1 {
			buffer.WriteString(".")
		}
		buffer.WriteString(pe.path[i])
	}
}

func isNilArgs(args interface{}) bool {
	m, ok := args.(map[string]interface{})
	return args == nil || (ok && len(m) == 0)
}

// unwrap will return the value associated with a pointer type, or nil if the
// pointer is nil
func unwrap(v interface{}) interface{} {
	i := reflect.ValueOf(v)
	for i.Kind() == reflect.Ptr && !i.IsNil() {
		i = i.Elem()
	}
	if i.Kind() == reflect.Invalid {
		return nil
	}
	return i.Interface()
}

// PrepareQuery checks that the given selectionSet matches the schema typ, and
// parses the args in selectionSet
func PrepareQuery(ctx context.Context, typ Type, selectionSet *SelectionSet) error {
	switch typ := typ.(type) {
	case *Scalar:
		if selectionSet != nil {
			return NewClientError("scalar field must have no selections")
		}
		return nil
	case *Enum:
		if selectionSet != nil {
			return NewClientError("enum field must have no selections")
		}
		return nil
	case *Union:
		if selectionSet == nil {
			return NewClientError("object field must have selections")
		}

		for _, fragment := range selectionSet.Fragments {
			for typString, graphqlTyp := range typ.Types {
				if fragment.On != typString {
					continue
				}
				if err := PrepareQuery(ctx, graphqlTyp, fragment.SelectionSet); err != nil {
					return err
				}
			}
		}
		for _, selection := range selectionSet.Selections {
			if selection.Name == "__typename" {
				if !isNilArgs(selection.UnparsedArgs) {
					return NewClientError(`error parsing args for "__typename": no args expected`)
				}
				if selection.SelectionSet != nil {
					return NewClientError(`scalar field "__typename" must have no selection`)
				}
				for _, fragment := range selectionSet.Fragments {
					fragment.SelectionSet.Selections = append(fragment.SelectionSet.Selections, selection)
				}
				continue
			}
			return NewClientError(`unknown field "%s"`, selection.Name)
		}
		return nil
	case *Object:
		if selectionSet == nil {
			return NewClientError("object field must have selections")
		}
		for _, selection := range selectionSet.Selections {
			if selection.Name == "__typename" {
				if !isNilArgs(selection.UnparsedArgs) {
					return NewClientError(`error parsing args for "__typename": no args expected`)
				}
				if selection.SelectionSet != nil {
					return NewClientError(`scalar field "__typename" must have no selection`)
				}
				continue
			}

			field, ok := typ.Fields[selection.Name]
			if !ok {
				return NewClientError(`unknown field "%s"`, selection.Name)
			}

			// Only parse args once for a given selection.
			if !selection.parsed {
				selection.parsed = true
				parsed, err := field.ParseArguments(selection.UnparsedArgs)
				if err != nil {
					return NewClientError(`error parsing args for "%s": %s`, selection.Name, err)
				}
				selection.Args = parsed
			}

			selection.ParentType = typ.Name

			if err := PrepareQuery(ctx, field.Type, selection.SelectionSet); err != nil {
				return err
			}
		}
		for _, fragment := range selectionSet.Fragments {
			if err := PrepareQuery(ctx, typ, fragment.SelectionSet); err != nil {
				return err
			}
		}
		return nil

	case *List:
		return PrepareQuery(ctx, typ.Type, selectionSet)

	case *NonNull:
		return PrepareQuery(ctx, typ.Type, selectionSet)

	default:
		panic("unknown type kind")
	}
}

func SafeExecuteBatchResolver(ctx context.Context, field *Field, sources []interface{}, args interface{}, selectionSet *SelectionSet) (results []interface{}, err error) {
	defer func() {
		if panicErr := recover(); panicErr != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			results, err = nil, fmt.Errorf("graphql: panic: %v\n%s", panicErr, buf)
		}
	}()
	return field.BatchResolver(ctx, sources, args, selectionSet)
}

func SafeExecuteResolver(ctx context.Context, field *Field, source, args interface{}, selectionSet *SelectionSet) (result interface{}, err error) {
	defer func() {
		if panicErr := recover(); panicErr != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			result, err = nil, fmt.Errorf("graphql: panic: %v\n%s", panicErr, buf)
		}
	}()
	return field.Resolve(ctx, source, args, selectionSet)
}

type ExecutorRunner interface {
	Execute(ctx context.Context, typ Type, source interface{}, query *Query) (interface{}, error)
}

type resolveAndExecuteCacheKey struct {
	field     *Field
	source    interface{}
	selection *Selection
}
