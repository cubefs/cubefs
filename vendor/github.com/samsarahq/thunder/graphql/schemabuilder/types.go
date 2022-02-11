package schemabuilder

import (
	"context"
	"reflect"
)

// A Object represents a Go type and set of methods to be converted into an
// Object in a GraphQL schema.
type Object struct {
	Name        string // Optional, defaults to Type's name.
	Description string
	Type        interface{}
	Methods     Methods // Deprecated, use FieldFunc instead.
	key         string
	ServiceName string
}

type paginationObject struct {
	Name string
	Fn   interface{}
}

// FieldFuncOption is an interface for the variadic options that can be passed
// to a FieldFunc for configuring options on that function.
type FieldFuncOption interface {
	apply(*method)
}

// fieldFuncOptionFunc is a helper to define FieldFuncOptions from a func.
type fieldFuncOptionFunc func(*method)

func (f fieldFuncOptionFunc) apply(m *method) { f(m) }

// NonNullable is an option that can be passed to a FieldFunc to indicate that
// its return value is required, even if the return value is a pointer type.
var NonNullable fieldFuncOptionFunc = func(m *method) {
	m.MarkedNonNullable = true
}

// Paginated is an option that can be passed to a FieldFunc to indicate that
// its return value should be paginated.
var Paginated fieldFuncOptionFunc = func(m *method) {
	m.Paginated = true
}

// Expensive is an option that can be passed to a FieldFunc to indicate that
// the function is expensive to execute, so it should be parallelized.
var Expensive fieldFuncOptionFunc = func(m *method) {
	m.Expensive = true
}

// FilterFunc is an option that can be passed to a FieldFunc to specify
// custom string matching algorithms for filtering FieldFunc results.
//
// It accepts 3 parameters:
// 1. name
// This should describe the filter behaviour (e.g. "fuzzySearch").
// This parameter corresponds with the PaginationArg prop FilterType.
// Specifying filterType on a GraphQL query will indicate which tokenization
// and matching algorithms (see below) are used to filter results.
//
// 2. tokenizeFilterText
// This argument should be an algorithm that determines how the search query string
// is broken up into search tokens. It expects to receive a search string and return
// a list of search token strings which will be used to compare against
// the field string for a match in 'filterFunc' (see below).
//
// 3. filterFunc
// This argument should be an algorithm that determines whether there is a match
// between the field string and the search tokens. It expects to receive the field string
// and the list of search tokens (generated from 'tokenizeFilterText') and return a boolean
// which signals whether there is a match that should be included in the returned results.
func FilterFunc(name string, tokenizeFilterText func(string) []string, filterFunc func(string, []string) bool) FieldFuncOption {
	var fieldFuncFilterMethods fieldFuncOptionFunc = func(m *method) {
		// Store custom filter functions
		if m.FilterMethods == nil {
			m.FilterMethods = map[string]func(string, []string) bool{}
		}

		if _, ok := m.FilterMethods[name]; ok {
			panic("Field Filter Functions have the same name: " + name)
		}
		m.FilterMethods[name] = filterFunc

		// Store custom filer text tokenization functions
		if m.TokenizeFilterTextMethods == nil {
			m.TokenizeFilterTextMethods = map[string]func(string) []string{}
		}

		if _, ok := m.TokenizeFilterTextMethods[name]; ok {
			panic("Tokenize Filter Text Functions have the same name: " + name)
		}
		m.TokenizeFilterTextMethods[name] = tokenizeFilterText
	}
	return fieldFuncFilterMethods
}

func FilterField(name string, filter interface{}, options ...FieldFuncOption) FieldFuncOption {
	textFilterMethod := &method{Fn: filter, Batch: false, MarkedNonNullable: true}
	for _, opt := range options {
		opt.apply(textFilterMethod)
	}
	var fieldFuncTextFilterFields fieldFuncOptionFunc = func(m *method) {
		if m.TextFilterMethods == nil {
			m.TextFilterMethods = map[string]*method{}
		}
		if _, ok := m.TextFilterMethods[name]; ok {
			panic("Field Filters have the same name: " + name)
		}
		m.TextFilterMethods[name] = textFilterMethod
	}
	return fieldFuncTextFilterFields
}

func BatchFilterField(name string, batchFilter interface{}, options ...FieldFuncOption) FieldFuncOption {
	textFilterMethod := &method{Fn: batchFilter, Batch: true, MarkedNonNullable: true}
	for _, opt := range options {
		opt.apply(textFilterMethod)
	}
	var fieldFuncTextFilterFields fieldFuncOptionFunc = func(m *method) {
		if m.TextFilterMethods == nil {
			m.TextFilterMethods = map[string]*method{}
		}
		if _, ok := m.TextFilterMethods[name]; ok {
			panic("Field Filters have the same name: " + name)
		}
		m.TextFilterMethods[name] = textFilterMethod
	}
	return fieldFuncTextFilterFields
}

func BatchFilterFieldWithFallback(name string, batchFilter interface{}, filter interface{}, flag func(context.Context) bool, options ...FieldFuncOption) FieldFuncOption {
	textFilterMethod := &method{
		Fn: batchFilter,
		BatchArgs: batchArgs{
			FallbackFunc:       filter,
			ShouldUseBatchFunc: flag,
		}, Batch: true,
		MarkedNonNullable: true}
	for _, opt := range options {
		opt.apply(textFilterMethod)
	}
	var fieldFuncTextFilterFields fieldFuncOptionFunc = func(m *method) {
		if m.TextFilterMethods == nil {
			m.TextFilterMethods = map[string]*method{}
		}
		if _, ok := m.TextFilterMethods[name]; ok {
			panic("Field Filters have the same name: " + name)
		}
		m.TextFilterMethods[name] = textFilterMethod
	}
	return fieldFuncTextFilterFields
}

func SortField(name string, sort interface{}, options ...FieldFuncOption) FieldFuncOption {
	sortMethod := &method{Fn: sort, Batch: false, MarkedNonNullable: true}
	for _, opt := range options {
		opt.apply(sortMethod)
	}
	var fieldFuncSortFields fieldFuncOptionFunc = func(m *method) {
		if m.SortMethods == nil {
			m.SortMethods = map[string]*method{}
		}
		if _, ok := m.SortMethods[name]; ok {
			panic("Sorts fields have the same name: " + name)
		}
		m.SortMethods[name] = sortMethod
	}
	return fieldFuncSortFields
}

func BatchSortField(name string, batchSort interface{}, options ...FieldFuncOption) FieldFuncOption {
	sortMethod := &method{Fn: batchSort, Batch: true, MarkedNonNullable: true}
	for _, opt := range options {
		opt.apply(sortMethod)
	}
	var fieldFuncSortFields fieldFuncOptionFunc = func(m *method) {
		if m.SortMethods == nil {
			m.SortMethods = map[string]*method{}
		}
		if _, ok := m.SortMethods[name]; ok {
			panic("Sorts fields have the same name: " + name)
		}
		m.SortMethods[name] = sortMethod
	}
	return fieldFuncSortFields
}

func BatchSortFieldWithFallback(name string, batchSort interface{}, sort interface{}, flag func(context.Context) bool, options ...FieldFuncOption) FieldFuncOption {
	sortMethod := &method{
		Fn: batchSort,
		BatchArgs: batchArgs{
			FallbackFunc:       sort,
			ShouldUseBatchFunc: flag,
		}, Batch: true,
		MarkedNonNullable: true}
	for _, opt := range options {
		opt.apply(sortMethod)
	}
	var fieldFuncSortFields fieldFuncOptionFunc = func(m *method) {
		if m.SortMethods == nil {
			m.SortMethods = map[string]*method{}
		}
		if _, ok := m.SortMethods[name]; ok {
			panic("Sorts fields have the same name: " + name)
		}
		m.SortMethods[name] = sortMethod
	}
	return fieldFuncSortFields
}

// FieldFunc exposes a field on an object. The function f can take a number of
// optional arguments:
// func([ctx context.Context], [o *Type], [args struct {}]) ([Result], [error])
//
// For example, for an object of type User, a fullName field might take just an
// instance of the object:
//    user.FieldFunc("fullName", func(u *User) string {
//       return u.FirstName + " " + u.LastName
//    })
//
// An addUser mutation field might take both a context and arguments:
//    mutation.FieldFunc("addUser", func(ctx context.Context, args struct{
//        FirstName string
//        LastName  string
//    }) (int, error) {
//        userID, err := db.AddUser(ctx, args.FirstName, args.LastName)
//        return userID, err
//    })
func (s *Object) FieldFunc(name string, f interface{}, options ...FieldFuncOption) {
	if s.Methods == nil {
		s.Methods = make(Methods)
	}

	m := &method{Fn: f}
	for _, opt := range options {
		opt.apply(m)
	}

	if _, ok := s.Methods[name]; ok {
		panic("duplicate method")
	}
	s.Methods[name] = m
}

func (s *Object) BatchFieldFunc(name string, batchFunc interface{}, options ...FieldFuncOption) {
	if s.Methods == nil {
		s.Methods = make(Methods)
	}

	m := &method{
		Fn:    batchFunc,
		Batch: true,
	}
	for _, opt := range options {
		opt.apply(m)
	}

	if _, ok := s.Methods[name]; ok {
		panic("duplicate method")
	}
	s.Methods[name] = m
}

func (s *Object) BatchFieldFuncWithFallback(name string, batchFunc interface{}, fallbackFunc interface{}, flag UseFallbackFlag, options ...FieldFuncOption) {
	if s.Methods == nil {
		s.Methods = make(Methods)
	}

	m := &method{
		Fn: batchFunc,
		BatchArgs: batchArgs{
			FallbackFunc:       fallbackFunc,
			ShouldUseBatchFunc: flag,
		},
		Batch: true,
	}
	for _, opt := range options {
		opt.apply(m)
	}

	if _, ok := s.Methods[name]; ok {
		panic("duplicate method")
	}
	s.Methods[name] = m
}

func (s *Object) ManualPaginationWithFallback(name string, manualPaginatedFunc interface{}, fallbackFunc interface{}, flag UseFallbackFlag, options ...FieldFuncOption) {
	if s.Methods == nil {
		s.Methods = make(Methods)
	}

	m := &method{
		Fn: manualPaginatedFunc,
		ManualPaginationArgs: manualPaginationArgs{
			FallbackFunc:       fallbackFunc,
			ShouldUseBatchFunc: flag,
		},
		Paginated: true,
	}
	for _, opt := range options {
		opt.apply(m)
	}

	if _, ok := s.Methods[name]; ok {
		panic("duplicate method")
	}
	s.Methods[name] = m
}

// Key registers the key field on an object. The field should be specified by the name of the
// graphql field.
// For example, for an object User:
//   type struct User {
//	   UserKey int64
//   }
// The key will be registered as:
// object.Key("userKey")
func (s *Object) Key(f string) {
	s.key = f
}

type method struct {
	MarkedNonNullable bool
	Fn                interface{}

	// Whether or not the FieldFunc is paginated.
	Paginated bool

	// Whether or not the FieldFunc has been marked as expensive.
	Expensive bool

	// Custom filter methods for determining whether a field matches a search query.
	FilterMethods map[string]func(string, []string) bool

	// Custom methods for generating search tokens from a search query.
	TokenizeFilterTextMethods map[string]func(string) []string

	// Text filter methods
	TextFilterMethods map[string]*method

	// Sort methods
	SortMethods map[string]*method

	ConcurrencyArgs concurrencyArgs

	// Whether the FieldFunc is a batchField
	Batch bool

	BatchArgs batchArgs

	ManualPaginationArgs manualPaginationArgs

	// RootObjectType is an object where all the fields are keys
	// that can be exposed over federation
	RootObjectType reflect.Type

	// ShadowObjectType is the reflect type of parent object if it
	// is a shadow object. A shadow object's fields are each of the
	// field that are sent as args to a federated sunquery.
	ShadowObjectType reflect.Type
}

type concurrencyArgs struct {
	numParallelInvocationsFunc NumParallelInvocationsFunc
}

// NumParallelInvocationsFunc is a configuration option for non-expensive and batch
// fields that controls how many goroutines will get created for the field
// execution.  The nodes for the field execution will be evenly split across
// the different goroutines.
type NumParallelInvocationsFunc func(ctx context.Context, numNodes int) int

func (f NumParallelInvocationsFunc) apply(m *method) {
	m.ConcurrencyArgs.numParallelInvocationsFunc = f
}

type UseFallbackFlag func(context.Context) bool

type batchArgs struct {
	FallbackFunc       interface{}
	ShouldUseBatchFunc UseFallbackFlag
}

type manualPaginationArgs struct {
	FallbackFunc       interface{}
	ShouldUseBatchFunc UseFallbackFlag
}

// A Methods map represents the set of methods exposed on a Object.
type Methods map[string]*method

// Union is a special marker struct that can be embedded into to denote
// that a type should be treated as a union type by the schemabuilder.
//
// For example, to denote that a return value that may be a *Asset or
// *Vehicle might look like:
//   type GatewayUnion struct {
//     schemabuilder.Union
//     *Asset
//     *Vehicle
//   }
//
// Fields returning a union type should expect to return this type as a
// one-hot struct, i.e. only Asset or Vehicle should be specified, but not both.
type Union struct{}

var unionType = reflect.TypeOf(Union{})
