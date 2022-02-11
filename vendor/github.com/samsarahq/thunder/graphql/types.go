package graphql

import (
	"context"
	"fmt"
)

// Type represents a GraphQL type, and should be either an Object, a Scalar,
// or a List
type Type interface {
	String() string

	// isType() is a no-op used to tag the known values of Type, to prevent
	// arbitrary interface{} from implementing Type
	isType()
}

// Scalar is a leaf value.  A custom "Unwrapper" can be attached to the scalar
// so it can have a custom unwrapping (if nil we will use the default unwrapper).
type Scalar struct {
	Type      string
	Unwrapper func(interface{}) (interface{}, error)
}

func (s *Scalar) isType() {}

func (s *Scalar) String() string {
	return s.Type
}

// Enum is a leaf value
type Enum struct {
	Type       string
	Values     []string
	ReverseMap map[interface{}]string
}

func (e *Enum) isType() {}

func (e *Enum) String() string {
	return e.Type
}

func (e *Enum) enumValues() []string {
	return e.Values
}

// Object is a value with several fields
type Object struct {
	Name        string
	Description string
	KeyField    *Field
	Fields      map[string]*Field
}

func (o *Object) isType() {}

func (o *Object) String() string {
	return o.Name
}

// List is a collection of other values
type List struct {
	Type Type
}

func (l *List) isType() {}

func (l *List) String() string {
	return fmt.Sprintf("[%s]", l.Type)
}

type InputObject struct {
	Name        string
	InputFields map[string]Type
}

func (io *InputObject) isType() {}

func (io *InputObject) String() string {
	return io.Name
}

// NonNull is a non-nullable other value
type NonNull struct {
	Type Type
}

func (n *NonNull) isType() {}

func (n *NonNull) String() string {
	return fmt.Sprintf("%s!", n.Type)
}

// Union is a option between multiple types
type Union struct {
	Name        string
	Description string
	Types       map[string]*Object
}

func (*Union) isType() {}

func (u *Union) String() string {
	return u.Name
}

// Verify *Scalar, *Object, *List, *InputObject, and *NonNull implement Type
var _ Type = &Scalar{}
var _ Type = &Object{}
var _ Type = &List{}
var _ Type = &InputObject{}
var _ Type = &NonNull{}
var _ Type = &Enum{}
var _ Type = &Union{}

// A Resolver calculates the value of a field of an object
type Resolver func(ctx context.Context, source, args interface{}, selectionSet *SelectionSet) (interface{}, error)

// A BatchResolver calculates the value of a field for a slice of objects.
type BatchResolver func(ctx context.Context, sources []interface{}, args interface{}, selectionSet *SelectionSet) ([]interface{}, error)

// Field knows how to compute field values of an Object
//
// Fields are responsible for computing their value themselves.
type Field struct {
	Resolve        Resolver
	BatchResolver  BatchResolver
	Type           Type
	Args           map[string]Type
	ParseArguments func(json interface{}) (interface{}, error)

	UseBatchFunc func(context.Context) bool
	Batch        bool
	External     bool
	Expensive    bool

	// NumParallelInvocationsFunc controls how many goroutines we'll create for a
	// field execution (batch or non-expensive).  We pass in the number of srcs
	// we're executing with so implementers can write custom logic.
	NumParallelInvocationsFunc func(ctx context.Context, numNodes int) int

	// FederatedKey tells us which services need this field as federated key.
	FederatedKey map[string]bool
}

type Schema struct {
	Query    Type
	Mutation Type
}

// SelectionSet represents a core GraphQL query
//
// A SelectionSet can contain multiple fields and multiple fragments. For
// example, the query
//
//     {
//       name
//       ... UserFragment
//       memberships {
//         organization { name }
//       }
//     }
//
// results in a root SelectionSet with two selections (name and memberships),
// and one fragment (UserFragment). The subselection `organization { name }`
// is stored in the memberships selection.
//
// Because GraphQL allows multiple fragments with the same name or alias,
// selections are stored in an array instead of a map.
type SelectionSet struct {
	Selections []*Selection
	Fragments  []*Fragment
}

// ShallowCopy returns a shallow copy of SelectionSet.
func (s *SelectionSet) ShallowCopy() *SelectionSet {
	cp := &SelectionSet{}
	if s.Selections != nil {
		cp.Selections = make([]*Selection, len(s.Selections))
		copy(cp.Selections, s.Selections)
	}
	if s.Fragments != nil {
		cp.Fragments = make([]*Fragment, len(s.Fragments))
		copy(cp.Fragments, s.Fragments)
	}
	return cp
}

// A selection represents a part of a GraphQL query
//
// The selection
//
//     me: user(id: 166) { name }
//
// has name "user" (representing the source field to be queried), alias "me"
// (representing the name to be used in the output), args id: 166 (representing
// arguments passed to the source field to be queried), and subselection name
// representing the information to be queried from the resulting object.
type Selection struct {
	Name         string
	Alias        string
	Args         interface{}
	SelectionSet *SelectionSet
	Directives   []*Directive

	// The parsed flag is used to make sure the args for this Selection are only
	// parsed once.
	parsed bool

	// UnparsedArgs are the original json map[string]interface{} arguments.
	// This field is only available able after PrepareQuery has been called.
	UnparsedArgs map[string]interface{}

	// ParentType is the type that this field hangs off of.
	ParentType string
}

// A Fragment represents a reusable part of a GraphQL query
//
// The On part of a Fragment represents the type of source object for which
// this Fragment should be used. That is not currently implemented in this
// package.
type Fragment struct {
	On           string
	SelectionSet *SelectionSet
	Directives   []*Directive
}

// A Directive can be attached to a field or fragment inclusion, and can
// affect execution of the query in any way the server desires.
//
// The selections
//     users @skip(if:true)
//     drivers @include(if:true)
// would skip the users selection and keep the drivers selection depending
// on the argument passed into the directive
type Directive struct {
	Name string
	Args interface{}
}
