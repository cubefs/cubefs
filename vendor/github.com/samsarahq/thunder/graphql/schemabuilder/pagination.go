package schemabuilder

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/samsarahq/thunder/batch"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/internal/filter"
	"golang.org/x/sync/errgroup"
)

// Connection conforms to the GraphQL Connection type in the Relay Pagination spec.
type Connection struct {
	TotalCount int64
	Edges      []Edge
	PageInfo   PageInfo
}

var typeOfString = reflect.TypeOf("")
var typeofFilterMap = reflect.TypeOf(map[batch.Index]string{})

// paginateManually applies the pagination arguments to the edges in memory and sets hasNextPage +
// hasPrevPage. The behavior is expected to conform to the Relay Cursor spec:
// https://facebook.github.io/relay/graphql/connections.htm#EdgesToReturn()
func (c *Connection) paginateManually(args PaginationArgs) error {
	var elemsAfter, elemsBefore bool
	c.Edges, elemsAfter, elemsBefore = applyCursorsToAllEdges(c.Edges, args.Before, args.After)

	c.PageInfo.HasNextPage = args.Before != nil && elemsAfter
	c.PageInfo.HasPrevPage = args.After != nil && elemsBefore

	if (safeInt64Ptr(args.First) < 0) || safeInt64Ptr(args.Last) < 0 {
		return graphql.NewClientError("first/last cannot be a negative integer")
	}

	if args.First != nil && args.Last != nil {
		return graphql.NewClientError("cannot use both first and last together")
	}

	if args.First != nil && len(c.Edges) > int(*args.First) {
		c.Edges = c.Edges[:int(*args.First)]
		c.PageInfo.HasNextPage = true
	}

	if args.Last != nil && len(c.Edges) > int(*args.Last) {
		c.Edges = c.Edges[len(c.Edges)-int(*args.Last):]
		c.PageInfo.HasPrevPage = true
	}
	return nil
}

// setCursors sets the start and end cursors of the current page.
func (c *Connection) setCursors() {
	if len(c.Edges) == 0 {
		return
	}
	c.PageInfo.EndCursor = c.Edges[len(c.Edges)-1].Cursor
	c.PageInfo.StartCursor = c.Edges[0].Cursor
}

// externallySetPageInfo takes in a user-defined PaginationInfo struct,
// using its count, HasNextPage and HasPrevPage information as the source
// of truth.
func (c *Connection) externallySetPageInfo(info PaginationInfo) (err error) {
	c.PageInfo.HasNextPage = info.HasNextPage
	c.PageInfo.HasPrevPage = info.HasPrevPage
	c.TotalCount, err = info.TotalCount()
	c.PageInfo.Pages = info.Pages
	return err
}

// PageInfo contains information for pagination on a connection type. The list of Pages is used for
// page-number based pagination where the ith index corresponds to the start cursor of (i+1)st page.
type PageInfo struct {
	HasNextPage bool
	EndCursor   string
	HasPrevPage bool
	StartCursor string
	Pages       []string
}

// Edge consists of a node paired with its b64 encoded cursor.
type Edge struct {
	Node   interface{}
	Cursor string
}

// ConnectionArgs conform to the pagination arguments as specified by the Relay Spec for Connection
// types. https://facebook.github.io/relay/graphql/connections.htm#sec-Arguments
type ConnectionArgs struct {
	// first: n
	First *int64
	// last: n
	Last *int64
	// after: cursor
	After *string
	// before: cursor
	Before *string
	// User-facing args.
	Args interface{}
	// filterText: "text search"
	FilterText *string
	// FilterTextFields: ["filter name"]
	FilterTextFields *[]string
	// sortBy: "fieldName"
	SortBy *string
	// sortOrder: "asc" | "desc"
	SortOrder *SortOrder
	// filterType: "customFilterType"
	// Note: FilterType is not part of the Relay Spec for Connection types
	FilterType *string
}

// PaginationArgs are used in externally set connections by embedding them in an args struct. They
// are mapped onto ConnectionArgs, which follows the Relay spec for connection types.
type PaginationArgs struct {
	First  *int64
	Last   *int64
	After  *string
	Before *string

	FilterText       *string
	FilterTextFields *[]string
	SortBy           *string
	SortOrder        *SortOrder
	FilterType       *string
}

func (p PaginationArgs) limit() int {
	if p.First != nil {
		return int(*p.First)
	}
	if p.Last != nil {
		return int(*p.Last)
	}
	return 0
}

// PaginationInfo can be returned in a PaginateFieldFunc. The TotalCount function returns the
// totalCount field on the connection Type. If the resolver makes a SQL Query, then HasNextPage and
// HasPrevPage can be resolved in an efficient manner by requesting first/last:n + 1 items in the
// query. Then the flags can be filled in by checking the result size.
type PaginationInfo struct {
	TotalCountFunc func() int64
	HasNextPage    bool
	HasPrevPage    bool
	Pages          []string
}

// PostProcessOptions is used to instruct Thunder to perform additional operations on the output
// in the case of manually paginated resolvers.
// This struct should be returned as the second result from a manually-paginated field func.
type PostProcessOptions struct {
	// Whether or not Thunder should apply filtering on the output. This flag can
	// be used by externally managed resolvers when they decide to fall back to Thunder's
	// filtering based on the query arguments.
	ApplyTextFilter bool
	// Whether or not Thunder should set the Page Info's fields based on the output.
	// An exception to this are start and end cursors, which will be set by Thunder
	// regardless of the SetPageInfo flag.
	SetPageInfo bool
}

func (i PaginationInfo) TotalCount() (int64, error) {
	if i.TotalCountFunc == nil {
		return 0, errors.New("must set TotalCountFunc on PaginationInfo")
	}
	return i.TotalCountFunc(), nil
}

func getTypeName(typ reflect.Type) string {
	if typ.Kind() == reflect.Ptr {
		return typ.Elem().Name()
	}
	return fmt.Sprintf("NonNull%s", typ.Name())
}

type connectionContext struct {
	*funcContext
	// The string value for the key field name.
	Key string
	// Whether or not the FieldFunc returns PageInfo (overrides thunder's auto-populated PageInfo).
	ReturnsPageInfo bool
	// The post process options returned by the resolver.
	PostProcessOptions PostProcessOptions
	// The index of PaginationArgs in the arguments provided to the FieldFunc.
	PaginationArgsIndex int
	// The GraphQL fields for filtered text to be resolved.
	FilterTextFields map[string]*graphql.Field
	// The GraphQL fields for sorting to be resolved.
	SortFields map[string]*graphql.Field
	// The slice sorting function for each GraphQL field.
	SortFunctions map[string]func([]sortReference, SortOrder)
	// The custom filter functions available.
	FilterFunctions map[string]func(string, []string) bool
	// The custom search tokenization functions available.
	TokenizeSearchFunctions map[string]func(string) []string
}

// embedsPaginationArgs returns true if PaginationArgs were embedded.
func (c *connectionContext) embedsPaginationArgs() bool {
	return c.PaginationArgsIndex != -1
}

// IsExternallyManaged returns true if the connection is managed by the FieldFunc's function
// and not thunder.
func (c *connectionContext) IsExternallyManaged() bool {
	return c.embedsPaginationArgs() || c.ReturnsPageInfo
}

// Validate returns an error if the connection isn't correctly implemented.
func (c *connectionContext) Validate() error {
	if c.IsExternallyManaged() && !(c.embedsPaginationArgs() && c.ReturnsPageInfo) {
		return errors.New("if pagination args are embedded then pagination info must be included as a return value")
	}
	return nil
}

// constructEdgeType wraps the typ (which is the type of the Node) in an Edge type conforming to the
// Relay spec.
func (sb *schemaBuilder) constructEdgeType(typ reflect.Type) (graphql.Type, error) {
	nodeType, err := sb.getType(typ)
	if err != nil {
		return nil, err
	}

	fieldMap := make(map[string]*graphql.Field)

	nodeField := &graphql.Field{
		Resolve: func(ctx context.Context, source, args interface{}, selectionSet *graphql.SelectionSet) (interface{}, error) {
			if value, ok := source.(Edge); ok {
				return value.Node, nil
			}

			return nil, fmt.Errorf("error resolving node in edge")

		},
		Type:           &graphql.NonNull{Type: nodeType},
		ParseArguments: nilParseArguments,
	}
	fieldMap["node"] = nodeField

	cursorType, err := sb.getType(typeOfString)
	if err != nil {
		return nil, err
	}

	cursorField := &graphql.Field{
		Resolve: func(ctx context.Context, source, args interface{}, selectionSet *graphql.SelectionSet) (interface{}, error) {
			if value, ok := source.(Edge); ok {
				return value.Cursor, nil
			}
			return nil, fmt.Errorf("error resolving cursor in edge")
		},
		Type:           cursorType,
		ParseArguments: nilParseArguments,
	}

	fieldMap["cursor"] = cursorField

	return &graphql.NonNull{
		Type: &graphql.Object{
			Name:        fmt.Sprintf("%sEdge", getTypeName(typ)),
			Description: "",
			Fields:      fieldMap,
		},
	}, nil

}

// constructConnectionType wraps typ (type of the Node) in a Connection Type conforming to the Relay spec.
func (c *connectionContext) constructConnectionType(sb *schemaBuilder, typ reflect.Type) (graphql.Type, error) {
	fieldMap := make(map[string]*graphql.Field)

	countType, _ := reflect.TypeOf(Connection{}).FieldByName("TotalCount")
	countField, err := sb.buildField(countType)
	if err != nil {
		return nil, err
	}

	fieldMap["totalCount"] = countField
	edgeType, err := sb.constructEdgeType(typ)
	if err != nil {
		return nil, err
	}

	edgesSliceType := &graphql.NonNull{Type: &graphql.List{Type: edgeType}}

	edgesSliceField := &graphql.Field{
		Resolve: func(ctx context.Context, source, args interface{}, selectionSet *graphql.SelectionSet) (interface{}, error) {
			if value, ok := source.(Connection); ok {
				return value.Edges, nil
			}
			return nil, fmt.Errorf("error resolving edges in connection")
		},
		Type:           edgesSliceType,
		ParseArguments: nilParseArguments,
	}

	fieldMap["edges"] = edgesSliceField

	pageInfoType, _ := reflect.TypeOf(Connection{}).FieldByName("PageInfo")
	pageInfoField, err := sb.buildField(pageInfoType)

	if err != nil {
		return nil, err
	}
	fieldMap["pageInfo"] = pageInfoField
	retObject := &graphql.NonNull{
		Type: &graphql.Object{
			Name:        fmt.Sprintf("%sConnection", getTypeName(typ)),
			Description: "",
			Fields:      fieldMap,
		},
	}
	return retObject, nil
}

func safeInt64Ptr(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

// getCursorIndex returns the index corresponding to the cursor in the slice.
func getCursorIndex(edges []Edge, cursor string) int {
	for i, val := range edges {
		if val.Cursor == cursor {
			return i
		}
	}
	return -1
}

// applyCursorsToAllEdges returns the slice of edges after applying the after and before arguments.
// It also implements part of the hasNextPage and hasPrevPage algorithm by returning if there are
// elements after or before the arguments.
func applyCursorsToAllEdges(edges []Edge, before *string, after *string) ([]Edge, bool, bool) {
	edgeCount := len(edges)
	elemsAfter := false
	elemsBefore := false

	if after != nil {
		i := getCursorIndex(edges, *after)
		if i != -1 {
			edges = edges[i+1:]
			if i != 0 {
				elemsBefore = true
			}
		}

	}
	if before != nil {
		i := getCursorIndex(edges, *before)
		if i != -1 {
			edges = edges[:i]
			if i != edgeCount-1 {
				elemsAfter = true
			}
		}

	}

	return edges, elemsAfter, elemsBefore

}

func (c *connectionContext) nodesToEdges(nodes []interface{}) (edges []Edge) {
	for _, node := range nodes {
		keyValue := reflect.ValueOf(node)
		if keyValue.Kind() == reflect.Ptr {
			keyValue = keyValue.Elem()
		}
		keyString := []byte(fmt.Sprintf("%v", keyValue.FieldByName(c.Key).Interface()))
		cursorVal := base64.StdEncoding.EncodeToString(keyString)
		edges = append(edges, Edge{Node: node, Cursor: cursorVal})
	}

	return edges
}

// Creates a pages slice, starting with a blank cursor, then every n+1 edge's cursor (if you have 20
// entries per page, 19, 39, 59 etc). This works for `after:` but works unexpectedly for `before:`,
// in that it is off by two (You would get 1-18 for before: 19).

// NOTE: The cursors are based off of the total and are not relative to the current query, meaning
// that they will shift with each query as entries are added.
func (c *connectionContext) pagesFromEdges(edges []Edge, limit int) (pages []string) {
	for i, edge := range edges {
		// The blank cursor indicates the initial page.
		if i == 0 {
			pages = append(pages, "")
		}

		// Limit at zero means infinite / no pages.
		if limit == 0 {
			continue
		}
		// The last cursor can't be followed by another page because there are no more entries.
		if i == len(edges)-1 {
			continue
		}
		// If the next cursor is the start cursor of a page then push the current cursor
		// to the list.
		if (i+1)%limit == 0 {
			pages = append(pages, edge.Cursor)
		}
	}

	return pages
}

type SafeBatchNodesToKeep struct {
	nodesToKeep []bool
	mux         sync.Mutex
}

func (c *connectionContext) applyBatchTextFilter(ctx context.Context, nodes []interface{}, searchTokens []string, filterType *string, batchedFields map[string]*graphql.Field, nodesToKeep []bool) error {
	g, ctx := errgroup.WithContext(ctx)
	m := &sync.Mutex{}
	for unscopeName, unscopedFilterField := range batchedFields {
		name, filterField := unscopeName, unscopedFilterField
		g.Go(func() error {
			texts, err := graphql.SafeExecuteBatchResolver(ctx, filterField, nodes, nil, nil)
			if err != nil {
				return err
			}
			for i, text := range texts {
				textString, ok := text.(string)
				if !ok {
					return fmt.Errorf("filter %s returned %T, must be a string", name, text)
				}

				shouldKeep := false
				if filterType == nil {
					shouldKeep = filter.DefaultFilterFunc(textString, searchTokens)
				} else if filterFunc, ok := c.FilterFunctions[*filterType]; ok {
					shouldKeep = filterFunc(textString, searchTokens)
				}

				if shouldKeep {
					m.Lock()
					nodesToKeep[i] = true
					m.Unlock()
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *connectionContext) checkFilters(ctx context.Context, node interface{}, searchTokens []string, filterFields map[string]*graphql.Field, filterType *string) (bool, error) {
	keep := false
	for name, filterField := range filterFields {
		// Resolve the graphql.Field made for sorting.
		text, err := graphql.SafeExecuteResolver(ctx, filterField, node, nil, nil)
		if err != nil {
			return keep, err
		}
		// Only strings are allowed for FilterText fields.
		textString, ok := text.(string)
		if !ok {
			return keep, fmt.Errorf("filter %s returned %T, must be a string", name, text)
		}

		if filterType == nil {
			if filter.DefaultFilterFunc(textString, searchTokens) {
				keep = true
				break
			}
		} else if filterFunc, ok := c.FilterFunctions[*filterType]; ok {
			if filterFunc(textString, searchTokens) {
				keep = true
				break
			}
		}
	}
	return keep, nil
}

// We found that parallelizing non-expensive fields was slower due to the overhead of
// spawning goroutines, so we execute non-expensive fields serially. We're also concerned
// about the memory overhead of spawning many goroutines
func (c *connectionContext) applyTextFilterNotBatchedExpensive(ctx context.Context, nodes []interface{}, searchTokens []string, filterType *string, filterFields map[string]*graphql.Field, nodesToKeep []bool) error {
	g, ctx := errgroup.WithContext(ctx)
	for unscopedI, unscopedNode := range nodes {
		i, node := unscopedI, unscopedNode
		g.Go(func() error {
			keep, err := c.checkFilters(ctx, node, searchTokens, filterFields, filterType)
			nodesToKeep[i] = keep
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *connectionContext) applyTextFilterNotBatched(ctx context.Context, nodes []interface{}, searchTokens []string, filterType *string, filterFields map[string]*graphql.Field, nodesToKeep []bool) error {
	for unscopedI, unscopedNode := range nodes {
		i, node := unscopedI, unscopedNode
		keep, err := c.checkFilters(ctx, node, searchTokens, filterFields, filterType)
		if err != nil {
			return err
		}
		nodesToKeep[i] = keep
	}
	return nil
}

func (c *connectionContext) applyTextFilter(ctx context.Context, nodes []interface{}, args PaginationArgs) ([]interface{}, error) {
	if args.FilterText == nil || *args.FilterText == "" {
		return nodes, nil
	}

	filterFields := make(map[string]bool)
	if args.FilterTextFields != nil {
		for _, name := range *args.FilterTextFields {
			filterFields[name] = true
		}
	} else {
		for name, _ := range c.FilterTextFields {
			filterFields[name] = true
		}
	}

	filterTextFieldsNotBatched := make(map[string]*graphql.Field)
	filterTextFieldsNotBatchedExpensive := make(map[string]*graphql.Field)
	filterTextFieldsBatched := make(map[string]*graphql.Field)
	for name, filterField := range c.FilterTextFields {
		if _, ok := filterFields[name]; !ok {
			continue
		}
		if filterField.Batch && filterField.UseBatchFunc(ctx) {
			filterTextFieldsBatched[name] = filterField
		} else {
			if filterField.Expensive {
				filterTextFieldsNotBatchedExpensive[name] = filterField
			} else {
				filterTextFieldsNotBatched[name] = filterField
			}
		}
	}

	g, ctx := errgroup.WithContext(ctx)

	// Get search tokens from search query
	var searchTokens []string
	if args.FilterType == nil {
		searchTokens = filter.GetDefaultSearchTokens(*args.FilterText)
	} else if tokenizeSearchFunc, ok := c.TokenizeSearchFunctions[*args.FilterType]; ok {
		searchTokens = tokenizeSearchFunc(*args.FilterText)
	}

	nodesToKeep := make([]bool, len(nodes))
	expensiveNodesToKeep := make([]bool, len(nodes))
	batchedNodesToKeep := make([]bool, len(nodes))

	if len(filterTextFieldsNotBatched) > 0 {
		g.Go(func() error {
			return c.applyTextFilterNotBatched(ctx, nodes, searchTokens, args.FilterType, filterTextFieldsNotBatched, nodesToKeep)
		})
	}
	if len(filterTextFieldsNotBatchedExpensive) > 0 {
		g.Go(func() error {
			return c.applyTextFilterNotBatchedExpensive(ctx, nodes, searchTokens, args.FilterType, filterTextFieldsNotBatchedExpensive, expensiveNodesToKeep)
		})
	}
	if len(filterTextFieldsBatched) > 0 {
		g.Go(func() error {
			return c.applyBatchTextFilter(ctx, nodes, searchTokens, args.FilterType, filterTextFieldsBatched, batchedNodesToKeep)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	var filteredNodes []interface{}
	for i := range nodesToKeep {
		if nodesToKeep[i] || batchedNodesToKeep[i] || expensiveNodesToKeep[i] {
			filteredNodes = append(filteredNodes, nodes[i])
		}
	}
	return filteredNodes, nil
}

func getSortReference(ctx context.Context, sortField *graphql.Field, node interface{}, i int) (*sortReference, error) {
	// Resolve the graphql.Field made for sorting.
	sortValue, err := graphql.SafeExecuteResolver(ctx, sortField, node, nil, nil)
	if err != nil {
		return nil, err
	}
	// Hang onto index in order added in order to properly sort the nodes.
	return &sortReference{
		index: i,
		value: reflect.ValueOf(sortValue),
	}, nil

}

func (c *connectionContext) applySort(ctx context.Context, nodes []interface{}, args PaginationArgs) ([]interface{}, error) {
	if args.SortBy == nil {
		return nodes, nil
	}

	// Default to ascending sort.
	sortOrder := SortOrder_Ascending
	if args.SortOrder != nil {
		sortOrder = *args.SortOrder
	}

	sortField, ok := c.SortFields[*args.SortBy]
	// If the field wasn't registered, it's an unknown sort field.
	if !ok {
		return nil, fmt.Errorf("unknown sort field %s", *args.SortBy)
	}

	// sortValues is the slice we'll be sorting (with the sorted values) in order to figure out node order.
	sortValues := make([]sortReference, len(nodes))
	g, ctx := errgroup.WithContext(ctx)
	if sortField.Batch && sortField.UseBatchFunc(ctx) {
		sortValuesBatched, err := graphql.SafeExecuteBatchResolver(ctx, sortField, nodes, nil, nil)
		if err != nil {
			return nil, err
		}
		for i, sortValue := range sortValuesBatched {
			sortValues[i] = sortReference{
				index: i,
				value: reflect.ValueOf(sortValue),
			}
		}
	} else {
		for unscopedI, unscopedNode := range nodes {
			i, node := unscopedI, unscopedNode
			if sortField.Expensive {
				g.Go(func() error {
					sortValue, err := getSortReference(ctx, sortField, node, i)
					if err != nil {
						return err
					}
					sortValues[i] = *sortValue
					return nil
				})
			} else {
				sortValue, err := getSortReference(ctx, sortField, node, i)
				if err != nil {
					return nil, err
				}
				sortValues[i] = *sortValue
			}
		}
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Sort values by appropriate function.
	c.SortFunctions[*args.SortBy](sortValues, sortOrder)

	// Map sort order onto nodes.
	sortedNodes := make([]interface{}, len(nodes))
	for i, val := range sortValues {
		sortedNodes[i] = nodes[val.index]
	}

	return sortedNodes, nil
}

// getConnection applies the ConnectionArgs to nodes and returns the result in a wrapped Connection
// type.
func (c *connectionContext) getConnection(ctx context.Context, out []reflect.Value, args PaginationArgs) (Connection, error) {
	nodes := castSlice(out[0].Interface())
	if len(nodes) == 0 {
		return Connection{}, nil
	}

	if !c.IsExternallyManaged() || c.PostProcessOptions.ApplyTextFilter {
		var err error
		nodes, err = c.applyTextFilter(ctx, nodes, args)
		if err != nil {
			return Connection{}, err
		}
	}

	if !c.IsExternallyManaged() {
		var err error
		nodes, err = c.applySort(ctx, nodes, args)
		if err != nil {
			return Connection{}, err
		}
	}

	limit := args.limit()
	edges := c.nodesToEdges(nodes)
	pages := c.pagesFromEdges(edges, limit)
	connection := Connection{
		TotalCount: int64(len(nodes)),
		Edges:      edges,
		PageInfo: PageInfo{
			Pages: pages,
		},
	}

	// If the pagination is externally managed, thunder isn't going to handle setting page
	// information or reducing the edges, unless it is explicitly instructed to.
	if c.IsExternallyManaged() && !c.PostProcessOptions.SetPageInfo {
		// XXX: We might want to handle the case where the externally managed result set is of
		// incorrect size (too big) and error.
		if err := connection.externallySetPageInfo(out[1].Interface().(PaginationInfo)); err != nil {
			return Connection{}, err
		}
	} else {
		if err := connection.paginateManually(args); err != nil {
			return Connection{}, err
		}
	}

	connection.setCursors()
	return connection, nil
}

// PaginateFieldFunc registers a function that is also paginated according to the Relay
// Connection Spec. The field is registered as a Connection Type and first, last, before and after
// are automatically added as arguments to the function. The return type to the function must be a
// list. The element of the list is wrapped as a Node Type.
// If the resolver needs to use the pagination arguments, then the PaginationArgs struct must be
// embedded in the args struct passed in the resolver function, and the PaginationInfo struct needs
// to be returned in the resolver func.
//
// Deprecated: Use FieldFunc(name, func, Paginated) instead.
func (o *Object) PaginateFieldFunc(name string, f interface{}) {
	o.FieldFunc(name, f, Paginated)
}

// indexOfPaginationArgs gets the index of PaginationArgs if they were embedded in a struct,
// otherwise returns -1.
func indexOfPaginationArgs(argType reflect.Type) int {
	for i := 0; i < argType.NumField(); i++ {
		field := argType.Field(i)

		if field.Type == reflect.TypeOf(PaginationArgs{}) {
			return i
		}
	}
	return -1
}

func getFuncReturnType(fn interface{}) reflect.Type {
	typ := reflect.TypeOf(fn)
	if typ.NumOut() == 0 {
		return nil
	}
	return typ.Out(0)
}

func supportedSort(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	}
	return false
}

type SortOrder int64

const (
	SortOrder_Ascending SortOrder = iota
	SortOrder_Descending
)

type sortReference struct {
	index int
	value reflect.Value
}

func getSort(typ reflect.Type) func([]sortReference, SortOrder) {
	switch typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return sorts[reflect.Int64]
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return sorts[reflect.Uint64]
	case reflect.Float32, reflect.Float64:
		return sorts[reflect.Float64]
	case reflect.String:
		return sorts[reflect.String]
	default:
		// This should literally be impossible.
		panic(fmt.Sprintf("unexpected type %v used in getSort", typ))
	}
}

var sorts = map[reflect.Kind]func([]sortReference, SortOrder){
	reflect.Int64: func(slice []sortReference, order SortOrder) {
		sort.SliceStable(slice, func(i, j int) bool {
			a := slice[i].value
			b := slice[j].value
			if order == SortOrder_Ascending {
				return a.Int() < b.Int()
			} else {
				return a.Int() > b.Int()
			}
		})
	},
	reflect.Uint64: func(slice []sortReference, order SortOrder) {
		sort.SliceStable(slice, func(i, j int) bool {
			a := slice[i].value
			b := slice[j].value
			if order == SortOrder_Ascending {
				return a.Uint() < b.Uint()
			} else {
				return a.Uint() > b.Uint()
			}
		})
	},
	reflect.Float64: func(slice []sortReference, order SortOrder) {
		sort.SliceStable(slice, func(i, j int) bool {
			a := slice[i].value
			b := slice[j].value
			if order == SortOrder_Ascending {
				return a.Float() < b.Float()
			} else {
				return a.Float() > b.Float()
			}
		})
	},
	reflect.String: func(slice []sortReference, order SortOrder) {
		sort.SliceStable(slice, func(i, j int) bool {
			a := slice[i].value
			b := slice[j].value
			if order == SortOrder_Ascending {
				return strings.ToLower(a.String()) < strings.ToLower(b.String())
			} else {
				return strings.ToLower(a.String()) > strings.ToLower(b.String())
			}
		})
	},
}

func (c *connectionContext) checkSortFunctionTypes(name string, sortMethod *method) error {
	if sortMethod.BatchArgs.FallbackFunc != nil {
		// Check the return type of the fallback function.
		sortableTyp := getFuncReturnType(sortMethod.BatchArgs.FallbackFunc)
		if !supportedSort(sortableTyp) {
			return fmt.Errorf(
				"invalid sort field %s: unsupported return type %v, must be of kind int, uint, float or string",
				name,
				sortableTyp,
			)
		}
		c.SortFunctions[name] = getSort(sortableTyp)
	}

	if sortMethod.Batch == false {
		// Check the return type of the function.
		sortableTyp := getFuncReturnType(sortMethod.Fn)
		if !supportedSort(sortableTyp) {
			return fmt.Errorf(
				"invalid sort field %s: unsupported return type %v, must be of kind int, uint, float or string",
				name,
				sortableTyp,
			)
		}
		c.SortFunctions[name] = getSort(sortableTyp)
	} else {
		// Check the return type of the batched function
		sortableTyp := getFuncReturnType(sortMethod.Fn)
		if sortableTyp.Kind() != reflect.Map || !supportedSort(sortableTyp.Elem()) {
			return fmt.Errorf(
				"invalid batch sort field %s: unsupported return type %v, must be of kind int, uint, float or string",
				name,
				sortableTyp,
			)
		}
		c.SortFunctions[name] = getSort(sortableTyp.Elem())
	}
	return nil
}

func (c *connectionContext) consumeSorts(sb *schemaBuilder, m *method, typ reflect.Type) error {
	c.SortFunctions = make(map[string]func([]sortReference, SortOrder))
	c.SortFields = make(map[string]*graphql.Field)

	for name, sortMethod := range m.SortMethods {
		err := c.checkSortFunctionTypes(name, sortMethod)
		if err != nil {
			return err
		}

		// Build a GraphQL field for the function.
		var field *graphql.Field
		if sortMethod.Batch && sortMethod.BatchArgs.FallbackFunc != nil && sortMethod.BatchArgs.ShouldUseBatchFunc != nil {
			field, err = sb.buildBatchFunctionWithFallback(typ, sortMethod)
		} else if sortMethod.Batch {
			field, err = sb.buildBatchFunction(typ, sortMethod)
		} else {
			field, err = sb.buildFunction(typ, sortMethod)
		}
		if err != nil {
			return err
		}

		if field.Args != nil && len(field.Args) > 0 {
			return fmt.Errorf("invalid sort field %s: sort fields can't take arguments", name)
		}
		c.SortFields[name] = field
	}

	return nil
}

func (c *connectionContext) checkFilterTextFunctionTypes(name string, filterMethod *method) error {
	if filterMethod.BatchArgs.FallbackFunc != nil {
		batchFuncTyp := getFuncReturnType(filterMethod.BatchArgs.FallbackFunc)
		if batchFuncTyp != typeOfString {
			return fmt.Errorf("invalid text filter field %s: unsupported return type %v, must be a string", name, batchFuncTyp)
		}
	}

	if filterMethod.Batch == false {
		batchFuncTyp := getFuncReturnType(filterMethod.Fn)
		if batchFuncTyp != typeOfString {
			return fmt.Errorf("invalid text filter field %s: unsupported return type %v, must be a string", name, batchFuncTyp)
		}
	} else {
		batchFuncTyp := getFuncReturnType(filterMethod.Fn)
		if batchFuncTyp != typeofFilterMap {
			return fmt.Errorf("invalid text filter field %s: unsupported return type %v, must be a map[batch.Index]string", name, batchFuncTyp)
		}
	}
	return nil
}

func (c *connectionContext) consumeTextFilters(sb *schemaBuilder, m *method, typ reflect.Type) error {
	// Store custom filter functions
	c.FilterFunctions = make(map[string]func(string, []string) bool)
	for name, filterMethod := range m.FilterMethods {
		c.FilterFunctions[name] = filterMethod
	}

	// Store custom tokenize search functions
	c.TokenizeSearchFunctions = make(map[string]func(string) []string)
	for name, tokenizeSearchMethod := range m.TokenizeFilterTextMethods {
		c.TokenizeSearchFunctions[name] = tokenizeSearchMethod
	}

	c.FilterTextFields = make(map[string]*graphql.Field)

	for name, filterMethod := range m.TextFilterMethods {

		err := c.checkFilterTextFunctionTypes(name, filterMethod)
		if err != nil {
			return err
		}

		var field *graphql.Field
		if filterMethod.Batch && filterMethod.BatchArgs.FallbackFunc != nil && filterMethod.BatchArgs.ShouldUseBatchFunc != nil {
			field, err = sb.buildBatchFunctionWithFallback(typ, filterMethod)
		} else if filterMethod.Batch {
			field, err = sb.buildBatchFunction(typ, filterMethod)
		} else {
			field, err = sb.buildFunction(typ, filterMethod)
		}

		if err != nil {
			return err
		}
		if field.Args != nil && len(field.Args) > 0 {
			return fmt.Errorf("invalid text filter field %s: text filter fields can't take arguments", name)
		}
		c.FilterTextFields[name] = field
	}
	return nil
}

func (c *connectionContext) consumePaginatedArgs(sb *schemaBuilder, in []reflect.Type) (*argParser, graphql.Type, []reflect.Type, error) {
	var argParser *argParser
	var argType graphql.Type
	var err error
	c.PaginationArgsIndex = -1
	// If the args passed into paginated field func embed the PaginationArgs then the arg parser
	// needs to be constructed differently from the default case.
	if len(in) > 0 && in[0] != selectionSetType {
		c.PaginationArgsIndex = indexOfPaginationArgs(in[0])
		if c.IsExternallyManaged() {
			argParser, argType, err = sb.buildEmbeddedPaginatedArgParser(in[0])
			if err != nil {
				return nil, nil, in, err
			}
		} else {
			argParser, argType, err = sb.buildPaginatedArgParser(in[0])
			if err != nil {
				return nil, nil, in, err
			}
		}
		in = in[1:]
	} else {
		argParser, argType, err = sb.buildPaginatedArgParser(nil)
		if err != nil {
			return nil, nil, in, err
		}

	}
	return argParser, argType, in, nil
}

func (sb *schemaBuilder) getKeyFieldOnStruct(nodeType reflect.Type) (string, error) {
	nodeObj := sb.objects[nodeType]
	if nodeObj == nil && nodeType.Kind() == reflect.Ptr {
		nodeObj = sb.objects[nodeType.Elem()]
	}
	if nodeObj == nil {
		return "", fmt.Errorf("%s must be a struct and registered as an object along with its key", nodeType)
	}
	nodeKey := reverseGraphqlFieldName(nodeObj.key)
	if nodeKey == "" {
		return nodeKey, fmt.Errorf("a key field must be registered for paginated objects")
	}
	if nodeType.Kind() == reflect.Ptr {
		nodeType = nodeType.Elem()
	}
	if _, ok := nodeType.FieldByName(nodeKey); !ok {
		return nodeKey, fmt.Errorf("field doesn't exist on struct")
	}

	return nodeKey, nil

}

// Parses the return types and checks if there's a pageInfo struct being returned by the resolver
func (c *connectionContext) parsePaginatedReturnSignature(m *method) (err error) {
	c.ReturnsPageInfo = false

	out := make([]reflect.Type, 0, c.funcType.NumOut())
	for i := 0; i < c.funcType.NumOut(); i++ {
		out = append(out, c.funcType.Out(i))
	}

	if len(out) > 0 && out[0] != errType {
		c.hasRet = true
		out = out[1:]
	}

	if len(out) > 0 && out[0] == reflect.TypeOf(PaginationInfo{}) {
		c.ReturnsPageInfo = true
		out = out[1:]
		if len(out) == 0 || out[0] != reflect.TypeOf(PostProcessOptions{}) {
			err = fmt.Errorf("%s returns a PaginationInfo not followed by a PaginationPostProcessOptions", c.funcType)
			return
		}
		out = out[1:]
	}

	if len(out) > 0 && out[0] == errType {
		c.hasError = true
		out = out[1:]
	}
	if len(out) != 0 {
		err = fmt.Errorf("%s return values should [result][, error]", c.funcType)
		return
	}

	if !c.hasRet && m.MarkedNonNullable {
		err = fmt.Errorf("%s is marked non-nullable, but has no return value", c.funcType)
		return
	}
	return

}

// buildPaginatedFieldWithFallback corresponds to buildFunction on a manually paginated type and a fallback paginated type
func (sb *schemaBuilder) buildPaginatedFieldWithFallback(typ reflect.Type, m *method) (*graphql.Field, error) {
	fallbackField, fallbackFuncCtx, err := sb.buildPaginatedFunctionAndFuncCtx(typ, &method{
		Fn:                m.ManualPaginationArgs.FallbackFunc,
		Expensive:         m.Expensive,
		Paginated:         m.Paginated,
		TextFilterMethods: m.TextFilterMethods,
		SortMethods:       m.SortMethods,
	})
	if err != nil {
		return nil, err
	}

	manualPaginationField, manualPaginationFuncCtx, err := sb.buildPaginatedFunctionAndFuncCtx(typ, m)
	if err != nil {
		return nil, err
	}

	// Validate that function signatures the manually paginated version and fallback version
	if fallbackFuncCtx.hasContext != manualPaginationFuncCtx.hasContext ||
		fallbackFuncCtx.hasArgs != manualPaginationFuncCtx.hasArgs ||
		fallbackFuncCtx.hasSelectionSet != manualPaginationFuncCtx.hasSelectionSet ||
		fallbackFuncCtx.hasError != manualPaginationFuncCtx.hasError ||
		fallbackFuncCtx.hasRet != manualPaginationFuncCtx.hasRet {
		return nil, fmt.Errorf("manual pagination and fallback function signatures did not match")
	}

	if fallbackField.Type.String() != manualPaginationField.Type.String() {
		return nil, fmt.Errorf("manual pagination and fallback graphql return types did not match: ManualPagination(%v) Fallback(%v)", manualPaginationField.Type, fallbackField.Type)
	}

	if len(fallbackField.Args) != len(manualPaginationField.Args) {
		return nil, fmt.Errorf("manual pagination and fallback arg type did not match: ManualPagination(%v) Fallback(%v)", manualPaginationField.Args, fallbackField.Args)
	}

	for key, fallbackTyp := range fallbackField.Args {
		if manualPaginationType, ok := manualPaginationField.Args[key]; !ok || fallbackTyp.String() != manualPaginationType.String() {
			return nil, fmt.Errorf("manual pagination and fallback func arg types did not match: ManualPagination(%v) Fallback(%v)", manualPaginationType, fallbackTyp)
		}
	}

	dualParser := &dualArgParser{
		argParser:         manualPaginationField.ParseArguments,
		fallbackArgParser: fallbackField.ParseArguments,
	}
	field := &graphql.Field{
		Resolve: func(ctx context.Context, source, args interface{}, selectionSet *graphql.SelectionSet) (i interface{}, e error) {
			dualArgs := args.(dualArgResponses)
			if m.ManualPaginationArgs.ShouldUseBatchFunc(ctx) {
				return fallbackField.Resolve(ctx, source, dualArgs.fallbackArgValue, selectionSet)
			}
			return manualPaginationField.Resolve(ctx, source, dualArgs.argValue, selectionSet)
		},
		Type:                       manualPaginationField.Type,
		Args:                       manualPaginationField.Args,
		ParseArguments:             dualParser.Parse,
		UseBatchFunc:               manualPaginationField.UseBatchFunc,
		Batch:                      manualPaginationField.Batch,
		External:                   manualPaginationField.External,
		Expensive:                  manualPaginationField.Expensive,
		NumParallelInvocationsFunc: manualPaginationField.NumParallelInvocationsFunc,
	}

	return field, nil

}

// buildPaginatedField corresponds to buildFunction on a paginated type. It wraps the return result
// of f in a connection type.
func (sb *schemaBuilder) buildPaginatedField(typ reflect.Type, m *method) (*graphql.Field, error) {
	paginatedField, _, err := sb.buildPaginatedFunctionAndFuncCtx(typ, m)
	return paginatedField, err
}

func (sb *schemaBuilder) buildPaginatedFunctionAndFuncCtx(typ reflect.Type, m *method) (*graphql.Field, *connectionContext, error) {
	c := &connectionContext{funcContext: &funcContext{typ: typ}}
	fun, err := c.getFuncVal(m)
	if err != nil {
		return nil, c, err
	}

	in := c.getFuncInputTypes()
	in = c.consumeContextAndSource(in)

	argParser, argType, in, err := c.consumePaginatedArgs(sb, in)
	if err != nil {
		return nil, c, err
	}
	c.hasArgs = true

	in = c.consumeSelectionSet(in)

	// We have succeeded if no arguments remain.
	if len(in) != 0 {
		return nil, nil, fmt.Errorf("%s arguments should be [context][, [*]%s][, args][, selectionSet]", c.funcType, typ)
	}

	// Parse return values. The first return value must be the actual value, and
	// the second value can optionally be an error.
	if err := c.parsePaginatedReturnSignature(&method{MarkedNonNullable: true}); err != nil {
		return nil, nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, nil, err
	}

	// It's safe to assume that there's a return type since the method is marked as non-nullable
	// when calling parseReturnSignature above.
	if c.funcType.Out(0).Kind() != reflect.Slice {
		return nil, nil, fmt.Errorf("paginated field func must return a slice type")
	}
	nodeType := c.funcType.Out(0).Elem()
	retType, err := c.constructConnectionType(sb, nodeType)
	if err != nil {
		return nil, nil, err
	}

	// If the node type is a pointer, get a non-pointer reference for building text filter and
	// sort FieldFuncs.
	nonPtrNodeType := nodeType
	if nodeType.Kind() == reflect.Ptr {
		nonPtrNodeType = nodeType.Elem()
	}

	if err := c.consumeTextFilters(sb, m, nonPtrNodeType); err != nil {
		return nil, nil, err
	}

	if err := c.consumeSorts(sb, m, nonPtrNodeType); err != nil {
		return nil, nil, err
	}

	c.Key, err = sb.getKeyFieldOnStruct(nodeType)
	if err != nil {
		return nil, nil, err
	}

	args, err := c.argsTypeMap(argType)

	ret := &graphql.Field{
		Resolve: func(ctx context.Context, source, args interface{}, selectionSet *graphql.SelectionSet) (interface{}, error) {

			argsVal := args
			hasArgs := true
			if !c.IsExternallyManaged() {
				val, ok := args.(ConnectionArgs)
				if !ok {
					return nil, fmt.Errorf("arguments should implement ConnectionArgs")
				}
				hasArgs = val.Args != nil
				if hasArgs {
					argsVal = reflect.ValueOf(val.Args).Elem().Interface()
				}
			}
			in := c.prepareResolveArgs(source, hasArgs, argsVal, ctx, selectionSet)
			var out []reflect.Value
			out = fun.Call(in)
			return c.extractReturnAndErr(ctx, out, args, retType)

		},
		Args:                       args,
		Type:                       retType,
		ParseArguments:             argParser.Parse,
		Expensive:                  m.Expensive,
		External:                   true,
		NumParallelInvocationsFunc: m.ConcurrencyArgs.numParallelInvocationsFunc,
	}
	return ret, c, nil
}

func (c *connectionContext) extractReturnAndErr(ctx context.Context, out []reflect.Value, args interface{}, retType graphql.Type) (interface{}, error) {
	var paginationArgs PaginationArgs

	// If the pagination args are not embedded then they need to be extracted out of ConnectionArgs
	// struct and setup for the slicing functions.
	if !c.IsExternallyManaged() {
		connectionArgs, _ := args.(ConnectionArgs)

		paginationArgs = PaginationArgs{
			First:            connectionArgs.First,
			Last:             connectionArgs.Last,
			After:            connectionArgs.After,
			Before:           connectionArgs.Before,
			FilterText:       connectionArgs.FilterText,
			FilterTextFields: connectionArgs.FilterTextFields,
			SortBy:           connectionArgs.SortBy,
			SortOrder:        connectionArgs.SortOrder,
			FilterType:       connectionArgs.FilterType,
		}
	} else {
		paginationArgs = reflect.ValueOf(args).Field(c.PaginationArgsIndex).Interface().(PaginationArgs)
		c.PostProcessOptions = out[2].Interface().(PostProcessOptions)
	}

	result, err := c.getConnection(ctx, out, paginationArgs)
	if err != nil {
		return nil, err
	}
	if c.hasError {
		if err := out[len(out)-1]; !err.IsNil() {
			return nil, err.Interface().(error)
		}
	}

	return result, nil
}

func castSlice(slice interface{}) []interface{} {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		panic("cast given a non-slice type")
	}

	ret := make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	return ret
}

// buildEmbeddedArgParser when the user embeds in the pagination args.
func (sb *schemaBuilder) buildEmbeddedPaginatedArgParser(typ reflect.Type) (*argParser, graphql.Type, error) {
	fields := make(map[string]argField)

	argType := &graphql.InputObject{
		Name:        typ.Name(),
		InputFields: make(map[string]graphql.Type),
	}
	pagArgIndex := 0
	argType.Name += "_InputObject"
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// The field which is of type interface should only be one and will be used to parse the
		// original function args.
		if field.Type.Kind() == reflect.Interface {
			continue
		}
		if field.Type == reflect.TypeOf(PaginationArgs{}) {
			pagArgIndex = i
			continue
		}

		name := makeGraphql(field.Name)

		var parser *argParser
		var fieldArgTyp graphql.Type

		parser, fieldArgTyp, err := sb.makeArgParser(field.Type)
		if err != nil {
			return nil, nil, err
		}

		argType.InputFields[name] = fieldArgTyp
		fields[name] = argField{
			field:  field,
			parser: parser,
		}
	}

	pagArgParser, pagArgType, err := sb.makeStructParser(reflect.TypeOf(PaginationArgs{}))
	if err != nil {
		return nil, nil, err
	}
	pagObj, ok := pagArgType.(*graphql.InputObject)
	if !ok {
		panic("failed to cast paginated args to an input object")
	}
	for name, objField := range pagObj.InputFields {
		if _, ok := argType.InputFields[name]; ok {
			return nil, nil, fmt.Errorf("these arg names are restricted: First, After, Last and Before")
		}
		argType.InputFields[name] = objField
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

			// nestedArgFields is the map used to parse the remaining fields: any field which isn't
			// part of ConnectionArgs should be a field of the args used for the paginated field.
			pagArgFields := make(map[string]interface{})
			for name := range asMap {
				if _, ok := fields[name]; !ok {
					pagArgFields[name] = asMap[name]
				}
			}

			fieldDest := dest.Field(pagArgIndex)
			if err := pagArgParser.FromJSON(pagArgFields, fieldDest); err != nil {
				return err
			}

			return nil
		},
		Type: typ,
	}, argType, nil

}

// buildPaginatedArgParser corresponds to buildArgParser for arguments used on a paginated
// fieldFunc. The args are nested as the Args field in the ConnectionArgs.
func (sb *schemaBuilder) buildPaginatedArgParser(originalArgType reflect.Type) (*argParser, graphql.Type, error) {
	//nestedArgParser and nestedArgType are used for building the parser function for the args
	//passed in to the paginated field.
	typ := reflect.TypeOf(ConnectionArgs{})

	// Fields build a map of the fields for ConnectionArgs.
	fields := make(map[string]argField)

	argType := &graphql.InputObject{
		Name:        typ.Name(),
		InputFields: make(map[string]graphql.Type),
	}

	argType.Name += "_InputObject"

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// The field which is of type interface should only be one and will be used to parse the
		// original function args.
		if field.Type.Kind() == reflect.Interface {
			continue
		}

		name := makeGraphql(field.Name)

		var parser *argParser
		var fieldArgTyp graphql.Type

		parser, fieldArgTyp, err := sb.makeArgParser(field.Type)
		if err != nil {
			return nil, nil, err
		}

		argType.InputFields[name] = fieldArgTyp

		fields[name] = argField{
			field:  field,
			parser: parser,
		}
	}

	var nestedArgParser *argParser
	var nestedArgType graphql.Type
	var err error
	if originalArgType != nil {
		nestedArgParser, nestedArgType, err = sb.makeStructParser(originalArgType)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build args for paginated field")
		}
		userInputObject, ok := nestedArgType.(*graphql.InputObject)
		if !ok {
			return nil, nil, fmt.Errorf("args should be an object")
		}

		for name, typ := range userInputObject.InputFields {
			argType.InputFields[name] = typ
		}
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

			// nestedArgFields is the map used to parse the remaining fields: any field which isn't
			// part of ConnectionArgs should be a field of the args used for the paginated field.
			nestedArgFields := make(map[string]interface{})
			for name := range asMap {
				if _, ok := fields[name]; !ok {
					nestedArgFields[name] = asMap[name]
				}
			}

			if nestedArgParser == nil {
				if len(nestedArgFields) != 0 {
					return fmt.Errorf("error in parsing args")
				}
				return nil
			}

			fieldDest := dest.FieldByName("Args")
			tmpDest := reflect.New(nestedArgParser.Type)
			if err := nestedArgParser.FromJSON(nestedArgFields, tmpDest.Elem()); err != nil {
				return err
			}
			fieldDest.Set(tmpDest)

			return nil
		},
		Type: typ,
	}, argType, nil
}
