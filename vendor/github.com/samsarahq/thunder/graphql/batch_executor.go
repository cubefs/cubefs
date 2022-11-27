package graphql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/samsarahq/thunder/reactive"
)

// WorkUnit is a set of execution work that will be done when running
// a graphql query.  For every source there is an equivalent destination
// OutputNode that is used to record the result of running a section of the
// graphql query.
type WorkUnit struct {
	Ctx          context.Context
	field        *Field
	selection    *Selection
	sources      []interface{}
	destinations []*outputNode
	useBatch     bool
	objectName   string
}

type nonExpensive struct{}

func CheckNonExpensive(ctx context.Context) bool {
	return ctx.Value(nonExpensive{}) != nil
}

func (w *WorkUnit) Selection() *Selection {
	return w.selection
}

func (w *WorkUnit) IsBatch() bool {
	return w.useBatch
}

func (w *WorkUnit) IsExpensive() bool {
	return w.field.Expensive
}

// Splits the work unit to a series of work units (one for every source/dest pair).
func splitWorkUnit(unit *WorkUnit) []*WorkUnit {
	workUnits := make([]*WorkUnit, 0, len(unit.sources))
	for idx, source := range unit.sources {
		workUnits = append(workUnits, &WorkUnit{
			Ctx:          unit.Ctx,
			field:        unit.field,
			selection:    unit.selection,
			sources:      []interface{}{source},
			destinations: []*outputNode{unit.destinations[idx]},
			useBatch:     unit.useBatch,
			objectName:   unit.objectName,
		})
	}
	return workUnits
}

// Splits the work unit to N work units (based on configuration).
func splitToNWorkUnits(unit *WorkUnit, numUnits int) []*WorkUnit {
	if numUnits > len(unit.sources) {
		numUnits = len(unit.sources)
	}
	if numUnits <= 0 {
		numUnits = 1
	}

	avgUnitSize := (len(unit.sources) / numUnits) + 1
	workUnits := make([]*WorkUnit, 0, numUnits)
	for i := 0; i < numUnits; i++ {
		workUnits = append(workUnits, &WorkUnit{
			Ctx:          unit.Ctx,
			field:        unit.field,
			selection:    unit.selection,
			sources:      make([]interface{}, 0, avgUnitSize),
			destinations: make([]*outputNode, 0, avgUnitSize),
			useBatch:     unit.useBatch,
			objectName:   unit.objectName,
		})
	}

	for idx, source := range unit.sources {
		workUnits[idx%numUnits].sources = append(workUnits[idx%numUnits].sources, source)
		workUnits[idx%numUnits].destinations = append(workUnits[idx%numUnits].destinations, unit.destinations[idx])
	}

	return workUnits
}

// UnitResolver is a function that executes a function and returns a set of
// new work units that need to be run.
type UnitResolver func(*WorkUnit) []*WorkUnit

// WorkScheduler is an interface that can be provided to the BatchExecutor
// to control how we traverse the Execution graph.  Examples would include using
// a bounded goroutine pool, or using unbounded goroutine generation for each
// work unit.
type WorkScheduler interface {
	Run(resolver UnitResolver, startingUnits ...*WorkUnit)
}

func NewExecutor(scheduler WorkScheduler) ExecutorRunner {
	return &Executor{
		scheduler: scheduler,
	}
}

// BatchExecutor is a GraphQL executor.  Given a query it can run through the
// execution of the request.
type Executor struct {
	scheduler WorkScheduler
}

// Execute executes a query by traversing the GraphQL query graph and resolving
// or executing fields.  Any work that needs to be done is passed off to the
// scheduler to handle managing concurrency of the request.
// It must return a JSON marshallable response (or an error).
func (e *Executor) Execute(ctx context.Context, typ Type, source interface{}, query *Query) (interface{}, error) {
	queryObject, ok := typ.(*Object)
	if !ok {
		return nil, fmt.Errorf("expected query or mutation object for execution, got: %s", typ.String())
	}

	topLevelSelections, err := Flatten(query.SelectionSet)
	if err != nil {
		return nil, err
	}
	topLevelRespWriter := newTopLevelOutputNode(query.Name)
	initialSelectionWorkUnits := make([]*WorkUnit, 0, len(topLevelSelections))
	writers := make(map[string]*outputNode)
	for _, selection := range topLevelSelections {
		ok, err := ShouldIncludeNode(selection.Directives)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		field, ok := queryObject.Fields[selection.Name]
		if !ok {
			return nil, fmt.Errorf("invalid top-level selection %q", selection.Name)
		}

		writer := newOutputNode(topLevelRespWriter, selection.Alias)
		writers[selection.Alias] = writer

		initialSelectionWorkUnits = append(
			initialSelectionWorkUnits,
			&WorkUnit{
				Ctx:          ctx,
				sources:      []interface{}{source},
				field:        field,
				destinations: []*outputNode{writer},
				selection:    selection,
				objectName:   queryObject.Name,
			},
		)
	}

	e.scheduler.Run(executeWorkUnit, initialSelectionWorkUnits...)

	if topLevelRespWriter.errRecorder.err != nil {
		return nil, topLevelRespWriter.errRecorder.err
	}
	return outputNodeToJSON(writers), nil
}

// executeWorkUnit executes/resolves a work unit and checks the
// selections of the unit to determine if it needs to schedule more work (which
// will be returned as new work units that will need to get scheduled.
func executeWorkUnit(unit *WorkUnit) []*WorkUnit {
	if unit.field.Batch && unit.useBatch {
		return executeBatchWorkUnit(unit)
	}

	if !unit.field.Expensive {
		return executeNonExpensiveWorkUnit(unit)
	}

	var units []*WorkUnit
	for idx, src := range unit.sources {
		units = append(units, executeNonBatchWorkUnitWithCaching(src, unit.destinations[idx], unit)...)
	}
	return units
}

func executeBatchWorkUnit(unit *WorkUnit) []*WorkUnit {
	results, err := SafeExecuteBatchResolver(unit.Ctx, unit.field, unit.sources, unit.selection.Args, unit.selection.SelectionSet)
	if err != nil {
		for _, dest := range unit.destinations {
			dest.Fail(err)
		}
		return nil
	}
	unitChildren, err := resolveBatch(unit.Ctx, results, unit.field.Type, unit.selection.SelectionSet, unit.destinations)
	if err != nil {
		for _, dest := range unit.destinations {
			dest.Fail(err)
		}
		return nil
	}
	return unitChildren
}

func executeNonExpensiveWorkUnit(unit *WorkUnit) []*WorkUnit {
	results := make([]interface{}, 0, len(unit.sources))
	for idx, src := range unit.sources {
		ctx := unit.Ctx

		// Fields on the Mutation object should not be marked as "non-Expensive" because they are guaranteed to only execute once.
		// The only fields we want to validate "expensiveness" on are non-Mutation Fields.
		if unit.objectName != "Mutation" {
			ctx = context.WithValue(unit.Ctx, nonExpensive{}, struct{}{})
		}
		fieldResult, err := SafeExecuteResolver(ctx, unit.field, src, unit.selection.Args, unit.selection.SelectionSet)
		if err != nil {
			// Fail the unit and exit.
			unit.destinations[idx].Fail(err)
			return nil
		}
		results = append(results, fieldResult)
	}
	unitChildren, err := resolveBatch(unit.Ctx, results, unit.field.Type, unit.selection.SelectionSet, unit.destinations)
	if err != nil {
		for _, dest := range unit.destinations {
			dest.Fail(err)
		}
		return nil
	}
	return unitChildren
}

// executeNonBatchWorkUnitWithCaching wraps a resolve request in a reactive cache
// call.
// This function makes two assumptions:
// - We assume that all the reactive cache will get cleared if there is an error.
// - We assume that there is no "error-catching" mechanism that will stop an
//   error from propagating all the way to the top of the request stack.
func executeNonBatchWorkUnitWithCaching(src interface{}, dest *outputNode, unit *WorkUnit) []*WorkUnit {
	var workUnits []*WorkUnit
	subDestRes, err := reactive.Cache(unit.Ctx, getWorkCacheKey(src, unit.field, unit.selection), func(ctx context.Context) (interface{}, error) {
		subDest := newOutputNode(dest, "")
		workUnits = executeNonBatchWorkUnit(ctx, src, subDest, unit)
		return subDest.res, nil
	})
	if err != nil {
		dest.Fail(err)
	}
	dest.Fill(subDestRes)
	return workUnits
}

// getWorkCacheKey gets the work cache key for the provided source.
func getWorkCacheKey(src interface{}, field *Field, selection *Selection) resolveAndExecuteCacheKey {
	value := reflect.ValueOf(src)
	// cache the body of resolve and execute so that if the source doesn't change, we
	// don't need to recompute
	key := resolveAndExecuteCacheKey{field: field, source: src, selection: selection}
	// some types can't be put in a map; for those, use a always different value
	// as source
	if value.IsValid() && !value.Type().Comparable() {
		// TODO: Warn, or somehow prevent using type-system?
		key.source = new(byte)
	}
	return key
}

// executeNonBatchWorkUnit resolves a non-batch field in our graphql response graph.
func executeNonBatchWorkUnit(ctx context.Context, src interface{}, dest *outputNode, unit *WorkUnit) []*WorkUnit {
	fieldResult, err := SafeExecuteResolver(ctx, unit.field, src, unit.selection.Args, unit.selection.SelectionSet)
	if err != nil {
		dest.Fail(err)
		return nil
	}
	subFieldWorkUnits, err := resolveBatch(ctx, []interface{}{fieldResult}, unit.field.Type, unit.selection.SelectionSet, []*outputNode{dest})
	if err != nil {
		dest.Fail(err)
		return nil
	}
	return subFieldWorkUnits
}

// resolveBatch traverses the provided sources and fills in result data and
// returns new work units that are required to resolve the rest of the
// query result.
func resolveBatch(ctx context.Context, sources []interface{}, typ Type, selectionSet *SelectionSet, destinations []*outputNode) ([]*WorkUnit, error) {
	if len(sources) == 0 {
		return nil, nil
	}
	switch typ := typ.(type) {
	case *Scalar:
		return nil, resolveScalarBatch(sources, typ, destinations)
	case *Enum:
		return nil, resolveEnumBatch(sources, typ, destinations)
	case *List:
		return resolveListBatch(ctx, sources, typ, selectionSet, destinations)
	case *Union:
		return resolveUnionBatch(ctx, sources, typ, selectionSet, destinations)
	case *Object:
		return resolveObjectBatch(ctx, sources, typ, selectionSet, destinations)
	case *NonNull:
		return resolveBatch(ctx, sources, typ.Type, selectionSet, destinations)
	default:
		panic(typ)
	}
}

// Resolves the scalar type value for all the provided sources.
func resolveScalarBatch(sources []interface{}, typ *Scalar, destinations []*outputNode) error {
	for i, source := range sources {
		if typ.Unwrapper == nil {
			destinations[i].Fill(unwrap(source))
			continue
		}
		res, err := typ.Unwrapper(source)
		if err != nil {
			return err
		}
		destinations[i].Fill(res)
	}
	return nil
}

// Resolves the enum type value for all the provided sources.
func resolveEnumBatch(sources []interface{}, typ *Enum, destinations []*outputNode) error {
	for i, source := range sources {
		val := unwrap(source)
		if mapVal, ok := typ.ReverseMap[val]; !ok {
			err := errors.New("enum is not valid")
			destinations[i].Fail(err)
			return err
		} else {
			destinations[i].Fill(mapVal)
		}
	}
	return nil
}

// Flattens the sources for the list type and calls into an unwrapper method for
// the list's subtype.
func resolveListBatch(ctx context.Context, sources []interface{}, typ *List, selectionSet *SelectionSet, destinations []*outputNode) ([]*WorkUnit, error) {
	reflectedSources := make([]reflect.Value, len(sources))
	numFlattenedSources := 0
	for idx, source := range sources {
		reflectedSources[idx] = reflect.ValueOf(source)
		if reflectedSources[idx].IsValid() {
			numFlattenedSources += reflectedSources[idx].Len()
		}
	}

	flattenedResps := make([]*outputNode, 0, numFlattenedSources)
	flattenedSources := make([]interface{}, 0, numFlattenedSources)
	for idx, slice := range reflectedSources {
		if !slice.IsValid() {
			destinations[idx].Fill(make([]interface{}, 0))
			continue
		}
		respList := make([]interface{}, slice.Len())
		for i := 0; i < slice.Len(); i++ {
			writer := newOutputNode(destinations[idx], strconv.Itoa(i))
			respList[i] = writer
			flattenedResps = append(flattenedResps, writer)
			flattenedSources = append(flattenedSources, slice.Index(i).Interface())
		}
		destinations[idx].Fill(respList)
	}
	return resolveBatch(ctx, flattenedSources, typ.Type, selectionSet, flattenedResps)
}

// Traverses the Union type and resolves or creates work units to resolve
// all of the sub-objects for all the provided sources.
func resolveUnionBatch(ctx context.Context, sources []interface{}, typ *Union, selectionSet *SelectionSet, destinations []*outputNode) ([]*WorkUnit, error) {
	sourcesByType := make(map[string][]interface{}, len(typ.Types))
	destinationsByType := make(map[string][]*outputNode, len(typ.Types))
	for idx, src := range sources {
		union := reflect.ValueOf(src)
		if !union.IsValid() || (union.Kind() == reflect.Ptr && union.IsNil()) {
			// Don't create a destination for any nil Unions types
			destinations[idx].Fill(nil)
			continue
		}

		srcType := ""
		if union.Kind() == reflect.Ptr && union.Elem().Kind() == reflect.Struct {
			union = union.Elem()
		}
		for typString := range typ.Types {
			inner := union.FieldByName(typString)
			if inner.IsNil() {
				continue
			}
			if srcType != "" {
				return nil, fmt.Errorf("union type field should only return one value, but received: %s %s", srcType, typString)
			}
			srcType = typString
			sourcesByType[srcType] = append(sourcesByType[srcType], inner.Interface())
			destinationsByType[srcType] = append(destinationsByType[srcType], destinations[idx])
		}
	}

	var workUnits []*WorkUnit
	for srcType, sources := range sourcesByType {
		gqlType := typ.Types[srcType]
		for _, fragment := range selectionSet.Fragments {
			if fragment.On != srcType {
				continue
			}
			units, err := resolveObjectBatch(ctx, sources, gqlType, fragment.SelectionSet, destinationsByType[srcType])
			if err != nil {
				return nil, err
			}
			workUnits = append(workUnits, units...)
		}

	}
	return workUnits, nil
}

// Traverses the object selections and resolves or creates work units to resolve
// all of the object fields for every source passed in.
func resolveObjectBatch(ctx context.Context, sources []interface{}, typ *Object, selectionSet *SelectionSet, destinations []*outputNode) ([]*WorkUnit, error) {
	selections, err := Flatten(selectionSet)
	if err != nil {
		return nil, err
	}
	numExpensive := 0
	numNonExpensive := 0
	for _, selection := range selections {
		field, ok := typ.Fields[selection.Name]
		if !ok {
			continue
		}
		if shouldUseBatch(ctx, field) {
			numNonExpensive++
			continue
		}
		if field.Expensive {
			numExpensive++
			continue
		}
		if field.External {
			numNonExpensive++
			continue
		}
	}

	// For every object, create a "destination" map that we can fill with our
	// result values.  Filter out invalid/nil objects.
	nonNilSources := make([]interface{}, 0, len(sources))
	nonNilDestinations := make([]map[string]interface{}, 0, len(destinations))
	originDestinations := make([]*outputNode, 0, len(destinations))
	for idx, source := range sources {
		value := reflect.ValueOf(source)
		if !value.IsValid() || (value.Kind() == reflect.Ptr && value.IsNil()) {
			destinations[idx].Fill(nil)
			continue
		}
		nonNilSources = append(nonNilSources, source)
		destMap := make(map[string]interface{}, len(selections))
		destinations[idx].Fill(destMap)
		nonNilDestinations = append(nonNilDestinations, destMap)
		originDestinations = append(originDestinations, destinations[idx])
	}

	// Number of Work Units = (NumExpensiveFields x NumSources) + NumNonExpensiveFields
	workUnits := make([]*WorkUnit, 0, numNonExpensive+(numExpensive*len(nonNilSources)))

	// for every selection, resolve the value or schedule an work unit for the field
	for _, selection := range selections {
		if selection.Name == "__typename" {
			for idx := range nonNilDestinations {
				nonNilDestinations[idx][selection.Alias] = typ.Name
			}
			continue
		}

		if ok, err := ShouldIncludeNode(selection.Directives); err != nil {
			return nil, nestPathError(selection.Alias, err)
		} else if !ok {
			continue
		}

		destForSelection := make([]*outputNode, 0, len(nonNilDestinations))
		for idx, destMap := range nonNilDestinations {
			filler := newOutputNode(originDestinations[idx], selection.Alias)
			destForSelection = append(destForSelection, filler)
			destMap[selection.Alias] = filler
		}

		field := typ.Fields[selection.Name]
		unit := &WorkUnit{
			Ctx:          ctx,
			field:        field,
			sources:      nonNilSources,
			destinations: destForSelection,
			selection:    selection,
			objectName:   typ.Name,
		}

		switch {
		case shouldUseBatch(ctx, field):
			unit.useBatch = true
			if field.NumParallelInvocationsFunc != nil {
				workUnits = append(workUnits, splitToNWorkUnits(unit, field.NumParallelInvocationsFunc(ctx, len(unit.sources)))...)
			} else {
				workUnits = append(workUnits, unit)
			}
		case field.Expensive:
			// Expensive fields should be executed as multiple "Units".  The scheduler
			// controls how the units are executed
			workUnits = append(workUnits, splitWorkUnit(unit)...)
		case field.External:
			// External non-Expensive fields should be fast (so we can run them at the
			// same time), but, since they are still external functions we don't want
			// to run them where they could potentially block.
			// So we create an work unit with all the fields to execute
			// asynchronously.
			if field.NumParallelInvocationsFunc != nil {
				workUnits = append(workUnits, splitToNWorkUnits(unit, field.NumParallelInvocationsFunc(ctx, len(unit.sources)))...)
			} else {
				workUnits = append(workUnits, unit)
			}
		default:
			// If the fields are not expensive or external the work time should be
			// bounded, so we can resolve them immediately.
			workUnits = append(
				workUnits,
				executeWorkUnit(unit)...,
			)
		}
	}

	if typ.KeyField != nil {
		destForSelection := make([]*outputNode, 0, len(nonNilDestinations))
		for idx, destMap := range nonNilDestinations {
			filler := newOutputNode(originDestinations[idx], "__key")
			destForSelection = append(destForSelection, filler)
			destMap["__key"] = filler
		}
		workUnits = append(
			workUnits,
			executeWorkUnit(&WorkUnit{
				Ctx:          ctx,
				field:        typ.KeyField,
				sources:      nonNilSources,
				destinations: destForSelection,
				selection:    &Selection{},
				objectName:   typ.Name,
			})...,
		)
	}

	return workUnits, nil
}

// shouldUseBatch determines whether we will execute this field as a batch
// based on the field information.
func shouldUseBatch(ctx context.Context, field *Field) bool {
	return field.Batch && field.UseBatchFunc(ctx)
}
