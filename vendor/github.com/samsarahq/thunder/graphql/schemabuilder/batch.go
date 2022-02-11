package schemabuilder

import (
	"context"
	"fmt"
	"reflect"

	"github.com/samsarahq/thunder/batch"
	"github.com/samsarahq/thunder/graphql"
)

// buildBatchFunction corresponds to buildFunction for a batchFieldFunc
func (sb *schemaBuilder) buildBatchFunctionWithFallback(typ reflect.Type, m *method) (*graphql.Field, error) {
	fallbackField, fallbackFuncCtx, err := sb.buildFunctionAndFuncCtx(typ, &method{
		Fn:                m.BatchArgs.FallbackFunc,
		MarkedNonNullable: m.MarkedNonNullable,
		// We don't want to accidentally make the fallback non-expensive
		// if someone forgets to pass the Expensive option to `BatchFieldFuncWithFallback`.
		// It's safe to assume the fallback is expensive, because why else
		// would someone bother batching it?
		Expensive: true,
	})
	if err != nil {
		return nil, err
	}

	batchField, batchFuncCtx, err := sb.buildBatchFunctionAndFuncCtx(typ, m)
	if err != nil {
		return nil, err
	}
	if fallbackFuncCtx.hasContext != batchFuncCtx.hasContext ||
		!fallbackFuncCtx.hasSource || // Batch func always has a source.
		fallbackFuncCtx.hasArgs != batchFuncCtx.hasArgs ||
		fallbackFuncCtx.hasSelectionSet != batchFuncCtx.hasSelectionSet ||
		fallbackFuncCtx.hasError != batchFuncCtx.hasError ||
		fallbackFuncCtx.hasRet != batchFuncCtx.hasRet {
		return nil, fmt.Errorf("batch and fallback function signatures did not match")
	}

	if fallbackField.Type.String() != batchField.Type.String() {
		return nil, fmt.Errorf("batch and fallback graphql return types did not match: Batch(%v) Fallback(%v)", batchField.Type, fallbackField.Type)
	}

	if len(fallbackField.Args) != len(batchField.Args) {
		return nil, fmt.Errorf("batch and fallback arg type did not match: Batch(%v) Fallback(%v)", batchField.Args, fallbackField.Args)
	}
	for key, fallbackTyp := range fallbackField.Args {
		if batchType, ok := batchField.Args[key]; !ok || fallbackTyp.String() != batchType.String() {
			return nil, fmt.Errorf("batch and fallback func arg types did not match: Batch(%v) Fallback(%v)", batchType, fallbackTyp)
		}
	}

	if m.BatchArgs.ShouldUseBatchFunc == nil {
		return nil, fmt.Errorf("batch function requires fallback check function (got nil)")
	}

	batchField.UseBatchFunc = m.BatchArgs.ShouldUseBatchFunc
	batchField.Resolve = fallbackField.Resolve
	return batchField, nil
}

func (sb *schemaBuilder) buildBatchFunction(typ reflect.Type, m *method) (*graphql.Field, error) {
	batchField, _, err := sb.buildBatchFunctionAndFuncCtx(typ, m)
	if err != nil {
		return nil, err
	}
	batchField.UseBatchFunc = func(context.Context) bool {
		return true
	}
	return batchField, nil
}

// buildBatchFunction corresponds to buildFunction for a batchFieldFunc
func (sb *schemaBuilder) buildBatchFunctionAndFuncCtx(typ reflect.Type, m *method) (*graphql.Field, *batchFuncContext, error) {
	funcCtx := &batchFuncContext{parentTyp: typ}

	if typ.Kind() == reflect.Ptr {
		return nil, nil, fmt.Errorf("source-type of buildBatchFunction cannot be a pointer (got: %v)", typ)
	}

	callableFunc, err := funcCtx.getFuncVal(m)
	if err != nil {
		return nil, nil, err
	}

	in := funcCtx.getFuncInputTypes()
	if len(in) == 0 {
		return nil, nil, fmt.Errorf("batch Field funcs require at least one input field")
	}

	in = funcCtx.consumeContext(in)
	in, err = funcCtx.consumeRequiredSourceBatch(in)
	if err != nil {
		return nil, nil, err
	}
	argParser, args, in, err := funcCtx.consumeArgs(sb, in)
	if err != nil {
		return nil, nil, err
	}
	in = funcCtx.consumeSelectionSet(in)

	// We have succeeded if no arguments remain.
	if len(in) != 0 {
		return nil, nil, fmt.Errorf("%s arguments should be [context,]map[int][*]%s[, args][, selectionSet]", funcCtx.funcType, typ)
	}

	out := funcCtx.getFuncOutputTypes()
	retType, out, err := funcCtx.consumeReturnValue(m, sb, out)
	if err != nil {
		return nil, nil, err
	}
	out = funcCtx.consumeReturnError(out)
	if len(out) > 0 {
		return nil, nil, fmt.Errorf("%s return should be [map[int]<Type>][,error]", funcCtx.funcType)
	}

	batchExecFunc := func(ctx context.Context, sources []interface{}, funcRawArgs interface{}, selectionSet *graphql.SelectionSet) ([]interface{}, error) {
		// Set up function arguments.
		funcInputArgs, idxValues := funcCtx.prepareResolveArgs(sources, funcRawArgs, ctx, selectionSet)

		// Call the function.
		funcOutputArgs := callableFunc.Call(funcInputArgs)

		return funcCtx.extractResultsAndErr(funcOutputArgs, idxValues, retType)
	}

	return &graphql.Field{
		BatchResolver:              batchExecFunc,
		Batch:                      true,
		External:                   true,
		Args:                       args,
		Type:                       retType,
		ParseArguments:             argParser.Parse,
		Expensive:                  m.Expensive,
		NumParallelInvocationsFunc: m.ConcurrencyArgs.numParallelInvocationsFunc,
	}, funcCtx, nil
}

// funcContext is used to parse the function signature in buildFunction.
type batchFuncContext struct {
	hasContext      bool
	hasArgs         bool
	hasSelectionSet bool
	hasRet          bool
	hasError        bool

	enforceNoNilResps bool

	funcType     reflect.Type
	batchMapType reflect.Type
	isPtrFunc    bool
	parentTyp    reflect.Type
}

// getFuncVal returns a reflect.Value of an executable function.
func (funcCtx *batchFuncContext) getFuncVal(m *method) (reflect.Value, error) {
	fun := reflect.ValueOf(m.Fn)
	if fun.Kind() != reflect.Func {
		return fun, fmt.Errorf("fun must be func, not %s", fun)
	}
	funcCtx.funcType = fun.Type()
	return fun, nil
}

// getFuncInputTypes returns the input arguments for the function we're
// representing.
func (funcCtx *batchFuncContext) getFuncInputTypes() []reflect.Type {
	in := make([]reflect.Type, 0, funcCtx.funcType.NumIn())
	for i := 0; i < funcCtx.funcType.NumIn(); i++ {
		in = append(in, funcCtx.funcType.In(i))
	}
	return in
}

// consumeContext reads in the input parameters for the provided
// function and determines whether the function has a Context input parameter.
// It returns the input types without the context parameter if it was there.
func (funcCtx *batchFuncContext) consumeContext(in []reflect.Type) []reflect.Type {
	if len(in) > 0 && in[0] == contextType {
		funcCtx.hasContext = true
		in = in[1:]
	}
	return in
}

// consumeRequiredSourceBatch reads in the input parameters for the provided
// function and guarantees that the input parameters include a batch of the
// parent type (map[int]*ParentObject).  If we don't have the batch we return an
// error because the function is invalid.
func (funcCtx *batchFuncContext) consumeRequiredSourceBatch(in []reflect.Type) ([]reflect.Type, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("requires batch source input parameter for func")
	}
	inType := in[0]
	in = in[1:]

	parentPtrType := reflect.PtrTo(funcCtx.parentTyp)
	if inType.Kind() != reflect.Map ||
		!isBatchIndexType(inType.Key()) ||
		(inType.Elem() != parentPtrType && inType.Elem() != funcCtx.parentTyp) {
		return nil, fmt.Errorf(
			"invalid source batch type, expected one of map[batch.Index]*%s or map[batch.Index]%s, but got %s",
			funcCtx.parentTyp.String(),
			funcCtx.parentTyp.String(),
			inType.String(),
		)
	}

	funcCtx.isPtrFunc = inType.Elem() == parentPtrType
	funcCtx.batchMapType = inType

	return in, nil
}

// consumeArgs reads the args parameter if it is there and returns an argParser,
// argTypeMap and the filtered input parameters.
func (funcCtx *batchFuncContext) consumeArgs(sb *schemaBuilder, in []reflect.Type) (*argParser, map[string]graphql.Type, []reflect.Type, error) {
	if len(in) == 0 || in[0] == selectionSetType {
		return nil, nil, in, nil
	}
	inType := in[0]
	in = in[1:]
	argParser, argType, err := sb.makeStructParser(inType)
	if err != nil {
		return nil, nil, in, fmt.Errorf("attempted to parse %s as arguments struct, but failed: %s", inType.Name(), err.Error())
	}
	inputObject, ok := argType.(*graphql.InputObject)
	if !ok {
		return nil, nil, nil, fmt.Errorf("%s's args should be an object", funcCtx.funcType)
	}
	args := make(map[string]graphql.Type, len(inputObject.InputFields))
	for name, typ := range inputObject.InputFields {
		args[name] = typ
	}
	funcCtx.hasArgs = true
	return argParser, args, in, nil
}

// consumeSelectionSet reads the input parameters and will pop off the
// selectionSet type if we detect it in the input fields.  Check out
// graphql.SelectionSet for more infomation about selection sets.
func (funcCtx *batchFuncContext) consumeSelectionSet(in []reflect.Type) []reflect.Type {
	if len(in) > 0 && in[0] == selectionSetType {
		in = in[1:]
		funcCtx.hasSelectionSet = true
	}
	return in
}

func (funcCtx *batchFuncContext) getFuncOutputTypes() []reflect.Type {
	out := make([]reflect.Type, 0, funcCtx.funcType.NumOut())
	for i := 0; i < funcCtx.funcType.NumOut(); i++ {
		out = append(out, funcCtx.funcType.Out(i))
	}
	return out
}

// consumeReturnValue consumes the function output's response value if it exists
// and validates that the response is a proper batch type.
func (funcCtx *batchFuncContext) consumeReturnValue(m *method, sb *schemaBuilder, out []reflect.Type) (graphql.Type, []reflect.Type, error) {
	if len(out) == 0 || out[0] == errType {
		retType, err := sb.getType(reflect.TypeOf(true))
		if err != nil {
			return nil, nil, err
		}
		return retType, out, nil
	}
	outType := out[0]
	out = out[1:]
	if outType.Kind() != reflect.Map ||
		!isBatchIndexType(outType.Key()) {
		return nil, nil, fmt.Errorf(
			"invalid response batch type, expected map[batch.Index]<Type>, but got %s",
			outType.String(),
		)
	}
	retType, err := sb.getType(outType.Elem())
	if err != nil {
		return nil, nil, err
	}
	if nonNull, ok := retType.(*graphql.NonNull); ok {
		if _, isList := nonNull.Type.(*graphql.List); !isList {
			// Batch functions don't support NonNull responses by default unless they
			// are lists we can fill with zero length values.
			retType = nonNull.Type
		}
	}
	if m.MarkedNonNullable {
		funcCtx.enforceNoNilResps = true
		if _, ok := retType.(*graphql.NonNull); !ok {
			retType = &graphql.NonNull{Type: retType}
		}
	}
	funcCtx.hasRet = true
	return retType, out, nil
}

var batchIndexTyp reflect.Type

func init() {
	var batchIndexPointer *batch.Index
	batchIndexTyp = reflect.TypeOf(batchIndexPointer).Elem()
}

func isBatchIndexType(t reflect.Type) bool {
	return t == batchIndexTyp
}

// consumeReturnValue consumes the function output's error type if it exists.
func (funcCtx *batchFuncContext) consumeReturnError(out []reflect.Type) []reflect.Type {
	if len(out) > 0 && out[0] == errType {
		funcCtx.hasError = true
		out = out[1:]
	}
	return out
}

// prepareResolveArgs converts the provided source, args and context into the
// required list of reflect.Value types that the function needs to be called.
func (funcCtx *batchFuncContext) prepareResolveArgs(sources []interface{}, args interface{}, ctx context.Context, selectionSet *graphql.SelectionSet) (in []reflect.Value, idxValues []reflect.Value) {
	in = make([]reflect.Value, 0, funcCtx.funcType.NumIn())
	if funcCtx.hasContext {
		in = append(in, reflect.ValueOf(ctx))
	}

	batchMap := reflect.MakeMapWithSize(funcCtx.batchMapType, len(sources))
	idxValues = make([]reflect.Value, len(sources))
	for idx, source := range sources {
		idxVal := idx
		sourceValue := reflect.ValueOf(source)
		ptrSource := sourceValue.Kind() == reflect.Ptr
		idxValues[idxVal] = reflect.ValueOf(batch.NewIndex(idxVal))
		switch {
		case ptrSource && !funcCtx.isPtrFunc:
			batchMap.SetMapIndex(idxValues[idxVal], sourceValue.Elem())
		case !ptrSource && funcCtx.isPtrFunc:
			copyPtr := reflect.New(funcCtx.parentTyp)
			copyPtr.Elem().Set(sourceValue)
			batchMap.SetMapIndex(idxValues[idxVal], copyPtr)
		default:
			batchMap.SetMapIndex(idxValues[idxVal], sourceValue)
		}
	}
	in = append(in, batchMap)

	// Set up other arguments.
	if funcCtx.hasArgs {
		in = append(in, reflect.ValueOf(args))
	}
	if funcCtx.hasSelectionSet {
		in = append(in, reflect.ValueOf(selectionSet))
	}

	return in, idxValues
}

// extractResultsAndErr converts the response from calling the function into
// the expected type for the response object (as opposed to a reflect.Value).
// It also handles reading whether the function ended with errors.
func (funcCtx *batchFuncContext) extractResultsAndErr(out []reflect.Value, idxValues []reflect.Value, retType graphql.Type) ([]interface{}, error) {
	if funcCtx.hasError {
		if err := out[len(out)-1]; !err.IsNil() {
			return nil, err.Interface().(error)
		}
	}
	if !funcCtx.hasRet {
		res := make([]interface{}, len(idxValues))
		for i := 0; i < len(idxValues); i++ {
			res[i] = true
		}
		return res, nil
	}
	resBatch := out[0]

	resList := make([]interface{}, len(idxValues))
	for idx, idxVal := range idxValues {
		res := resBatch.MapIndex(idxVal)
		if !res.IsValid() || (res.Kind() == reflect.Ptr && res.IsNil()) {
			if funcCtx.enforceNoNilResps {
				return nil, fmt.Errorf("%s is marked non-nullable but returned a null value", funcCtx.funcType)
			}
			continue
		}
		resList[idx] = res.Interface()
	}
	return resList, nil
}
