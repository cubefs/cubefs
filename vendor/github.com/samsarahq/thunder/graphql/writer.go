package graphql

import (
	"encoding/json"
	"sync"
)

// errorRecorder is a concurrency-safe way where we can record the first error
// we get from executing the graphql query.
type errorRecorder struct {
	sync.Once
	err error
}

func (e *errorRecorder) record(err error) {
	if err == nil {
		return
	}
	e.Do(func() {
		e.err = err
	})
}

type pathTracker struct {
	parent *pathTracker
	path   string
}

func (p *pathTracker) getPath() []string {
	path := make([]string, 0)
	cur := p
	for cur != nil {
		if cur.path != "" {
			path = append(path, cur.path)
		}
		cur = cur.parent
	}
	return path
}

// newTopLevelOutputNode creates a top-level object writer, this should be
// the object writer that starts the graphql query.
func newTopLevelOutputNode(path string) *outputNode {
	return &outputNode{
		pathTracker: &pathTracker{path: path},
		errRecorder: &errorRecorder{},
	}
}

// newOutputNode creates an object writer as a part of a chain of objects.
// It keeps track of the path and current parent so we can properly propagate
// error information up the stack.
func newOutputNode(parent *outputNode, path string) *outputNode {
	return &outputNode{
		pathTracker: &pathTracker{parent: parent.pathTracker, path: path},
		errRecorder: parent.errRecorder,
	}
}

type outputNode struct {
	pathTracker *pathTracker
	res         interface{}
	errRecorder *errorRecorder
}

func (o *outputNode) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.res)
}

func (o *outputNode) Fill(res interface{}) {
	o.res = res
}

func (o *outputNode) Fail(err error) {
	path := o.getPath()
	err = nestPathErrorMulti(path, err)
	o.errRecorder.record(err)
}

// getPath traverses the parent list to get the current execution path.
func (o *outputNode) getPath() []string {
	return o.pathTracker.getPath()
}

// Unwraps the object writer JSON map to a "regular" JSON comparable type.
func outputNodeToJSON(src interface{}) interface{} {
	switch src := src.(type) {
	case map[string]*outputNode:
		newMap := make(map[string]interface{}, len(src))
		for key, val := range src {
			newMap[key] = outputNodeToJSON(val)
		}
		return newMap
	case []*outputNode:
		newList := make([]interface{}, len(src))
		for idx, val := range src {
			newList[idx] = outputNodeToJSON(val)
		}
		return newList
	case *outputNode:
		return outputNodeToJSON(src.res)
	case []interface{}:
		for idx := range src {
			src[idx] = outputNodeToJSON(src[idx])
		}
		return src
	case map[string]interface{}:
		for key := range src {
			src[key] = outputNodeToJSON(src[key])
		}
		return src
	default:
		return src
	}
}
