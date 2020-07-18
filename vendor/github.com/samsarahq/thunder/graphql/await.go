package graphql

import "fmt"

func await(value interface{}) (interface{}, error) {
	switch value := value.(type) {
	case *thunk:
		return value.await()

	case map[string]interface{}:
		for k, v := range value {
			v, err := await(v)
			if err != nil {
				return nil, nestPathError(k, err)
			}
			value[k] = v
		}

	case []interface{}:
		for i, v := range value {
			v, err := await(v)
			if err != nil {
				return nil, nestPathError(fmt.Sprint(i), err)
			}
			value[i] = v
		}
	}

	return value, nil
}

type thunk struct {
	value interface{}
	err   error
	done  chan struct{}
}

func fork(f func() (interface{}, error)) *thunk {
	t := &thunk{
		done: make(chan struct{}, 0),
	}

	go func() {
		t.value, t.err = f()
		close(t.done)
	}()

	return t
}

func (t *thunk) await() (interface{}, error) {
	<-t.done
	return t.value, t.err
}
