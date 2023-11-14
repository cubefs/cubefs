package storage

import "context"

type Interceptor interface {
	Before() (context.Context, error)
	After(ctx context.Context, n int64, err error)
}

type BeforeFunc func() (ctx context.Context, err error)
type AfterFunc func(ctx context.Context, n int64, err error)

type funcInterceptor struct {
	before BeforeFunc
	after  AfterFunc
}

func (i *funcInterceptor) Before() (context.Context, error) {
	if i.before != nil {
		return i.before()
	}
	return nil, nil
}

func (i *funcInterceptor) After(ctx context.Context, n int64, err error) {
	if i.after != nil {
		i.after(ctx, n, err)
		return
	}
	return
}

func NewFuncInterceptor(before BeforeFunc, after AfterFunc) Interceptor {
	return &funcInterceptor{
		before: before,
		after:  after,
	}
}

var noopInterceptor = NewFuncInterceptor(nil, nil)

type IOType int

const (
	IOWrite IOType = iota
	IORead
	IORemove
	IOSync
	__types
)

type IOInterceptors [__types]Interceptor

func (ioi *IOInterceptors) Get(typ IOType) Interceptor {
	if i := ioi[typ]; i != nil {
		return i
	}
	return noopInterceptor
}

func (ioi *IOInterceptors) Register(typ IOType, interceptor Interceptor) {
	ioi[typ] = interceptor
}
