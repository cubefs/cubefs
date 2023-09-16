package storage

import "context"

type Interceptor interface {
	Before() (ctx context.Context, abort bool)
	After(context.Context)
}

type BeforeFunc func() (ctx context.Context, abort bool)
type AfterFunc func(context.Context)

type funcInterceptor struct {
	before BeforeFunc
	after  AfterFunc
}

func (i *funcInterceptor) Before() (ctx context.Context, abort bool) {
	if i.before != nil {
		return i.before()
	}
	return nil, false
}

func (i *funcInterceptor) After(ctx context.Context) {
	if i.after != nil {
		i.after(ctx)
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
