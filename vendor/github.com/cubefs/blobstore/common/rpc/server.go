package rpc

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type (
	// HandlerFunc defines the handler of app function
	HandlerFunc func(*Context)

	// ServerOption server option applier
	// Order: if args in body ignore others options,
	//        else uri > query > form > postfrom
	ServerOption interface {
		apply(*serverOptions)
	}
	serverOptions struct {
		argsBody     bool
		argsURI      bool
		argsQuery    bool
		argsForm     bool
		argsPostForm bool

		metaCapacity int
	}
	funcServerOption struct {
		f func(*serverOptions)
	}
)

func (so *serverOptions) copy() *serverOptions {
	return &serverOptions{
		argsBody:     so.argsBody,
		argsURI:      so.argsURI,
		argsQuery:    so.argsQuery,
		argsForm:     so.argsForm,
		argsPostForm: so.argsPostForm,

		metaCapacity: so.metaCapacity,
	}
}

func (so *serverOptions) hasArgs() bool {
	return so.argsBody || so.argsURI || so.argsQuery || so.argsForm || so.argsPostForm
}

func (fo *funcServerOption) apply(f *serverOptions) {
	fo.f(f)
}

func newFuncServerOption(f func(*serverOptions)) *funcServerOption {
	return &funcServerOption{
		f: f,
	}
}

// OptArgsBody argument in request body
func OptArgsBody() ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.argsBody = true
	})
}

// OptArgsURI argument in uri
func OptArgsURI() ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.argsURI = true
	})
}

// OptArgsQuery argument in query string
func OptArgsQuery() ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.argsQuery = true
	})
}

// OptArgsForm argument in form
func OptArgsForm() ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.argsForm = true
	})
}

// OptArgsPostForm argument in post form
func OptArgsPostForm() ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.argsPostForm = true
	})
}

// OptMetaCapacity initial meta capacity
func OptMetaCapacity(capacity int) ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		if capacity >= 0 {
			o.metaCapacity = capacity
		}
	})
}

// makeHandler make handle of httprouter
func makeHandler(handlers []HandlerFunc, opts ...ServerOption) httprouter.Handle {
	opt := new(serverOptions)
	for _, o := range opts {
		o.apply(opt)
	}

	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		c := &Context{
			opts:  opt,
			Param: ps,

			Request: r,
			Writer:  w,

			Meta: make(map[string]interface{}, opt.metaCapacity),

			index:    -1,
			handlers: handlers,
		}
		c.Next()
		if !c.wroteHeader {
			c.RespondStatus(http.StatusOK)
		}
	}
}
