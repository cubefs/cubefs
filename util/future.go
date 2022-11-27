package util

type respErr struct {
	errCh chan error
}

func (e *respErr) init() {
	e.errCh = make(chan error, 1)
}

func (e *respErr) respond(err error) {
	e.errCh <- err
	close(e.errCh)
}

func (e *respErr) error() <-chan error {
	return e.errCh
}

// Future the future
type Future struct {
	respErr
	respCh chan interface{}
}

func NewFuture() *Future {
	f := &Future{
		respCh: make(chan interface{}, 1),
	}
	f.init()
	return f
}

func (f *Future) Respond(resp interface{}, err error) {
	if err == nil {
		f.respCh <- resp
		close(f.respCh)
	} else {
		f.respErr.respond(err)
	}
}

// Response wait response
func (f *Future) Response() (resp interface{}, err error) {
	select {
	case err = <-f.error():
		return
	case resp = <-f.respCh:
		return
	}
}

// AsyncResponse export channels
func (f *Future) AsyncResponse() (respCh <-chan interface{}, errCh <-chan error) {
	return f.respCh, f.errCh
}
