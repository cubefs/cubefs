package async

type Future struct {
	resultC chan interface{}
	errorC  chan error
}

func NewFuture() *Future {
	return &Future{
		resultC: make(chan interface{}, 1),
		errorC:  make(chan error, 1),
	}
}

func (f *Future) Respond(result interface{}, err error) {
	if err != nil {
		f.errorC <- err
		return
	}
	f.resultC <- result
}

// Response wait response
func (f *Future) Response() (result interface{}, err error) {
	select {
	case err = <-f.errorC:
		return
	case result = <-f.resultC:
		return
	}
}
