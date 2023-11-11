package async

type WorkerFunc func()

type PanicHandler func(interface{})

func RunWorker(f WorkerFunc, handlers ...PanicHandler) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				for _, handler := range handlers {
					if handler != nil {
						handler(r)
					}
				}
			}
		}()
		if f != nil {
			f()
		}
	}()
}

type ParamWorkerFunc func(...interface{})

func (f ParamWorkerFunc) RunWith(args ...interface{}) {
	if f != nil {
		f(args...)
	}
}

func ParamWorker(f ParamWorkerFunc, handlers ...PanicHandler) ParamWorkerFunc {
	return func(args ...interface{}) {
		go func(args ...interface{}) {
			defer func() {
				if r := recover(); r != nil {
					for _, handler := range handlers {
						if handler != nil {
							handler(r)
						}
					}
				}
			}()
			if f != nil {
				f.RunWith(args...)
			}
		}(args...)
	}
}
