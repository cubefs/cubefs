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
