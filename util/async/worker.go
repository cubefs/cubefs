package async

type WorkerFunc func()

type PanicHandler func(interface{})

func RunWorker(f WorkerFunc, handler PanicHandler) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				if handler != nil{
					handler(r)
				}
			}
		}()
		if f != nil {
			f()
		}
	}()
}
