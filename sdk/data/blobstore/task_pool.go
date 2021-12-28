package blobstore

type Instance struct {
	mq chan task
}

type task struct {
	op *rwSlice
	fn func(op *rwSlice)
}

func New(worker int, size int) Instance {
	mq := make(chan task, size)
	for i := 0; i < worker; i++ {
		go func() {
			for {
				task, ok := <-mq
				if !ok {
					break
				}
				task.fn(task.op)
			}
		}()
	}
	return Instance{mq}
}

func (r Instance) Execute(op *rwSlice, fn func(op *rwSlice)) {
	r.mq <- task{
		op: op,
		fn: fn,
	}
}

func (r Instance) Close() {
	close(r.mq)
}
