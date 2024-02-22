package rdma

type EpollWorker struct {
	ctx     ReadAble
	jobs    chan int
	closeCh chan struct{}
}

func (ew *EpollWorker) initEpollWorker(ctx ReadAble) {
	ew.jobs = make(chan int, 100)
	ew.closeCh = make(chan struct{})
	ew.ctx = ctx
	go func(epollWorker *EpollWorker) {
		for {
			select {
			case <-epollWorker.jobs:
				//println("exec eventFunc")
				epollWorker.ctx()
				//ew.wg.Done()
			case <-epollWorker.closeCh:
				return
			}
		}
	}(ew)

}

func (ew *EpollWorker) epollWorkerAddJob() {
	//println("exec add job")
	ew.jobs <- 1
}
