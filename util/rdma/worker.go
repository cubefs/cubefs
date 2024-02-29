package rdma

import "golang.org/x/sys/unix"

type EpollWorker struct {
	ctx ReadAble
	//jobs    chan int
	pipeFd  []int
	closeCh chan struct{}
}

func (ew *EpollWorker) initEpollWorker(ctx ReadAble) {
	//ew.jobs = make(chan int, 500)
	ew.pipeFd = make([]int, 2)
	if err := unix.Pipe(ew.pipeFd); err != nil {
		//TODO
	}
	ew.closeCh = make(chan struct{})
	ew.ctx = ctx
	go func(epollWorker *EpollWorker) {
		buffer := make([]byte, 1024)
		for {
			/*
				select {
				case <-epollWorker.jobs:
					//println("exec eventFunc")
					epollWorker.ctx()
					//ew.wg.Done()
				case <-epollWorker.closeCh:
					return
				}
			*/
			_, err := unix.Read(epollWorker.pipeFd[0], buffer)
			if err != nil {
				//TODO
			}
			epollWorker.ctx()
		}
	}(ew)

}

func (ew *EpollWorker) epollWorkerAddJob() {
	//println("exec add job")
	_, err := unix.Write(ew.pipeFd[1], []byte("1"))
	if err != nil {
		//TODO
	}
}
