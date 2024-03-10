package rdma

import "golang.org/x/sys/unix"

type EpollWorker struct {
	ctx     ReadAble
	pipeFd  []int
	closeCh chan struct{}
}

func (ew *EpollWorker) initEpollWorker(ctx ReadAble) {
	ew.pipeFd = make([]int, 2)
	if err := unix.Pipe(ew.pipeFd); err != nil {
	}
	ew.closeCh = make(chan struct{})
	ew.ctx = ctx
	go func(epollWorker *EpollWorker) {
		buffer := make([]byte, 1024)
		for {
			_, err := unix.Read(epollWorker.pipeFd[0], buffer)
			if err != nil {
			}
			epollWorker.ctx()
		}
	}(ew)

}

func (ew *EpollWorker) epollWorkerAddJob() {
	_, err := unix.Write(ew.pipeFd[1], []byte("1"))
	if err != nil {
	}
}
