package rdma

import "C"
import (
	"golang.org/x/sys/unix"
	"sync"
	"syscall"
)

var (
	epollFd = -1
	lock    sync.RWMutex
	once    sync.Once
)

type ReadAble func()
type EpollContext struct {
	cond *sync.Cond
}

type EPoll struct {
	epollFd int
	fds     map[int]*EpollWorker
}

var instance *EPoll

func GetEpoll() *EPoll {
	once.Do(func() {
		instance = &EPoll{}
		instance.init()
	})

	return instance
}

func (e *EPoll) init() {
	var err error
	e.epollFd, err = syscall.EpollCreate(256)
	if err != nil {
		panic(err)
	}
	instance.fds = make(map[int]*EpollWorker)

	go e.epollLoop()
}

func (e *EPoll) getContext(fd int) *EpollWorker {
	lock.RLock()
	defer lock.RUnlock()
	return e.fds[fd]
}

func (e *EPoll) EpollAdd(fd int, ctx ReadAble) {
	event := syscall.EpollEvent{}
	event.Events = unix.EPOLLIN | unix.EPOLLET
	event.Fd = int32(fd)
	lock.Lock()
	ew := &EpollWorker{}
	ew.initEpollWorker(ctx)
	e.fds[fd] = ew
	lock.Unlock()
	syscall.EpollCtl(e.epollFd, syscall.EPOLL_CTL_ADD, fd, &event)
}

func (e *EPoll) EpollDel(fd int) {
	/*
		lock.Lock()
		delete(e.fds, fd)
		lock.Unlock()
		syscall.EpollCtl(e.epollFd, syscall.EPOLL_CTL_DEL, fd, nil)
	*/
}

func (e *EPoll) epollLoop() error {
	events := make([]syscall.EpollEvent, 100)
	for {
		n, err := syscall.EpollWait(e.epollFd, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return err
		}
		for i := 0; i < n; i++ {
			/* serial
			if eventFunction := e.getContext(int(events[i].Fd)); eventFunction != nil {
				eventFunction()
			}
			*/
			//parallel
			if epollWorker := e.getContext(int(events[i].Fd)); epollWorker != nil {
				epollWorker.epollWorkerAddJob()
			}
		}
	}
}
