package common

func NewConcurrencyControl(num int32) (cControl *concurrencyControl) {
	cControl = &concurrencyControl{
		c:   make(chan struct{}, num),
		num: num,
	}
	return
}

type concurrencyControl struct {
	c   chan struct{}
	num int32
}

func (cControl *concurrencyControl) Add() {
	cControl.c <- struct{}{}
}

func (cControl *concurrencyControl) Realse() {
	<-cControl.c
}

func (cControl *concurrencyControl) Close() {
	close(cControl.c)
}
