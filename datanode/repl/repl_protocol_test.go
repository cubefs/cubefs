package repl

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPutResponseCallsPostWhenChannelFull(t *testing.T) {
	var postCalled int32
	rp := &ReplProtocol{
		responseCh:      make(chan *Packet, 1),
		toBeProcessedCh: make(chan *Packet, 1),
		postFunc: func(p *Packet) error {
			atomic.AddInt32(&postCalled, 1)
			return nil
		},
	}

	rp.responseCh <- &Packet{}
	err := rp.putResponse(&Packet{})

	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&postCalled))
}

func TestPutToBeProcessCallsPostWhenChannelFull(t *testing.T) {
	var postCalled int32
	rp := &ReplProtocol{
		responseCh:      make(chan *Packet, 1),
		toBeProcessedCh: make(chan *Packet, 1),
		postFunc: func(p *Packet) error {
			atomic.AddInt32(&postCalled, 1)
			return nil
		},
	}

	rp.toBeProcessedCh <- &Packet{}
	err := rp.putToBeProcess(&Packet{})

	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&postCalled))
}
