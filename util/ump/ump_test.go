package ump

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestUmp(t *testing.T) {

	InitUmp("ebs")
	for i := 0; i < 100; i++ {

		go func() {
			for {
				up := BeforeTP("wocao" + strconv.FormatInt(rand.Int63(), 16))
				AfterTP(up, nil)
				Alive("nimei")
				Alarm("baojingle", "weishenmene")
			}
		}()
	}

	for i := 0; i < 100; i++ {
		time.Sleep(time.Second)
	}
}
