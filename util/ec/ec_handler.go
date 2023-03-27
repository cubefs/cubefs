package ec

import (
	"github.com/cubefs/cubefs/util/errors"
	"github.com/klauspost/reedsolomon"
)

type EcHandler struct {
	blockSize int

	dataShardsNum   int
	parityShardsNum int

	encoder reedsolomon.Encoder
}

func NewEcHandler(blockSize, dataShardsNum, parityShardsNum int) (ech *EcHandler, err error) {
	var enc reedsolomon.Encoder
	if enc, err = reedsolomon.New(dataShardsNum, parityShardsNum); err != nil {
		return
	}

	ech = new(EcHandler)

	ech.blockSize = blockSize
	ech.dataShardsNum = dataShardsNum
	ech.parityShardsNum = parityShardsNum

	ech.encoder = enc

	return
}

func NewEcCoder(blockSize, dataShardsNum, parityShardsNum int) (ech *EcHandler, err error) {
	return NewEcHandler(blockSize, dataShardsNum, parityShardsNum)
}

func (ech *EcHandler) GetBlockSize() int {
	return ech.blockSize
}

func (ech *EcHandler) GetShardsNum() (data, parity int) {
	data = ech.dataShardsNum
	parity = ech.parityShardsNum
	return
}
func (ech *EcHandler) GetEncodeSize() int {
	return ech.blockSize * ech.dataShardsNum
}

func (ech *EcHandler) Encode(data []byte) (shards [][]byte, err error) {
	if len(data) != ech.blockSize*ech.dataShardsNum {
		err = errors.New("unmatched encode size")
		return
	}

	enc := ech.encoder

	if cap(data) > len(data) {
		data_tmp := make([]byte, len(data))
		copy(data_tmp, data)
		data = data_tmp
	}

	var shards_tmp [][]byte
	if shards_tmp, err = enc.Split(data); err != nil {
		return
	}

	if err = enc.Encode(shards_tmp); err != nil {
		return
	}

	shards = shards_tmp

	return
}

func (ech *EcHandler) Reconstruct(shards [][]byte) (err error) {
	if len(shards) != ech.dataShardsNum+ech.parityShardsNum {
		err = errors.New("unmatched number of shards")
		return err
	}

	lost := 0
	for _, shard := range shards {
		if shard == nil {
			lost++
			if lost > ech.parityShardsNum {
				err = errors.New("too many lost shards")
				return
			}
			continue
		}
		if len(shard) != ech.blockSize {
			err = errors.New("unmatched shard size")
			return
		}
	}

	err = ech.encoder.Reconstruct(shards)

	return
}

func (ech *EcHandler) Verify(shards [][]byte) (verify bool, err error) {
	return ech.encoder.Verify(shards)
}
