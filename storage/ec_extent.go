package storage

func (e *Extent) tinyExtentAvaliAndHoleOffset(offset int64) (dataOffset, holeOffset int64, err error) {
	e.Lock()
	defer e.Unlock()
	dataOffset, err = e.file.Seek(offset, SEEK_DATA)
	if err != nil {
		return
	}
	holeOffset, err = e.file.Seek(offset, SEEK_HOLE)
	if err != nil {
		return
	}
	return
}