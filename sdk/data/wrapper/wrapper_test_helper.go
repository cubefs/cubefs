package wrapper

// InsertPartitionForTest inserts a DataPartition into Wrapper for testing.
func InsertPartitionForTest(w *Wrapper, dp *DataPartition) {
	if w.partitions == nil {
		w.partitions = make(map[uint64]*DataPartition)
	}
	w.replaceOrInsertPartition(dp)
}
