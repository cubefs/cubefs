package lcnode

var (
	defaultUChanInitCapacity         int = 10000
	defaultS3ScanRoutineNumPerTask   int = 100
	defaultSnapShotRoutineNumPerTask int = 100
	maxRoutineNumPerTask             int = 5000
	defaultMaxChanUnm                int = 10000000 //worst case: may take up around 400MB of heap space
)
