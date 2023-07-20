package pool

type TaskPool struct {
	pool chan func()
}

func New(poolSize, poolCacheSize int) TaskPool {
	pool := make(chan func(), poolCacheSize)
	for i := 0; i < poolSize; i++ {
		go func() {
			for {
				task, ok := <-pool
				if !ok {
					break
				}
				task()
			}
		}()
	}

	return TaskPool{pool: pool}
}

func (t TaskPool) Run(task func()) {
	t.pool <- task
}

func (t TaskPool) TryRun(task func())  bool {
	select {
	case t.pool<-task:
		return true
	default:
		return false
	}
}

func (t TaskPool) Close()  {
	close(t.pool)
}

func GetPoolSize(poolSize, length int) int  {
	if length < poolSize {
		return length
	}
	return poolSize
}