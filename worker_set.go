package fluentd_forwarder

import (
	"sync"
)

type WorkerSet struct {
	workers map[Worker]struct{}
	mtx     sync.Mutex
}

func (set *WorkerSet) Add(worker Worker) {
	defer set.mtx.Unlock()
	set.mtx.Lock()
	set.workers[worker] = struct{}{}
}

func (set *WorkerSet) Remove(worker Worker) {
	defer set.mtx.Unlock()
	set.mtx.Lock()
	delete(set.workers, worker)
}

func (set *WorkerSet) Slice() []Worker {
	retval := make([]Worker, len(set.workers))
	defer set.mtx.Unlock()
	set.mtx.Lock()
	i := 0
	for worker, _ := range set.workers {
		retval[i] = worker
		i += 1
	}
	return retval
}

func NewWorkerSet() *WorkerSet {
	return &WorkerSet{
		workers: make(map[Worker]struct{}),
		mtx:     sync.Mutex{},
	}
}
