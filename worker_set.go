package fluentd_forwarder

import (
	"sync"
)

type WorkerSet struct {
	workers map[Worker]struct{}
	mtx     sync.Mutex
}

func (set *WorkerSet) Add(worker Worker) {
	set.mtx.Lock()
	defer set.mtx.Unlock()
	set.workers[worker] = struct{}{}
}

func (set *WorkerSet) Remove(worker Worker) {
	set.mtx.Lock()
	defer set.mtx.Unlock()
	delete(set.workers, worker)
}

func (set *WorkerSet) Slice() []Worker {
	retval := make([]Worker, len(set.workers))
	set.mtx.Lock()
	defer set.mtx.Unlock()
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
