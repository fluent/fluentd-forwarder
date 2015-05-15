//
// Fluentd Forwarder
//
// Copyright (C) 2014 Treasure Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

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
