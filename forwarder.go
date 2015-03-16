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

package main

import (
	"fmt"
	"io"
)

type FluentRecord struct {
	Tag       string
	Timestamp uint64
	Data      map[string]interface{}
}

type TinyFluentRecord struct {
	Timestamp uint64
	Data      map[string]interface{}
}

type FluentRecordSet struct {
	Tag     string
	Records []TinyFluentRecord
}

type FluentRecordBuf struct {
	Length  int
	Data    [8192]byte
}

type Port interface {
	Emit(recordSets []FluentRecordSet) error
	EmitRaw(recordBuf FluentRecordBuf) error
}

type Worker interface {
	String() string
	Start()
	Stop()
	WaitForShutdown()
}

type Disposable interface {
	Dispose() error
}

type JournalChunk interface {
	Disposable
	Id() string
	String() string
	Size() (int64, error)
	Reader() (io.ReadCloser, error)
	NextChunk() JournalChunk
	MD5Sum() ([]byte, error)
	Dup() JournalChunk
}

type JournalChunkListener interface {
	NewChunkCreated(JournalChunk) error
	ChunkFlushed(JournalChunk) error
}

type Journal interface {
	Disposable
	Key() string
	Write(data []byte) error
	TailChunk() JournalChunk
	AddNewChunkListener(JournalChunkListener)
	AddFlushListener(JournalChunkListener)
	Flush(func(JournalChunk) interface{}) error
}

type JournalGroup interface {
	Disposable
	GetJournal(key string) Journal
	GetJournalKeys() []string
}

type JournalGroupFactory interface {
	GetJournalGroup() JournalGroup
}

type Panicked struct {
	Opaque interface{}
}

func (e *Panicked) Error() string {
	s, ok := e.Opaque.(string)
	if ok {
		return s
	}
	return fmt.Sprintf("%v", e.Opaque)
}
