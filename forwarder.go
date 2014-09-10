package fluentd_forwarder

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

type Port interface {
	Emit(recordSets []FluentRecordSet) error
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
