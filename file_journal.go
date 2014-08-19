package fluentd_forwarder

import (
	"os"
	"path"
	"errors"
	"strings"
	"math/rand"
	"time"
	"fmt"
	"io"
	logging "github.com/op/go-logging"
	"sync"
	"sync/atomic"
	"unsafe"
)

type FileJournalChunkDequeueHead struct {
	next *FileJournalChunk
	prev *FileJournalChunk
}

type FileJournalChunkDequeue struct {
	first *FileJournalChunk
	last *FileJournalChunk
	count int
	mtx sync.Mutex
}

type FileJournalChunk struct {
	head FileJournalChunkDequeueHead
	Path string
	Type JournalFileType
	TSuffix string
	Timestamp int64
	UniqueId []byte
	Size int64
	refcount int32
}

type FileJournal struct {
	group *FileJournalGroup
	key string
	chunks FileJournalChunkDequeue
	writer io.WriteCloser
	newChunkListeners map[JournalChunkListener]JournalChunkListener
	flushListeners map[JournalChunkListener]JournalChunkListener
	mtx sync.Mutex
}

type FileJournalGroup struct {
	factory *FileJournalGroupFactory
	worker Worker
	timeGetter func() time.Time
	logger *logging.Logger
	rand *rand.Rand
	fileMode os.FileMode
	maxSize int64
	pathPrefix string
	pathSuffix string
	journals map[string]*FileJournal
	mtx sync.Mutex
}

type FileJournalGroupFactory struct {
	logger *logging.Logger
	paths map[string]*FileJournalGroup
	randSource rand.Source
	timeGetter func() time.Time
	defaultPathSuffix string
	defaultFileMode os.FileMode
	maxSize int64
}

type FileJournalChunkWrapper struct {
	journal *FileJournal
	chunk *FileJournalChunk
	ownershipTaken int64
}

func (wrapper *FileJournalChunkWrapper) Path() string {
	return wrapper.chunk.Path
}

func (wrapper *FileJournalChunkWrapper) String() string {
	return wrapper.chunk.Path
}

func (wrapper *FileJournalChunkWrapper) Size() (int64, error) {
	chunk := (*FileJournalChunk)(atomic.LoadPointer((*unsafe.Pointer)((unsafe.Pointer)(&wrapper.chunk))))
	if chunk == nil {
		return -1, errors.New("already disposed")
	}
	return chunk.Size, nil
}

func (wrapper *FileJournalChunkWrapper) GetReader() (io.ReadCloser, error) {
	chunk := (*FileJournalChunk)(atomic.LoadPointer((*unsafe.Pointer)((unsafe.Pointer)(&wrapper.chunk))))
	if chunk == nil {
		return nil, errors.New("already disposed")
	}
	return chunk.getReader()
}

func (wrapper *FileJournalChunkWrapper) GetNextChunk() JournalChunk {
	chunk := (*FileJournalChunk)(atomic.LoadPointer((*unsafe.Pointer)((unsafe.Pointer)(&wrapper.chunk))))
	retval := (*FileJournalChunkWrapper)(nil)
	if chunk != nil {
		journal := wrapper.journal
		journal.chunks.mtx.Lock()
		if chunk.head.prev != nil {
			retval = journal.newChunkWrapper(chunk.head.prev)
		}
		journal.chunks.mtx.Unlock()
	}
	return retval
}

func (wrapper *FileJournalChunkWrapper) TakeOwnership() bool {
	chunk := (*FileJournalChunk)(atomic.LoadPointer((*unsafe.Pointer)((unsafe.Pointer)(&wrapper.chunk))))
	if chunk == nil {
		return false
	}
	if atomic.CompareAndSwapInt64(&wrapper.ownershipTaken, 0, 1) {
		wrapper.journal.deleteRef((*FileJournalChunk)(chunk))
		return true
	} else {
		return false
	}
}

func (wrapper *FileJournalChunkWrapper) Dispose() error {
	chunk := (*FileJournalChunk)(atomic.SwapPointer((*unsafe.Pointer)((unsafe.Pointer)(&wrapper.chunk)), nil))
	if chunk == nil {
		return errors.New("already disposed")
	}
	err, destroyed := wrapper.journal.deleteRef((*FileJournalChunk)(chunk))
	if err != nil {
		return err
	}
	if destroyed && wrapper.ownershipTaken != 0 && chunk.head.next == nil {
		// increment the refcount of the last chunk
		// to rehold the reference
		prevChunk := chunk.head.prev
		if prevChunk != nil {
			atomic.AddInt32(&prevChunk.refcount, 1)
		}
	}
	return nil
}

func (journal *FileJournal) newChunkWrapper(chunk *FileJournalChunk) *FileJournalChunkWrapper {
	atomic.AddInt32(&chunk.refcount, 1)
	return &FileJournalChunkWrapper { journal, chunk, 0 }
}

func (journal *FileJournal) deleteRef(chunk *FileJournalChunk) (error, bool) {
	refcount := atomic.AddInt32(&chunk.refcount, -1)
	if refcount == 0 {
		// first propagate to newer chunk
		if prevChunk := chunk.head.prev; prevChunk != nil {
			err, _ := journal.deleteRef(prevChunk)
			if err != nil {
				// undo the change
				atomic.AddInt32(&chunk.refcount, 1)
				return err, false
			}
		}
		err := os.Remove(chunk.Path)
		if err != nil {
			// undo the change
			atomic.AddInt32(&chunk.refcount, 1)
			return err, false
		}
		{
			journal.chunks.mtx.Lock()
			prevChunk := chunk.head.prev
			nextChunk := chunk.head.next
			if prevChunk == nil {
				journal.chunks.first = nextChunk
			} else {
				prevChunk.head.next = nextChunk
			}
			if nextChunk == nil {
				journal.chunks.last = prevChunk
			} else {
				nextChunk.head.prev = prevChunk
			}
			journal.chunks.count -= 1
			journal.chunks.mtx.Unlock()
		}
		return nil, true
	} else if refcount < 0 {
		// should never happen
		panic(fmt.Sprintf("something went wrong! chunk=%v, chunks.count=%d", chunk, journal.chunks.count))
	}
	return nil, false
}

func (chunk *FileJournalChunk) getReader() (io.ReadCloser, error) {
	return os.OpenFile(chunk.Path, os.O_RDONLY, 0)
}

func (journal *FileJournal) Key() string {
	return journal.key
}

func (journal *FileJournal) notifyFlushListeners(chunk *FileJournalChunk) {
	// lock for listener container must be acquired by caller
	for _, listener := range journal.flushListeners {
		err := listener.ChunkFlushed(journal.newChunkWrapper(chunk))
		if err != nil {
			journal.group.logger.Error("error occurred during notifying flush event: %s", err.Error())
		}
	}
}

func (journal *FileJournal) notifyNewChunkListeners(chunk *FileJournalChunk) {
	// lock for listener container must be acquired by caller
	for _, listener := range journal.newChunkListeners {
		err := listener.NewChunkCreated(journal.newChunkWrapper(chunk))
		if err != nil {
			journal.group.logger.Error("error occurred during notifying flush event: %s", err.Error())
		}
	}
}

func (journal *FileJournal) finalizeChunk(chunk *FileJournalChunk) error {
	group := journal.group
	variablePortion := BuildJournalPathWithTSuffix(
		journal.key,
		Rest,
		chunk.TSuffix,
	)
	newPath:= group.pathPrefix + variablePortion + group.pathSuffix
	err := os.Rename(chunk.Path, newPath)
	if err != nil {
		return err
	}
	chunk.Type = Rest
	chunk.Path = newPath
	journal.notifyFlushListeners(chunk)
	return nil
}

func (journal *FileJournal) Flush(visitor func (JournalChunk) error) error {
	err := func () error {
		journal.mtx.Lock()
		defer journal.mtx.Unlock()
		// this is safe the journal lock prevents any new chunk from being added to the head
		head := journal.chunks.first
		if head != nil {
			if head.Size != 0 {
				_, err := journal.newChunk()
				if err != nil {
					return err
				}
			} else {
				if journal.writer != nil {
					err := journal.writer.Close()
					if err != nil {
						return err
					}
					journal.writer = nil
				}
				err, _ := journal.deleteRef(head) // writer-holding ref
				if err != nil {
					return err
				}
			}
		}
		return nil
	}()
	if err != nil {
		return err
	}
	if visitor != nil {
		var chunks []*FileJournalChunk
		func () {
			journal.chunks.mtx.Lock()
			defer journal.chunks.mtx.Unlock()
			chunks = make([]*FileJournalChunk, journal.chunks.count)
			i := 0
			for chunk := journal.chunks.last; chunk != nil; chunk = chunk.head.prev {
				chunks[i] = chunk
				if i > 0 {
					atomic.AddInt32(&chunk.refcount, 1)
				}
				i += 1
			}
		}()
		for i, chunk := range chunks {
			err := visitor(journal.newChunkWrapper(chunk))
			if err != nil {
				for {
					i += 1
					if i >= len(chunks) {
						break
					}
					journal.deleteRef(chunks[i])
				}
				return err
			}
			err, _ = journal.deleteRef(chunk)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (journal *FileJournal) newChunk() (*FileJournalChunk, error) {
	group := journal.group
	info := BuildJournalPath(
		journal.key,
		Head,
		group.timeGetter(),
		group.rand.Int63n(0xfff),
	)
	chunk := &FileJournalChunk {
		head: FileJournalChunkDequeueHead { journal.chunks.first, nil },
		Path: (group.pathPrefix + info.VariablePortion + group.pathSuffix),
		Type: info.Type,
		TSuffix: info.TSuffix,
		UniqueId: info.UniqueId,
		refcount: 1,
	}
	file, err := os.OpenFile(chunk.Path, os.O_WRONLY | os.O_APPEND | os.O_CREATE | os.O_EXCL, journal.group.fileMode)
	if err != nil {
		return nil, err
	}
	if journal.writer != nil {
		err := journal.writer.Close()
		if err != nil {
			return nil, err
		}
		journal.writer = nil
	}

	oldHead := (*FileJournalChunk)(nil)
	{
		journal.chunks.mtx.Lock()
		oldHead = journal.chunks.first
		if oldHead != nil {
			oldHead.head.prev = chunk
		} else {
			journal.chunks.last = chunk
		}
		chunk.head.next = journal.chunks.first
		journal.chunks.first = chunk
		journal.chunks.count += 1
		journal.chunks.mtx.Unlock()
	}
	chunk.refcount += 1 // for writer

	if oldHead != nil {
		err := journal.finalizeChunk(oldHead)
		if err != nil {
			file.Close()
			os.Remove(chunk.Path)
			return nil, err
		}
		err, _ = journal.deleteRef(oldHead) // writer-holding ref
		if err != nil {
			file.Close()
			os.Remove(chunk.Path)
			return nil, err
		}
	}

	journal.writer = file
	journal.chunks.first.Size = 0
	journal.notifyNewChunkListeners(chunk)
	return chunk, nil
}

func (journal *FileJournal) AddFlushListener(listener JournalChunkListener) {
	journal.mtx.Lock()
	defer journal.mtx.Unlock()
	journal.flushListeners[listener] = listener
}

func (journal *FileJournal) AddNewChunkListener(listener JournalChunkListener) {
	journal.mtx.Lock()
	defer journal.mtx.Unlock()
	journal.newChunkListeners[listener] = listener
}

func (journal *FileJournal) Write(data []byte) error {
	journal.mtx.Lock()
	defer journal.mtx.Unlock()

	if journal.writer == nil {
		if journal.chunks.first == nil {
			_, err := journal.newChunk()
			if err != nil {
				return err
			}
		}
	} else {
		if journal.chunks.first == nil || journal.group.maxSize - journal.chunks.first.Size < int64(len(data)) {
			_, err := journal.newChunk()
			if err != nil {
				return err
			}
		}
	}

	if journal.writer == nil {
		return errors.New("journal has been disposed?")
	}
	n, err := journal.writer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("not all data could be written")
	}
	journal.chunks.first.Size += int64(n)
	return nil
}

func (journal *FileJournal) GetTailChunk() JournalChunk {
	retval := (*FileJournalChunkWrapper)(nil)
	{
		journal.chunks.mtx.Lock()
		if journal.chunks.last != nil {
			retval = journal.newChunkWrapper(journal.chunks.last)
		}
		journal.chunks.mtx.Unlock()
	}
	return retval
}

func (journal *FileJournal) Dispose() error {
	journal.mtx.Lock()
	defer journal.mtx.Unlock()
	if journal.writer != nil {
		err := journal.writer.Close()
		if err != nil {
			return err
		}
		journal.writer = nil
	}
	return nil
}

func (journalGroup *FileJournalGroup) Dispose() error {
	for _, journal := range journalGroup.journals {
		journal.Dispose()
	}
	return nil
}

func (journalGroup *FileJournalGroup) GetFileJournal(key string) *FileJournal {
	journalGroup.mtx.Lock()
	defer journalGroup.mtx.Unlock()

	journal, ok := journalGroup.journals[key]
	if ok {
		return journal
	}
	journal = &FileJournal {
		group: journalGroup,
		key: key,
		chunks: FileJournalChunkDequeue { nil, nil, 0, sync.Mutex {} },
		writer: nil,
		newChunkListeners: make(map[JournalChunkListener]JournalChunkListener),
		flushListeners: make(map[JournalChunkListener]JournalChunkListener),
	}
	journalGroup.journals[key] = journal
	return journal
}

func (journalGroup *FileJournalGroup) GetJournal(key string) Journal {
	return journalGroup.GetFileJournal(key)
}

func (journalGroup *FileJournalGroup) GetJournalKeys() []string {
	journalGroup.mtx.Lock()
	defer journalGroup.mtx.Unlock()

	retval := make([]string, len(journalGroup.journals))
	i := 0
	for k := range journalGroup.journals {
		retval[i] = k
		i += 1
	}
	return retval
}

// http://stackoverflow.com/questions/1525117/whats-the-fastest-algorithm-for-sorting-a-linked-list
// http://www.chiark.greenend.org.uk/~sgtatham/algorithms/listsort.html
func sortChunksByTimestamp(chunks *FileJournalChunkDequeue) {
	k := 1
	lhs := chunks.first
	if lhs == nil {
		return
	}
	for {
		result := FileJournalChunkDequeue { nil , nil, chunks.count, sync.Mutex {} }
		first := true
		for {
			picked := (*FileJournalChunk)(nil)
			lhsSize := 0
			rhsSize := k
			rhs := lhs
			i := k
			for i > 0 && rhs.head.next != nil {
				i -= 1
				rhs = rhs.head.next
			}
			lhsSize = k - i
			for {
				if lhsSize != 0 {
					if rhsSize != 0 && rhs != nil && lhs.Timestamp < rhs.Timestamp {
						picked = rhs
						rhs = rhs.head.next
						rhsSize -= 1
					} else {
						picked = lhs
						lhs = lhs.head.next
						lhsSize -= 1
					}
				} else {
					if rhsSize != 0 && rhs != nil {
						picked = rhs
						rhs = rhs.head.next
						rhsSize -= 1
					} else {
						break
					}
				}
				if picked.head.prev != nil {
					picked.head.prev.head.next = picked.head.next
				}
				if picked.head.next != nil {
					picked.head.next.head.prev = picked.head.prev
				}
				if result.last == nil {
					result.first = picked
				} else {
					result.last.head.next = picked
				}
				picked.head.prev = result.last
				picked.head.next = nil
				result.last = picked
			}
			lhs = rhs
			if lhs == nil {
				break
			}
			first = false
		}
		if first {
			*chunks = result
			break
		}
		k *= 2
		lhs = result.first
	}
}

func validateChunks(chunks *FileJournalChunkDequeue) error {
	chunkHead := (*FileJournalChunk)(nil)
	for chunk := chunks.first; chunk != nil; chunk = chunk.head.next {
		if chunk.Type == Head {
			if chunkHead != nil {
				return errors.New("multiple chunk heads found")
			}
			chunkHead = chunk
		}
	}
	if chunkHead != chunks.first {
		return errors.New("chunk head does not have the newest timestamp")
	}
	return nil
}

func scanJournals(logger *logging.Logger, pathPrefix string, pathSuffix string) (map[string]*FileJournal, error) {
	journals := make(map[string]*FileJournal)
	dirname, basename := path.Split(pathPrefix)
	if dirname == "" {
		dirname = "."
	}
	d, err := os.OpenFile(dirname, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	finfo, err := d.Stat()
	if err != nil {
		return nil, err
	}
	if !finfo.IsDir() {
		return nil, errors.New(fmt.Sprintf("%s is not a directory", dirname))
	}
	for {
		files_, err := d.Readdirnames(100)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		for _, file := range files_ {
			if !strings.HasSuffix(file, pathSuffix) {
				continue
			}
			variablePortion := file[len(basename):len(file) - len(pathSuffix)]
			info, err := DecodeJournalPath(variablePortion)
			if err != nil {
				logger.Warning("Unexpected file under the designated directory space (%s) - %s", dirname, file)
				continue
			}
			journalProto, ok := journals[info.Key]
			if !ok {
				journalProto = &FileJournal {
					key: info.Key,
					chunks: FileJournalChunkDequeue { nil, nil, 0, sync.Mutex {} },
					writer: nil,
				}
				journals[info.Key] = journalProto
			}
			chunk := &FileJournalChunk {
				head: FileJournalChunkDequeueHead { nil, journalProto.chunks.last },
				Type: info.Type,
				Path: pathPrefix + info.VariablePortion + pathSuffix,
				TSuffix: info.TSuffix,
				Timestamp: info.Timestamp,
				UniqueId: info.UniqueId,
				refcount: 1,
			}
			if journalProto.chunks.last == nil {
				journalProto.chunks.first = chunk
			} else {
				journalProto.chunks.last.head.next = chunk
			}
			journalProto.chunks.last = chunk
			journalProto.chunks.count += 1
		}
	}
	for _, journalProto := range journals {
		sortChunksByTimestamp(&journalProto.chunks)
		err := validateChunks(&journalProto.chunks)
		if err != nil {
			return nil, err
		}
	}
	return journals, nil
}

func (factory *FileJournalGroupFactory) GetJournalGroup(path string, worker Worker) (*FileJournalGroup, error) {
	registered, ok := factory.paths[path]
	if ok {
		if registered.worker == worker {
			return registered, nil
		} else {
			return nil, errors.New(fmt.Sprintf(
				"Other worker '%s' already use same buffer_path: %s",
				registered.worker.String(),
				path,
			))
		}
	}

	var pathPrefix string
	var pathSuffix string

	pos := strings.Index(path, "*")
	if pos >= 0 {
		pathPrefix = path[0:pos]
		pathSuffix = path[pos + 1:]
	} else {
		pathPrefix = path + "."
		pathSuffix = factory.defaultPathSuffix
	}

	journals, err := scanJournals(factory.logger, pathPrefix, pathSuffix)
	if err != nil {
		return nil, err
	}

	journalGroup := &FileJournalGroup {
		factory: factory,
		worker: worker,
		timeGetter: factory.timeGetter,
		logger: factory.logger,
		rand: rand.New(factory.randSource),
		fileMode: factory.defaultFileMode,
		maxSize: factory.maxSize,
		pathPrefix: pathPrefix,
		pathSuffix: pathSuffix,
		journals: journals,
		mtx: sync.Mutex {},
	}
	for _, journal := range journals {
		journal.group = journalGroup
		journal.newChunkListeners = make(map[JournalChunkListener]JournalChunkListener)
		journal.flushListeners = make(map[JournalChunkListener]JournalChunkListener)
		chunk := journal.chunks.first
		file, err := os.OpenFile(chunk.Path, os.O_WRONLY | os.O_APPEND, journal.group.fileMode)
		if err != nil {
			journalGroup.Dispose()
			return nil, err
		}
		position, err := file.Seek(0, os.SEEK_END)
		if err != nil {
			file.Close()
			journalGroup.Dispose()
			return nil, err
		}
		chunk.refcount += 1 // for writer
		chunk.Size = position
		journal.writer = file
	}
	factory.logger.Info("Path %s is designated to Worker %s", path, worker.String())
	factory.paths[path] = journalGroup
	return journalGroup, nil
}

func NewFileJournalGroupFactory(
	logger *logging.Logger,
	randSource rand.Source,
	timeGetter func() time.Time,
	defaultPathSuffix string,
	defaultFileMode os.FileMode,
	maxSize int64,
) *FileJournalGroupFactory {
	return &FileJournalGroupFactory {
		logger: logger,
		paths: make(map[string]*FileJournalGroup),
		randSource: randSource,
		timeGetter: timeGetter,
		defaultPathSuffix: defaultPathSuffix,
		defaultFileMode: defaultFileMode,
		maxSize: maxSize,
	}
}

