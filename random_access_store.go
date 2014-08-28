package fluentd_forwarder

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
)

type RandomAccessStore interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

type SizedRandomAccessStore interface {
	RandomAccessStore
	Size() (int64, error)
}

type NamedRandomAccessStore interface {
	RandomAccessStore
	Name() string
}

type RandomAccessStoreFactory interface {
	RandomAccessStore() (RandomAccessStore, error)
}

type SeekerWrapper struct {
	s  RandomAccessStore
	sk io.Seeker
	ns NamedRandomAccessStore
}

func (s *SeekerWrapper) ReadAt(p []byte, offset int64) (int, error) { return s.s.ReadAt(p, offset) }

func (s *SeekerWrapper) WriteAt(p []byte, offset int64) (int, error) { return s.s.WriteAt(p, offset) }

func (s *SeekerWrapper) Close() error { return s.s.Close() }

func (s *SeekerWrapper) Name() string {
	if s.ns != nil {
		return s.ns.Name()
	} else {
		return ""
	}
}

func (s *SeekerWrapper) Size() (int64, error) {
	return s.sk.Seek(0, os.SEEK_END)
}

func NewSeekerWrapper(s RandomAccessStore) *SeekerWrapper {
	ns, _ := s.(NamedRandomAccessStore)
	return &SeekerWrapper{s, s.(io.Seeker), ns}
}

type CloseHook struct {
	c  io.Closer
	r  io.Reader
	w  io.Writer
	ra io.ReaderAt
	wa io.WriterAt
	sk io.Seeker
	sz interface {
		Size() (int64, error)
	}
	n interface {
		Name() string
	}
	callback func(s io.Closer)
}

func (c *CloseHook) Close() error {
	err := c.c.Close()
	if err != nil {
		return err
	}
	c.callback(c.c)
	return nil
}

func (c *CloseHook) Name() string {
	if c.n == nil {
		panic("Underlying interface does not provide Name() (string)")
	}
	return c.n.Name()
}

func (c *CloseHook) Size() (int64, error) {
	if c.sz == nil {
		panic("Underlying interface does not provide Size() (int64, error)")
	}
	return c.sz.Size()
}

func (c *CloseHook) Read(p []byte) (int, error) {
	if c.r == nil {
		panic("Underlying interface does not provide Read([]byte) (int, error)")
	}
	return c.r.Read(p)
}

func (c *CloseHook) ReadAt(p []byte, offset int64) (int, error) {
	if c.ra == nil {
		panic("Underlying interface does not provide ReadAt([]byte, int64) (int, error)")
	}
	return c.ra.ReadAt(p, offset)
}

func (c *CloseHook) Write(p []byte) (int, error) {
	if c.w == nil {
		panic("Underlying interface does not provide Write([]byte) (int, error)")
	}
	return c.w.Write(p)
}

func (c *CloseHook) WriteAt(p []byte, offset int64) (int, error) {
	if c.wa == nil {
		panic("Underlying interface does not provide WriteAt([]byte, int64) (int, error)")
	}
	return c.wa.WriteAt(p, offset)
}

func (c *CloseHook) Seek(offset int64, whence int) (int64, error) {
	if c.sk == nil {
		panic("Underlying interface does not provide Seek(int64, int) (int64, error)")
	}
	return c.sk.Seek(offset, whence)
}

func NewCloseHook(c io.Closer, callback func(s io.Closer)) *CloseHook {
	r, _ := c.(io.Reader)
	w, _ := c.(io.Writer)
	ra, _ := c.(io.ReaderAt)
	wa, _ := c.(io.WriterAt)
	sk, _ := c.(io.Seeker)
	sz, _ := c.(interface {
		Size() (int64, error)
	})
	n, _ := c.(interface {
		Name() string
	})
	return &CloseHook{
		c:        c,
		r:        r,
		w:        w,
		ra:       ra,
		wa:       wa,
		sk:       sk,
		sz:       sz,
		n:        n,
		callback: callback,
	}
}

type StoreReadWriter struct {
	s    RandomAccessStore
	pos  int64
	size int64
}

func (rw *StoreReadWriter) Write(p []byte) (int, error) {
	n, err := rw.s.WriteAt(p, rw.pos)
	rw.pos += int64(n)
	return n, err
}

func (rw *StoreReadWriter) Read(p []byte) (int, error) {
	n, err := rw.s.ReadAt(p, rw.pos)
	if err == io.EOF {
		rw.size = rw.pos + int64(n)
	}
	rw.pos += int64(n)
	return n, err
}

func (rw *StoreReadWriter) Close() error { return nil }

func (rw *StoreReadWriter) Seek(pos int64, whence int) (int64, error) {
	switch whence {
	case os.SEEK_SET:
		rw.pos = pos
	case os.SEEK_CUR:
		rw.pos += pos
	case os.SEEK_END:
		if rw.size < 0 {
			return -1, errors.New("trying to seek to EOF while the store size is not known")
		}
		rw.pos = rw.size + pos
	}
	return rw.pos, nil
}

type MemoryRandomAccessStore struct {
	buf []byte
}

func (s *MemoryRandomAccessStore) WriteAt(p []byte, offset int64) (int, error) {
	err := (error)(nil)
	o := int(offset)
	e := o + len(p)
	if e > len(s.buf) {
		if e <= cap(s.buf) {
			s.buf = s.buf[0:e]
		} else {
			newBuf := make([]byte, e, cap(s.buf)*2)
			copy(newBuf, s.buf)
			s.buf = newBuf
		}
	}
	n := e - o
	copy(s.buf[o:e], p)
	return n, err
}

func (s *MemoryRandomAccessStore) ReadAt(p []byte, offset int64) (int, error) {
	err := (error)(nil)
	o := int(offset)
	e := o + len(p)
	if e > len(s.buf) {
		e = len(s.buf)
		err = io.EOF
	}
	n := e - o
	copy(p, s.buf[o:e])
	return n, err
}

func (s *MemoryRandomAccessStore) Size() (int64, error) {
	return int64(len(s.buf)), nil
}

func (s *MemoryRandomAccessStore) Close() error { return nil }

func NewMemoryRandomAccessStore() *MemoryRandomAccessStore {
	return &MemoryRandomAccessStore{
		buf: make([]byte, 0, 16),
	}
}

type MemoryRandomAccessStoreFactory struct{}

func (ras *MemoryRandomAccessStoreFactory) RandomAccessStore() (RandomAccessStore, error) {
	return NewMemoryRandomAccessStore(), nil
}

type TempFileRandomAccessStoreFactory struct {
	Dir    string
	Prefix string
	GCChan chan *os.File
}

func (ras *TempFileRandomAccessStoreFactory) RandomAccessStore() (RandomAccessStore, error) {
	f, err := ioutil.TempFile(ras.Dir, ras.Prefix)
	if err != nil {
		return nil, err
	}
	f_ := (RandomAccessStore)(f)
	if ras.GCChan != nil {
		c := ras.GCChan
		f_ = NewCloseHook(
			f_,
			func(s io.Closer) {
				c <- s.(*os.File)
			},
		)
	}
	return NewSeekerWrapper(f_), nil
}
