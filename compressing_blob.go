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
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"errors"
	ioextras "github.com/moriyoshi/go-ioextras"
	td_client "github.com/treasure-data/td-client-go"
	"hash"
	"io"
	"os"
)

type CompressingBlob struct {
	inner       td_client.Blob
	level       int
	bufferSize  int
	reader      *CompressingBlobReader
	tempFactory ioextras.RandomAccessStoreFactory
	md5sum      []byte
	size        int64
}

type CompressingBlobReader struct {
	buf             []byte // ring buffer
	o               int
	src             io.ReadCloser
	dst             *ioextras.StoreReadWriter
	s               ioextras.SizedRandomAccessStore
	w               *ioextras.StoreReadWriter
	bw              *bufio.Writer
	cw              *gzip.Writer
	h               hash.Hash
	eof             bool
	md5SumAvailable bool
	closeNotify     func(*CompressingBlobReader)
}

func (reader *CompressingBlobReader) drainAll() error {
	rn := 0
	err := (error)(nil)
	for reader.cw != nil && err == nil {
		o := reader.o
		if !reader.eof {
			rn, err = reader.src.Read(reader.buf[reader.o:cap(reader.buf)])
			o += rn
			if err == io.EOF {
				reader.eof = true
			}
		} else {
			if o == 0 {
				break
			}
		}
		if o > 0 {
			wn, werr := reader.cw.Write(reader.buf[0:o])
			if werr != nil {
				err = werr
			}
			copy(reader.buf[0:], reader.buf[wn:])
			reader.o = o - wn
		}
		if err != nil {
			reader.cw.Close()
			reader.cw = nil
			werr := reader.bw.Flush()
			if werr != nil {
				return werr
			}
			reader.bw = nil
		}
	}
	if err == io.EOF {
		err = nil
	}
	return err
}

func (reader *CompressingBlobReader) Read(p []byte) (int, error) {
	n := int(0)
	err := (error)(nil)
	rn := int(0)
	rpos, err := reader.dst.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err // should never happen
	}
	wpos, err := reader.w.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err // should never happen
	}
	for {
		n = int(wpos - rpos) // XXX: may underflow, but it should be ok
		if err != nil || len(p) <= n || reader.cw == nil {
			break
		}
		o := reader.o
		if !reader.eof {
			rn, err = reader.src.Read(reader.buf[reader.o:cap(reader.buf)])
			o += rn
			if err == io.EOF {
				reader.eof = true
			}
		}
		if o > 0 {
			wn, werr := reader.cw.Write(reader.buf[0:o])
			copy(reader.buf[0:], reader.buf[wn:o])
			reader.o = o - wn
			if werr != nil {
				return 0, werr
			}
		} else {
			if reader.eof && reader.cw != nil {
				reader.cw.Close()
				reader.cw = nil
				werr := reader.bw.Flush()
				if werr != nil {
					return 0, werr
				}
				reader.bw = nil
			}
		}
		var werr error
		wpos, werr = reader.w.Seek(0, os.SEEK_CUR)
		if werr != nil {
			return 0, werr // should never happen
		}
	}
	if len(p) > 0 && n == 0 && reader.eof {
		reader.md5SumAvailable = true
		return 0, io.EOF
	}
	if n > len(p) {
		n = len(p)
	}
	n, err = reader.dst.Read(p[0:n])
	reader.h.Write(p[0:n])
	if err == io.EOF {
		if !reader.eof {
			panic("something went wrong!")
		}
		reader.md5SumAvailable = true
	}
	return n, err
}

func (reader *CompressingBlobReader) size() (int64, error) {
	if reader.s == nil {
		return -1, errors.New("already closed")
	}
	if reader.cw != nil {
		err := reader.drainAll()
		if err != nil {
			return -1, err
		}
	}
	size, err := reader.s.Size()
	if err != nil {
		return -1, err // should never happen
	}
	return size, nil
}

func (reader *CompressingBlobReader) Close() error {
	bwerr := (error)(nil)
	errs := make([]error, 0, 4)
	err := reader.ensureMD5SumAvailble()
	if err != nil {
		return err
	}
	if reader.cw != nil {
		if err != nil {
			return err
		}
		err = reader.cw.Close()
		if err == nil {
			reader.cw = nil
		} else {
			errs = append(errs, err)
		}
	}
	if reader.bw != nil {
		bwerr = reader.bw.Flush()
		if bwerr == nil {
			reader.bw = nil
		} else {
			errs = append(errs, bwerr)
		}
	}
	if reader.src != nil {
		err := reader.src.Close()
		if err == nil {
			reader.src = nil
		} else {
			errs = append(errs, err)
		}
	}
	if bwerr == nil {
		if reader.s != nil {
			err := reader.s.Close()
			if err == nil {
				reader.s = nil
			} else {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return Errors(errs)
	} else {
		reader.closeNotify(reader)
		return nil
	}
}

func (reader *CompressingBlobReader) ensureMD5SumAvailble() error {
	if reader.md5SumAvailable {
		return nil
	}
	if reader.s == nil {
		return errors.New("already closed")
	}
	err := reader.drainAll()
	if err != nil {
		return err
	}
	r := *reader.dst
	_, err = io.Copy(reader.h, &r)
	if err != nil {
		return err
	}
	reader.md5SumAvailable = true
	return nil
}

func (reader *CompressingBlobReader) md5sum() ([]byte, error) {
	err := reader.ensureMD5SumAvailble()
	if err != nil {
		return nil, err
	}
	retval := make([]byte, 0, reader.h.Size())
	return reader.h.Sum(retval), nil
}

func (blob *CompressingBlob) newReader() (*CompressingBlobReader, error) {
	err := (error)(nil)
	src := (io.ReadCloser)(nil)
	s := (ioextras.SizedRandomAccessStore)(nil)
	defer func() {
		if err != nil {
			if src != nil {
				src.Close()
			}
			if s != nil {
				s.Close()
			}
		}
	}()
	src, err = blob.inner.Reader()
	if err != nil {
		return nil, err
	}
	s_, err := blob.tempFactory.RandomAccessStore()
	if err != nil {
		return nil, err
	}
	s = s_.(ioextras.SizedRandomAccessStore)
	w := &ioextras.StoreReadWriter{s, 0, -1}
	dst := &ioextras.StoreReadWriter{s, 0, -1}
	// assuming average compression ratio to be 1/3
	writeBufferSize := maxInt(4096, blob.bufferSize/3)
	bw := bufio.NewWriterSize(w, writeBufferSize)
	cw, err := gzip.NewWriterLevel(bw, blob.level)
	if err != nil {
		return nil, err
	}
	return &CompressingBlobReader{
		buf:             make([]byte, blob.bufferSize),
		o:               0,
		src:             src,
		dst:             dst,
		s:               s,
		w:               w,
		bw:              bw,
		cw:              cw,
		eof:             false,
		h:               md5.New(),
		md5SumAvailable: false,
		closeNotify: func(reader *CompressingBlobReader) {
			md5sum, err := reader.md5sum()
			if err == nil {
				blob.md5sum = md5sum
			}
			size, err := reader.size()
			if err == nil {
				blob.size = size
			}
			blob.reader = nil
		},
	}, nil
}

func (blob *CompressingBlob) ensureReaderAvailable() error {
	if blob.reader != nil {
		return nil
	}
	reader, err := blob.newReader()
	if err != nil {
		return err
	}
	blob.reader = reader
	blob.md5sum = nil
	blob.size = -1
	return nil
}

func (blob *CompressingBlob) Reader() (io.ReadCloser, error) {
	err := blob.ensureReaderAvailable()
	if err != nil {
		return nil, err
	}
	return blob.reader, nil
}

func (blob *CompressingBlob) Size() (int64, error) {
	if blob.size < 0 {
		err := blob.ensureReaderAvailable()
		if err != nil {
			return -1, err
		}
		size, err := blob.reader.size()
		if err != nil {
			return -1, err
		}
		blob.size = size
	}
	return blob.size, nil
}

func (blob *CompressingBlob) MD5Sum() ([]byte, error) {
	if blob.md5sum == nil {
		err := blob.ensureReaderAvailable()
		if err != nil {
			return nil, err
		}
		md5sum, err := blob.reader.md5sum()
		if err != nil {
			return nil, err
		}
		blob.md5sum = md5sum
	}
	return blob.md5sum, nil
}

func (blob *CompressingBlob) Dispose() error {
	if blob.reader != nil {
		return blob.reader.Close()
	}
	return nil
}

func NewCompressingBlob(blob td_client.Blob, bufferSize int, level int, tempFactory ioextras.RandomAccessStoreFactory) *CompressingBlob {
	return &CompressingBlob{
		inner:       blob,
		level:       level,
		bufferSize:  bufferSize,
		reader:      nil,
		tempFactory: tempFactory,
		md5sum:      nil,
		size:        -1,
	}
}
