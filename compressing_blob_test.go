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
	"bytes"
	"compress/gzip"
	"encoding/hex"
	ioextras "github.com/moriyoshi/go-ioextras"
	td_client "github.com/treasure-data/td-client-go"
	"io/ioutil"
	"testing"
)

func Test_CompressingBlob(t *testing.T) {
	for bufSize := 2; bufSize <= 32; bufSize += 1 {
		b := NewCompressingBlob(td_client.InMemoryBlob("testtesttesttest"), bufSize, gzip.DefaultCompression, &ioextras.MemoryRandomAccessStoreFactory{})
		r, err := b.Reader()
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		a := [32]byte{}
		n, err := r.Read(a[0:4])
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("n=%d\n", n)
		if n != 4 {
			t.Fail()
		}
		if a[0] != 0x1f || a[1] != 0x8b || a[2] != 8 || a[3] != 0 {
			t.Fail()
		}
		c, err := ioutil.ReadAll(r)
		r.Close()
		sum, err := b.MD5Sum()
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		hexSum := hex.EncodeToString(sum)
		if hexSum != "7e094b2f9bef89a2a889bba182e8efcf" {
			t.Log(hexSum)
			t.Fail()
		}
		s, err := b.Size()
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("size=%d", s)
		if s != 30 {
			t.Fail()
		}
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		copy(a[4:], c)
		t.Log(hex.EncodeToString(a[0 : 4+len(c)]))
		rr, err := gzip.NewReader(bytes.NewReader(a[0 : 4+len(c)]))
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		d, err := ioutil.ReadAll(rr)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		if string(d) != "testtesttesttest" {
			t.Fail()
		}
	}
}

func Test_CompressingBlob_MD5Sum_before_reading(t *testing.T) {
	for bufSize := 2; bufSize <= 32; bufSize += 1 {
		b := NewCompressingBlob(td_client.InMemoryBlob("testtesttesttest"), bufSize, gzip.DefaultCompression, &ioextras.MemoryRandomAccessStoreFactory{})
		r, err := b.Reader()
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		sum, err := b.MD5Sum()
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		hexSum := hex.EncodeToString(sum)
		if hexSum != "7e094b2f9bef89a2a889bba182e8efcf" {
			t.Log(hexSum)
			t.Fail()
		}
		a := [32]byte{}
		n, err := r.Read(a[0:4])
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("n=%d\n", n)
		if n != 4 {
			t.Fail()
		}
		if a[0] != 0x1f || a[1] != 0x8b || a[2] != 8 || a[3] != 0 {
			t.Fail()
		}
		c, err := ioutil.ReadAll(r)
		r.Close()
		s, err := b.Size()
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("size=%d", s)
		if s != 30 {
			t.Fail()
		}
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		copy(a[4:], c)
		t.Log(hex.EncodeToString(a[0 : 4+len(c)]))
		rr, err := gzip.NewReader(bytes.NewReader(a[0 : 4+len(c)]))
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		d, err := ioutil.ReadAll(rr)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		if string(d) != "testtesttesttest" {
			t.Fail()
		}
	}
}
