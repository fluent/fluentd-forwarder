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
	"errors"
	"fmt"
	logging "github.com/op/go-logging"
	"github.com/ugorji/go/codec"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
)

type forwardClient struct {
	input  *ForwardInput
	logger *logging.Logger
	conn   *net.TCPConn
	codec  *codec.MsgpackHandle
	dec    *codec.Decoder
}

type ForwardInput struct {
	port           Port
	logger         *logging.Logger
	bind           string
	listener       *net.TCPListener
	codec          *codec.MsgpackHandle
	clientsMtx     sync.Mutex
	clients        map[*net.TCPConn]*forwardClient
	entries        int64
	wg             sync.WaitGroup
	acceptChan     chan *net.TCPConn
	shutdownChan   chan struct{}
	isShuttingDown uintptr
}

type EntryCountTopic struct{}

type ConnectionCountTopic struct{}

type ForwardInputFactory struct{}

func coerceInPlace(data map[string]interface{}) {
	for k, v := range data {
		switch v_ := v.(type) {
		case []byte:
			data[k] = string(v_) // XXX: byte => rune
		case map[string]interface{}:
			coerceInPlace(v_)
		}
	}
}

func (c *forwardClient) decodeRecordSet(tag []byte, entries []interface{}) (FluentRecordSet, error) {
	records := make([]TinyFluentRecord, len(entries))
	for i, _entry := range entries {
		entry, ok := _entry.([]interface{})
		if !ok {
			return FluentRecordSet{}, errors.New("Failed to decode recordSet")
		}
		timestamp, ok := entry[0].(uint64)
		if !ok {
			return FluentRecordSet{}, errors.New("Failed to decode timestamp field")
		}
		data, ok := entry[1].(map[string]interface{})
		if !ok {
			return FluentRecordSet{}, errors.New("Failed to decode data field")
		}
		coerceInPlace(data)
		records[i] = TinyFluentRecord{
			Timestamp: timestamp,
			Data:      data,
		}
	}
	return FluentRecordSet{
		Tag:     string(tag), // XXX: byte => rune
		Records: records,
	}, nil
}

func (c *forwardClient) decodeEntries() ([]FluentRecordSet, error) {
	v := []interface{}{nil, nil, nil}
	err := c.dec.Decode(&v)
	if err != nil {
		return nil, err
	}
	tag, ok := v[0].([]byte)
	if !ok {
		return nil, errors.New("Failed to decode tag field")
	}

	var retval []FluentRecordSet
	switch timestamp_or_entries := v[1].(type) {
	case uint64:
		timestamp := timestamp_or_entries
		data, ok := v[2].(map[string]interface{})
		if !ok {
			return nil, errors.New("Failed to decode data field")
		}
		coerceInPlace(data)
		retval = []FluentRecordSet{
			{
				Tag: string(tag), // XXX: byte => rune
				Records: []TinyFluentRecord{
					{
						Timestamp: timestamp,
						Data:      data,
					},
				},
			},
		}
	case float64:
		timestamp := uint64(timestamp_or_entries)
		data, ok := v[2].(map[string]interface{})
		if !ok {
			return nil, errors.New("Failed to decode data field")
		}
		retval = []FluentRecordSet{
			{
				Tag: string(tag), // XXX: byte => rune
				Records: []TinyFluentRecord{
					{
						Timestamp: timestamp,
						Data:      data,
					},
				},
			},
		}
	case []interface{}:
		if !ok {
			return nil, errors.New("Unexpected payload format")
		}
		recordSet, err := c.decodeRecordSet(tag, timestamp_or_entries)
		if err != nil {
			return nil, err
		}
		retval = []FluentRecordSet{recordSet}
	case []byte:
		entries := make([]interface{}, 0)
		err := codec.NewDecoderBytes(timestamp_or_entries, c.codec).Decode(&entries)
		if err != nil {
			return nil, err
		}
		recordSet, err := c.decodeRecordSet(tag, entries)
		if err != nil {
			return nil, err
		}
		retval = []FluentRecordSet{recordSet}
	default:
		return nil, errors.New(fmt.Sprintf("Unknown type: %t", timestamp_or_entries))
	}
	atomic.AddInt64(&c.input.entries, int64(len(retval)))
	return retval, nil
}

func (c *forwardClient) startHandling() {
	c.input.wg.Add(1)
	go func() {
		defer func() {
			err := c.conn.Close()
			if err != nil {
				c.logger.Debug("Close: %s", err.Error())
			}
			c.input.markDischarged(c)
			c.input.wg.Done()
		}()
		c.input.logger.Info("Started handling connection from %s", c.conn.RemoteAddr().String())
		for {
			recordSets, err := c.decodeEntries()
			if err != nil {
				err_, ok := err.(net.Error)
				if ok {
					if err_.Temporary() {
						c.logger.Info("Temporary failure: %s", err_.Error())
						continue
					}
				}
				if err == io.EOF {
					c.logger.Info("Client %s closed the connection", c.conn.RemoteAddr().String())
				} else {
					c.logger.Error(err.Error())
				}
				break
			}

			if len(recordSets) > 0 {
				err_ := c.input.port.Emit(recordSets)
				if err_ != nil {
					c.logger.Error(err_.Error())
					break
				}
			}
		}
		c.input.logger.Info("Ended handling connection from %s", c.conn.RemoteAddr().String())
	}()
}

func (c *forwardClient) shutdown() {
	err := c.conn.Close()
	if err != nil {
		c.input.logger.Info("Error during closing connection: %s", err.Error())
	}
}

func newForwardClient(input *ForwardInput, logger *logging.Logger, conn *net.TCPConn, _codec *codec.MsgpackHandle) *forwardClient {
	c := &forwardClient{
		input:  input,
		logger: logger,
		conn:   conn,
		codec:  _codec,
		dec:    codec.NewDecoder(bufio.NewReader(conn), _codec),
	}
	input.markCharged(c)
	return c
}

func (input *ForwardInput) spawnAcceptor() {
	input.logger.Notice("Spawning acceptor")
	input.wg.Add(1)
	go func() {
		defer func() {
			close(input.acceptChan)
			input.wg.Done()
		}()
		input.logger.Notice("Acceptor started")
		for {
			conn, err := input.listener.AcceptTCP()
			if err != nil {
				input.logger.Notice(err.Error())
				break
			}
			if conn != nil {
				input.logger.Notice("Connected from %s", conn.RemoteAddr().String())
				input.acceptChan <- conn
			} else {
				input.logger.Notice("AcceptTCP returned nil; something went wrong")
				break
			}
		}
		input.logger.Notice("Acceptor ended")
	}()
}

func (input *ForwardInput) spawnDaemon() {
	input.logger.Notice("Spawning daemon")
	input.wg.Add(1)
	go func() {
		defer func() {
			close(input.shutdownChan)
			input.wg.Done()
		}()
		input.logger.Notice("Daemon started")
	loop:
		for {
			select {
			case conn := <-input.acceptChan:
				if conn != nil {
					input.logger.Notice("Got conn from acceptChan")
					newForwardClient(input, input.logger, conn, input.codec).startHandling()
				}
			case <-input.shutdownChan:
				input.listener.Close()
				for _, client := range input.clients {
					client.shutdown()
				}
				break loop
			}
		}
		input.logger.Notice("Daemon ended")
	}()
}

func (input *ForwardInput) markCharged(c *forwardClient) {
	input.clientsMtx.Lock()
	defer input.clientsMtx.Unlock()
	input.clients[c.conn] = c
}

func (input *ForwardInput) markDischarged(c *forwardClient) {
	input.clientsMtx.Lock()
	defer input.clientsMtx.Unlock()
	delete(input.clients, c.conn)
}

func (input *ForwardInput) String() string {
	return "input"
}

func (input *ForwardInput) Start() {
	input.spawnAcceptor()
	input.spawnDaemon()
}

func (input *ForwardInput) WaitForShutdown() {
	input.wg.Wait()
}

func (input *ForwardInput) Stop() {
	if atomic.CompareAndSwapUintptr(&input.isShuttingDown, uintptr(0), uintptr(1)) {
		input.shutdownChan <- struct{}{}
	}
}

func NewForwardInput(logger *logging.Logger, bind string, port Port) (*ForwardInput, error) {
	_codec := codec.MsgpackHandle{}
	_codec.MapType = reflect.TypeOf(map[string]interface{}(nil))
	_codec.RawToString = false
	addr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		logger.Error("%s", err.Error())
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		logger.Error("%s", err.Error())
		return nil, err
	}
	return &ForwardInput{
		port:           port,
		logger:         logger,
		bind:           bind,
		listener:       listener,
		codec:          &_codec,
		clients:        make(map[*net.TCPConn]*forwardClient),
		clientsMtx:     sync.Mutex{},
		entries:        0,
		wg:             sync.WaitGroup{},
		acceptChan:     make(chan *net.TCPConn),
		shutdownChan:   make(chan struct{}),
		isShuttingDown: uintptr(0),
	}, nil
}
