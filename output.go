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
	logging "github.com/op/go-logging"
	"github.com/ugorji/go/codec"
	"io"
	"math/rand"
	"net"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

var randSource = rand.NewSource(time.Now().UnixNano())

type ForwardOutput struct {
	logger               *logging.Logger
	codec                *codec.MsgpackHandle
	bind                 string
	retryInterval        time.Duration
	connectionTimeout    time.Duration
	writeTimeout         time.Duration
	enc                  *codec.Encoder
	conn                 net.Conn
	flushInterval        time.Duration
	wg                   sync.WaitGroup
	journalGroup         JournalGroup
	journal              Journal
	emitterChan          chan FluentRecordSet
	spoolerShutdownChan  chan struct{}
	isShuttingDown       uintptr
	completion           sync.Cond
	hasShutdownCompleted bool
	metadata             string
}

func encodeRecordSet(encoder *codec.Encoder, recordSet FluentRecordSet) error {
	v := []interface{}{recordSet.Tag, recordSet.Records}
	err := encoder.Encode(v)
	if err != nil {
		return err
	}
	return err
}

func (output *ForwardOutput) ensureConnected() error {
	if output.conn == nil {
		output.logger.Noticef("Connecting to %s...", output.bind)
		conn, err := net.DialTimeout("tcp", output.bind, output.connectionTimeout)
		if err != nil {
			output.logger.Errorf("Failed to connect to %s (reason: %s)", output.bind, err.Error())
			return err
		} else {
			output.conn = conn
		}
	}
	return nil
}

func (output *ForwardOutput) sendBuffer(buf []byte) error {
	for len(buf) > 0 {
		if atomic.LoadUintptr(&output.isShuttingDown) != 0 {
			break
		}
		err := output.ensureConnected()
		if err != nil {
			output.logger.Infof("Will be retried in %s", output.retryInterval.String())
			time.Sleep(output.retryInterval)
			continue
		}
		startTime := time.Now()
		if output.writeTimeout == 0 {
			output.conn.SetWriteDeadline(time.Time{})
		} else {
			output.conn.SetWriteDeadline(startTime.Add(output.writeTimeout))
		}
		n, err := output.conn.Write(buf)
		buf = buf[n:]
		if err != nil {
			output.logger.Errorf("Failed to flush buffer (reason: %s, left: %d bytes)", err.Error(), len(buf))
			err_, ok := err.(net.Error)
			if !ok || (!err_.Timeout() && !err_.Temporary()) {
				output.conn.Close()
				output.conn = nil
				continue
			}
		}
		if n > 0 {
			elapsed := time.Now().Sub(startTime)
			output.logger.Infof("Forwarded %d bytes in %f seconds (%d bytes left)\n", n, elapsed.Seconds(), len(buf))
		}
	}
	return nil
}

func (output *ForwardOutput) spawnSpooler() {
	output.logger.Notice("Spawning spooler")
	output.wg.Add(1)
	go func() {
		ticker := time.NewTicker(output.flushInterval)
		defer func() {
			ticker.Stop()
			output.journal.Dispose()
			if output.conn != nil {
				output.conn.Close()
			}
			output.conn = nil
			output.wg.Done()
		}()
		output.logger.Notice("Spooler started")
	outer:
		for {
			select {
			case <-ticker.C:
				buf := make([]byte, 16777216)
				output.logger.Notice("Flushing...")
				err := output.journal.Flush(func(chunk JournalChunk) interface{} {
					defer chunk.Dispose()
					output.logger.Infof("Flushing chunk %s", chunk.String())
					reader, err := chunk.Reader()
					defer reader.Close()
					if err != nil {
						return err
					}
					for {
						n, err := reader.Read(buf)
						if n > 0 {
							err_ := output.sendBuffer(buf[:n])
							if err_ != nil {
								return err
							}
						}
						if err != nil {
							if err == io.EOF {
								break
							} else {
								return err
							}
						}
					}
					return nil
				})
				if err != nil {
					output.logger.Errorf("Error during reading from the journal: %s", err.Error())
				}
			case <-output.spoolerShutdownChan:
				break outer
			}
		}
		output.logger.Notice("Spooler ended")
	}()
}

func (output *ForwardOutput) spawnEmitter() {
	output.logger.Notice("Spawning emitter")
	output.wg.Add(1)
	go func() {
		defer func() {
			output.spoolerShutdownChan <- struct{}{}
			output.wg.Done()
		}()
		output.logger.Notice("Emitter started")
		buffer := bytes.Buffer{}
		for recordSet := range output.emitterChan {
			buffer.Reset()
			encoder := codec.NewEncoder(&buffer, output.codec)
			addMetadata(&recordSet, output.metadata)
			err := encodeRecordSet(encoder, recordSet)
			if err != nil {
				output.logger.Error(err.Error())
				continue
			}
			output.logger.Debugf("Emitter processed %d entries", len(recordSet.Records))
			output.journal.Write(buffer.Bytes())
		}
		output.logger.Notice("Emitter ended")
	}()
}

func (output *ForwardOutput) Emit(recordSets []FluentRecordSet) error {
	defer func() {
		recover()
	}()
	for _, recordSet := range recordSets {
		output.emitterChan <- recordSet
	}
	return nil
}

func (output *ForwardOutput) String() string {
	return "output"
}

func (output *ForwardOutput) Stop() {
	if atomic.CompareAndSwapUintptr(&output.isShuttingDown, 0, 1) {
		close(output.emitterChan)
	}
}

func (output *ForwardOutput) WaitForShutdown() {
	output.completion.L.Lock()
	if !output.hasShutdownCompleted {
		output.completion.Wait()
	}
	output.completion.L.Unlock()
}

func (output *ForwardOutput) Start() {
	syncCh := make(chan struct{})
	go func() {
		<-syncCh
		output.wg.Wait()
		err := output.journalGroup.Dispose()
		if err != nil {
			output.logger.Error(err.Error())
		}
		output.completion.L.Lock()
		output.hasShutdownCompleted = true
		output.completion.Broadcast()
		output.completion.L.Unlock()
	}()
	output.spawnSpooler()
	output.spawnEmitter()
	syncCh <- struct{}{}
}

func NewForwardOutput(logger *logging.Logger, bind string, retryInterval time.Duration, connectionTimeout time.Duration, writeTimeout time.Duration, flushInterval time.Duration, journalGroupPath string, maxJournalChunkSize int64, metadata string) (*ForwardOutput, error) {
	_codec := codec.MsgpackHandle{}
	_codec.MapType = reflect.TypeOf(map[string]interface{}(nil))
	_codec.RawToString = false
	_codec.StructToArray = true

	journalFactory := NewFileJournalGroupFactory(
		logger,
		randSource,
		time.Now,
		".log",
		os.FileMode(0600),
		maxJournalChunkSize,
	)
	output := &ForwardOutput{
		logger:               logger,
		codec:                &_codec,
		bind:                 bind,
		retryInterval:        retryInterval,
		connectionTimeout:    connectionTimeout,
		writeTimeout:         writeTimeout,
		wg:                   sync.WaitGroup{},
		flushInterval:        flushInterval,
		emitterChan:          make(chan FluentRecordSet),
		spoolerShutdownChan:  make(chan struct{}),
		isShuttingDown:       0,
		completion:           sync.Cond{L: &sync.Mutex{}},
		hasShutdownCompleted: false,
		metadata:             metadata,
	}
	journalGroup, err := journalFactory.GetJournalGroup(journalGroupPath, output)
	if err != nil {
		return nil, err
	}
	output.journalGroup = journalGroup
	output.journal = journalGroup.GetJournal("output")
	return output, nil
}
