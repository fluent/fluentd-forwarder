package fluentd_forwarder

import (
	"bytes"
	"compress/gzip"
	"crypto/x509"
	"errors"
	logging "github.com/op/go-logging"
	td_client "github.com/treasure-data/td-client-go"
	"github.com/ugorji/go/codec"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type tdOutputSpooler struct {
	daemon         *tdOutputSpoolerDaemon
	ticker         *time.Ticker
	tag            string
	databaseName   string
	tableName      string
	key            string
	journal        Journal
	shutdownChan   chan struct{}
	isShuttingDown uintptr
	client         *td_client.TDClient
}

type tdOutputSpoolerDaemon struct {
	output       *TDOutput
	shutdownChan chan struct{}
	spoolersMtx  sync.Mutex
	spoolers     map[string]*tdOutputSpooler
	tempFactory  TempFileRandomAccessStoreFactory
	wg           sync.WaitGroup
	endNotify    func(*tdOutputSpoolerDaemon)
}

type TDOutput struct {
	logger         *logging.Logger
	codec          *codec.MsgpackHandle
	databaseName   string
	tableName      string
	tempDir        string
	enc            *codec.Encoder
	conn           net.Conn
	flushInterval  time.Duration
	wg             sync.WaitGroup
	journalGroup   JournalGroup
	emitterChan    chan FluentRecordSet
	spoolerDaemon  *tdOutputSpoolerDaemon
	isShuttingDown uintptr
	client         *td_client.TDClient
	sem            chan struct{}
	gcChan         chan *os.File
	completion     sync.Cond
	hasShutdownCompleted bool
}

func encodeRecords(encoder *codec.Encoder, records []TinyFluentRecord) error {
	for _, record := range records {
		e := map[string]interface{}{"time": record.Timestamp}
		for k, v := range record.Data {
			e[k] = v
		}
		err := encoder.Encode(e)
		if err != nil {
			return err
		}
	}
	return nil
}

func (spooler *tdOutputSpooler) cleanup() {
	spooler.ticker.Stop()
	spooler.journal.Dispose()
	spooler.daemon.wg.Done()
}

func (spooler *tdOutputSpooler) handle() {
	defer spooler.cleanup()
	spooler.daemon.output.logger.Notice("Spooler started")
outer:
	for {
		select {
		case <-spooler.ticker.C:
			spooler.daemon.output.logger.Notice("Flushing...")
			err := spooler.journal.Flush(func(chunk JournalChunk) interface{} {
				defer chunk.Dispose()
				if atomic.LoadUintptr(&spooler.isShuttingDown) != 0 {
					return errors.New("Flush aborted")
				}
				spooler.daemon.output.logger.Info("Flushing chunk %s", chunk.String())
				size, err := chunk.Size()
				if err != nil {
					return err
				}
				if size == 0 {
					return nil
				}
				futureErr := make(chan error, 1)
				sem := spooler.daemon.output.sem
				sem <- struct{}{}
				go func(size int64, chunk JournalChunk, futureErr chan error) {
					err := (error)(nil)
					defer func() {
						if err != nil {
							spooler.daemon.output.logger.Info("Failed to flush chunk %s (reason: %s)", chunk.String(), err.Error())
						} else {
							spooler.daemon.output.logger.Info("Completed flushing chunk %s", chunk.String())
						}
						<-sem
						// disposal must be done before notifying the initiator 
						chunk.Dispose()
						futureErr <- err
					}()
					err = func() error {
						compressingBlob := NewCompressingBlob(
							chunk,
							maxInt(4096, int(size/4)),
							gzip.BestSpeed,
							&spooler.daemon.tempFactory,
						)
						defer compressingBlob.Dispose()
						_, err := spooler.client.Import(
							spooler.databaseName,
							spooler.tableName,
							"msgpack.gz",
							td_client.NewBufferingBlobSize(
								compressingBlob,
								maxInt(4096, int(size/16)),
							),
							chunk.Id(),
						)
						return err
					}()
				}(size, chunk.Dup(), futureErr)
				return (<-chan error)(futureErr)
			})
			if err != nil {
				spooler.daemon.output.logger.Error("Error during reading from the journal: %s", err.Error())
			}
		case <-spooler.shutdownChan:
			break outer
		}
	}
	spooler.daemon.output.logger.Notice("Spooler ended")
}

func normalizeDatabaseName(name string) (string, error) {
	name_ := ([]byte)(name)
	if len(name_) == 0 {
		return "", errors.New("Empty name is not allowed")
	}
	if len(name_) < 3 {
		name_ = append(name_, ("___"[0 : 3-len(name)])...)
	}
	if 255 < len(name_) {
		name_ = append(name_[0:253], "__"...)
	}
	name_ = bytes.ToLower(name_)
	for i, c := range name_ {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			c = '_'
		}
		name_[i] = c
	}
	return (string)(name_), nil
}

func normalizeTableName(name string) (string, error) {
	return normalizeDatabaseName(name)
}

func newTDOutputSpooler(daemon *tdOutputSpoolerDaemon, databaseName, tableName, key string) *tdOutputSpooler {
	journal := daemon.output.journalGroup.GetJournal(key)
	return &tdOutputSpooler{
		daemon:       daemon,
		ticker:       time.NewTicker(daemon.output.flushInterval),
		databaseName: databaseName,
		tableName:    tableName,
		key:          key,
		journal:      journal,
		shutdownChan: make(chan struct{}, 1),
		isShuttingDown: 0,
		client:       daemon.output.client,
	}
}

func (daemon *tdOutputSpoolerDaemon) spawnSpooler(databaseName, tableName, key string) *tdOutputSpooler {
	daemon.spoolersMtx.Lock()
	defer daemon.spoolersMtx.Unlock()
	spooler, exists := daemon.spoolers[key]
	if exists {
		return spooler
	}
	spooler = newTDOutputSpooler(daemon, databaseName, tableName, key)
	daemon.output.logger.Notice("Spawning spooler " + spooler.key)
	daemon.spoolers[spooler.key] = spooler
	daemon.wg.Add(1)
	go spooler.handle()
	return spooler
}

func (daemon *tdOutputSpoolerDaemon) cleanup() {
	func() {
		daemon.spoolersMtx.Lock()
		defer daemon.spoolersMtx.Unlock()
		for _, spooler := range daemon.spoolers {
			if atomic.CompareAndSwapUintptr(&spooler.isShuttingDown, 0, 1) {
				spooler.shutdownChan <- struct{}{}
			}
		}
	}()
	daemon.wg.Wait()
	if daemon.endNotify != nil {
		daemon.endNotify(daemon)
	}
	daemon.output.wg.Done()
}

func (daemon *tdOutputSpoolerDaemon) handle() {
	defer daemon.cleanup()
	daemon.output.logger.Notice("Spooler daemon started")
	// spawn Spooler according to the existing journals
	for _, key := range daemon.output.journalGroup.GetJournalKeys() {
		pair := strings.SplitN(key, ".", 2)
		if len(pair) != 2 {
			daemon.output.logger.Warning("Journal %s ignored", key)
			continue
		}
		daemon.spawnSpooler(pair[0], pair[1], key)
	}
outer:
	for {
		select {
		case <-daemon.shutdownChan:
			break outer
		}
	}
	daemon.output.logger.Notice("Spooler daemon ended")
}

func newTDOutputSpoolerDaemon(output *TDOutput) *tdOutputSpoolerDaemon {
	return &tdOutputSpoolerDaemon{
		output:       output,
		shutdownChan: make(chan struct{}, 1),
		spoolers:     make(map[string]*tdOutputSpooler),
		tempFactory:  TempFileRandomAccessStoreFactory{output.tempDir, "", output.gcChan},
		wg:           sync.WaitGroup{},
		endNotify: func(*tdOutputSpoolerDaemon) {
			close(output.gcChan)
		},
	}
}

func (output *TDOutput) spawnSpoolerDaemon() {
	output.logger.Notice("Spawning spooler daemon")
	output.spoolerDaemon = newTDOutputSpoolerDaemon(output)
	output.wg.Add(1)
	go output.spoolerDaemon.handle()
}

func (daemon *tdOutputSpoolerDaemon) getSpooler(tag string) (*tdOutputSpooler, error) {
	databaseName := daemon.output.databaseName
	tableName := daemon.output.tableName
	if databaseName == "*" {
		if tableName == "*" {
			c := strings.SplitN(tag, ".", 2)
			if len(c) == 1 {
				tableName = c[0]
			} else if len(c) == 2 {
				databaseName = c[0]
				tableName = c[1]
			}
		} else {
			databaseName = tag
		}
	} else {
		if tableName == "*" {
			tableName = tag
		}
	}
	databaseName, err := normalizeDatabaseName(databaseName)
	if err != nil {
		return nil, err
	}
	tableName, err = normalizeTableName(tableName)
	if err != nil {
		return nil, err
	}
	key := databaseName + "." + tableName
	return daemon.spawnSpooler(databaseName, tableName, key), nil
}

func (output *TDOutput) spawnEmitter() {
	output.logger.Notice("Spawning emitter")
	output.wg.Add(1)
	go func() {
		defer func() {
			output.spoolerDaemon.shutdownChan <- struct{}{}
			output.wg.Done()
		}()
		output.logger.Notice("Emitter started")
		buffer := bytes.Buffer{}
		for recordSet := range output.emitterChan {
			buffer.Reset()
			encoder := codec.NewEncoder(&buffer, output.codec)
			err := func() error {
				spooler, err := output.spoolerDaemon.getSpooler(recordSet.Tag)
				if err != nil {
					return err
				}
				err = encodeRecords(encoder, recordSet.Records)
				if err != nil {
					return err
				}
				output.logger.Debug("Emitter processed %d entries", len(recordSet.Records))
				return spooler.journal.Write(buffer.Bytes())
			}()
			if err != nil {
				output.logger.Error("%s", err.Error())
				continue
			}
		}
		output.logger.Notice("Emitter ended")
	}()
}

func (output *TDOutput) spawnTempFileCollector() {
	output.logger.Notice("Spawning temporary file collector")
	output.wg.Add(1)
	go func() {
		defer func() {
			output.wg.Done()
		}()
		for f := range output.gcChan {
			output.logger.Debug("Deleting %s...", f.Name())
			err := os.Remove(f.Name())
			if err != nil {
				output.logger.Warning("Failed to delete %s: %s", f.Name(), err.Error())
			}
		}
		output.logger.Debug("temporary file collector ended")
	}()
}

func (output *TDOutput) Emit(recordSets []FluentRecordSet) error {
	defer func() {
		recover()
	}()
	for _, recordSet := range recordSets {
		output.emitterChan <- recordSet
	}
	return nil
}

func (output *TDOutput) String() string {
	return "output"
}

func (output *TDOutput) Stop() {
	if atomic.CompareAndSwapUintptr(&output.isShuttingDown, 0, 1) {
		close(output.emitterChan)
	}
}

func (output *TDOutput) WaitForShutdown() {
	output.completion.L.Lock()
	if !output.hasShutdownCompleted {
		output.completion.Wait()
	}
	output.completion.L.Unlock()
}

func (output *TDOutput) Start() {
	syncCh := make(chan struct{})
	go func() {
		<-syncCh
		output.wg.Wait()
		err := output.journalGroup.Dispose()
		if err != nil {
			output.logger.Error("%s", err.Error())
		}
		output.completion.L.Lock()
		output.hasShutdownCompleted = true
		output.completion.Broadcast()
		output.completion.L.Unlock()
	}()
	output.spawnTempFileCollector()
	output.spawnEmitter()
	output.spawnSpoolerDaemon()
	syncCh <- struct{}{}
}

func NewTDOutput(
	logger *logging.Logger,
	endpoint string,
	connectionTimeout time.Duration,
	writeTimeout time.Duration,
	flushInterval time.Duration,
	parallelism int,
	journalGroupPath string,
	maxJournalChunkSize int64,
	apiKey string,
	databaseName string,
	tableName string,
	tempDir string,
	useSsl bool,
	rootCAs *x509.CertPool,
	httpProxy string,
) (*TDOutput, error) {
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
	router := (td_client.EndpointRouter)(nil)
	if endpoint != "" {
		router = &td_client.FixedEndpointRouter{endpoint}
	}
	httpProxy_ := (interface{})(nil)
	if httpProxy != "" {
		httpProxy_ = httpProxy
	}
	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey:            apiKey,
		Router:            router,
		ConnectionTimeout: connectionTimeout,
		// ReadTimeout: readTimeout, // TODO
		SendTimeout: writeTimeout,
		Ssl:         useSsl,
		RootCAs:     rootCAs,
		Proxy:       httpProxy_,
	})
	if err != nil {
		return nil, err
	}
	output := &TDOutput{
		logger:         logger,
		codec:          &_codec,
		wg:             sync.WaitGroup{},
		flushInterval:  flushInterval,
		emitterChan:    make(chan FluentRecordSet),
		isShuttingDown: 0,
		client:         client,
		databaseName:   databaseName,
		tableName:      tableName,
		tempDir:        tempDir,
		sem:            make(chan struct{}, parallelism),
		gcChan:         make(chan *os.File, 10),
		completion:     sync.Cond{L: &sync.Mutex{}},
		hasShutdownCompleted: false,
	}
	journalGroup, err := journalFactory.GetJournalGroup(journalGroupPath, output)
	if err != nil {
		return nil, err
	}
	output.journalGroup = journalGroup
	return output, nil
}
