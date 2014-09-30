package main

import (
	"crypto/x509"
	"flag"
	"fmt"
	ioextras "github.com/moriyoshi/go-ioextras"
	strftime "github.com/moriyoshi/go-strftime"
	logging "github.com/op/go-logging"
	fluentd_forwarder "github.com/treasure-data/fluentd-forwarder"
	gcfg "code.google.com/p/gcfg"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime/pprof"
	"strings"
	"time"
)

type FluentdForwarderParams struct {
	RetryInterval       time.Duration
	ConnectionTimeout   time.Duration
	WriteTimeout        time.Duration
	FlushInterval       time.Duration
	Parallelism         int
	JournalGroupPath    string
	MaxJournalChunkSize int64
	ListenOn            string
	OutputType          string
	ForwardTo           string
	LogLevel            logging.Level
	LogFile             string
	DatabaseName        string
	TableName           string
	ApiKey              string
	Ssl                 bool
	SslCACertBundleFile string
	CPUProfileFile      string
}

type PortWorker interface {
	fluentd_forwarder.Port
	fluentd_forwarder.Worker
}

var progName = os.Args[0]
var progVersion string

func MustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}

func Error(fmtStr string, args ...interface{}) {
	fmt.Fprint(os.Stderr, progName, ": ")
	fmt.Fprintf(os.Stderr, fmtStr, args...)
	fmt.Fprint(os.Stderr, "\n")
}

type LogLevelValue logging.Level

func (v *LogLevelValue) String() string {
	return logging.Level(*v).String()
}

func (v *LogLevelValue) Set(s string) error {
	_v, err := logging.LogLevel(s)
	*v = LogLevelValue(_v)
	return err
}

func updateFlagsByConfig(configFile string, flagSet *flag.FlagSet) error {
	config := struct {
		Fluentd_Forwarder struct {
			Retry_interval string `retry-interval`
			Conn_timeout string `conn-timeout`
			Write_timeout string `write-timeout`
			Flush_interval string `flush-interval`
			Listen_on string `listen-on`
			To string `to`
			Buffer_path string `buffer-path`
			Buffer_chunk_limit string `buffer-chunk-limit`
			Log_level string `log-level`
			Ca_certs string `ca-certs`
			Cpuprofile string `cpuprofile`
			Log_file string `log-file`
		}
	}{}
	err := gcfg.ReadFileInto(&config, configFile)
	if err != nil {
		return err
	}
	r := reflect.ValueOf(config.Fluentd_Forwarder)
	rt := r.Type()
	for i, l := 0, rt.NumField(); i < l; i += 1 {
		f := rt.Field(i)
		fv := r.Field(i)
		v := fv.String()
		if v != "" {
			err := flagSet.Set(string(f.Tag), v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ParseArgs() *FluentdForwarderParams {
	configFile := ""
	retryInterval := (time.Duration)(0)
	connectionTimeout := (time.Duration)(0)
	writeTimeout := (time.Duration)(0)
	flushInterval := (time.Duration)(0)
	parallelism := 0
	listenOn := ""
	forwardTo := ""
	journalGroupPath := ""
	maxJournalChunkSize := int64(16777216)
	logLevel := LogLevelValue(logging.INFO)
	sslCACertBundleFile := ""
	cpuProfileFile := ""
	logFile := ""

	flagSet := flag.NewFlagSet(progName, flag.ExitOnError)

	flagSet.StringVar(&configFile, "config", "", "configuration file")
	flagSet.DurationVar(&retryInterval, "retry-interval", 0, "retry interval in which connection is tried against the remote agent")
	flagSet.DurationVar(&connectionTimeout, "conn-timeout", MustParseDuration("10s"), "connection timeout")
	flagSet.DurationVar(&writeTimeout, "write-timeout", MustParseDuration("10s"), "write timeout on wire")
	flagSet.DurationVar(&flushInterval, "flush-interval", MustParseDuration("5s"), "flush interval in which the events are forwareded to the remote agent")
	flagSet.IntVar(&parallelism, "parallelism", 1, "Number of chunks to submit at once (for td output)")
	flagSet.StringVar(&listenOn, "listen-on", "127.0.0.1:24224", "interface address and port on which the forwarder listens")
	flagSet.StringVar(&forwardTo, "to", "fluent://127.0.0.1:24225", "host and port to which the events are forwarded")
	flagSet.StringVar(&journalGroupPath, "buffer-path", "*", "directory / path on which buffer files are created. * may be used within the path to indicate the prefix or suffix like var/pre*suf")
	flagSet.Int64Var(&maxJournalChunkSize, "buffer-chunk-limit", 16777216, "Maximum size of a buffer chunk")
	flagSet.Var(&logLevel, "log-level", "log level (defaults to INFO)")
	flagSet.StringVar(&sslCACertBundleFile, "ca-certs", "", "path to SSL CA certificate bundle file")
	flagSet.StringVar(&cpuProfileFile, "cpuprofile", "", "write CPU profile to file")
	flagSet.StringVar(&logFile, "log-file", "", "path of the log file. log will be written to stderr if unspecified")
	flagSet.Parse(os.Args[1:])

	err := updateFlagsByConfig(configFile, flagSet)
	if err != nil {
		Error("%s", err.Error())
		os.Exit(1)
	}

	ssl := false
	outputType := ""
	databaseName := "*"
	tableName := "*"
	apiKey := ""

	if strings.Contains(forwardTo, "//") {
		u, err := url.Parse(forwardTo)
		if err != nil {
			Error("%s", err.Error())
			os.Exit(1)
		}
		switch u.Scheme {
		case "fluent", "fluentd":
			outputType = "fluent"
			forwardTo = u.Host
		case "td+http", "td+https":
			outputType = "td"
			forwardTo = u.Host
			if u.User != nil {
				apiKey = u.User.Username()
			}
			if u.Scheme == "td+https" {
				ssl = true
			}
			p := strings.Split(u.Path, "/")
			if len(p) > 1 {
				databaseName = p[1]
			}
			if len(p) > 2 {
				tableName = p[2]
			}
		}
	} else {
		outputType = "fluent"
	}
	if outputType == "" {
		Error("Invalid output specifier")
		os.Exit(1)
	} else if outputType == "fluent" {
		if !strings.ContainsRune(forwardTo, ':') {
			forwardTo += ":24224"
		}
	}
	return &FluentdForwarderParams{
		RetryInterval:       retryInterval,
		ConnectionTimeout:   connectionTimeout,
		WriteTimeout:        writeTimeout,
		FlushInterval:       flushInterval,
		Parallelism:         parallelism,
		ListenOn:            listenOn,
		OutputType:          outputType,
		ForwardTo:           forwardTo,
		Ssl:                 ssl,
		DatabaseName:        databaseName,
		TableName:           tableName,
		ApiKey:              apiKey,
		JournalGroupPath:    journalGroupPath,
		MaxJournalChunkSize: maxJournalChunkSize,
		LogLevel:            logging.Level(logLevel),
		LogFile:             logFile,
		SslCACertBundleFile: sslCACertBundleFile,
		CPUProfileFile:      cpuProfileFile,
	}
}

func ValidateParams(params *FluentdForwarderParams) bool {
	if params.RetryInterval < 0 {
		Error("Retry interval may not be negative")
		return false
	}
	if params.RetryInterval > 0 && params.RetryInterval < 100000000 {
		Error("Retry interval must be greater than or equal to 100ms")
		return false
	}
	if params.FlushInterval < 100000000 {
		Error("Flush interval must be greater than or equal to 100ms")
		return false
	}
	if params.FlushInterval < 100000000 {
		Error("Flush interval must be greater than or equal to 100ms")
		return false
	}
	switch params.OutputType {
	case "fluent":
		if params.RetryInterval == 0 {
			params.RetryInterval = MustParseDuration("5s")
		}
		if params.RetryInterval > params.FlushInterval {
			Error("Retry interval may not be greater than flush interval")
			return false
		}
	case "td":
		if params.RetryInterval != 0 {
			Error("Retry interval will be ignored")
			return false
		}
	}
	return true
}

func main() {
	params := ParseArgs()
	if !ValidateParams(params) {
		os.Exit(1)
	}
	logWriter := (io.Writer)(nil)
	if params.LogFile != "" {
		logWriter = ioextras.NewStaticRotatingWriter(
			func(_ interface{}) (string, error) {
				path := strftime.Format(params.LogFile, time.Now())
				return path, nil
			},
			func(path string, _ interface{}) (io.Writer, error) {
				dir, _ := filepath.Split(path)
				err := os.MkdirAll(dir, os.FileMode(0777))
				if err != nil {
					return nil, err
				}
				return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.FileMode(0666))
			},
			nil,
		)
	} else {
		logWriter = os.Stderr
	}
	logBackend := logging.NewLogBackend(logWriter, "[fluentd-forwarder] ", log.Ldate|log.Ltime|log.Lmicroseconds)
	logging.SetBackend(logBackend)
	logger := logging.MustGetLogger("fluentd-forwarder")
	logging.SetLevel(params.LogLevel, "fluentd-forwarder")
	if progVersion != "" {
		logger.Info("Version %s starting...", progVersion)
	}

	workerSet := fluentd_forwarder.NewWorkerSet()

	if params.CPUProfileFile != "" {
		f, err := os.Create(params.CPUProfileFile)
		if err != nil {
			Error(err.Error())
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	output := (PortWorker)(nil)
	err := (error)(nil)
	switch params.OutputType {
	case "fluent":
		output, err = fluentd_forwarder.NewForwardOutput(
			logger,
			params.ForwardTo,
			params.RetryInterval,
			params.ConnectionTimeout,
			params.WriteTimeout,
			params.FlushInterval,
			params.JournalGroupPath,
			params.MaxJournalChunkSize,
		)
	case "td":
		rootCAs := (*x509.CertPool)(nil)
		if params.SslCACertBundleFile != "" {
			b, err := ioutil.ReadFile(params.SslCACertBundleFile)
			if err != nil {
				Error("Failed to read CA bundle file: %s", err.Error())
				os.Exit(1)
			}
			rootCAs = x509.NewCertPool()
			if !rootCAs.AppendCertsFromPEM(b) {
				Error("No valid certificate found in %s", params.SslCACertBundleFile)
				os.Exit(1)
			}
		}
		output, err = fluentd_forwarder.NewTDOutput(
			logger,
			params.ForwardTo,
			params.ConnectionTimeout,
			params.WriteTimeout,
			params.FlushInterval,
			params.Parallelism,
			params.JournalGroupPath,
			params.MaxJournalChunkSize,
			params.ApiKey,
			params.DatabaseName,
			params.TableName,
			"",
			params.Ssl,
			rootCAs,
			"", // TODO:http-proxy
		)
	}
	if err != nil {
		Error("%s", err.Error())
		return
	}
	workerSet.Add(output)
	input, err := fluentd_forwarder.NewForwardInput(logger, params.ListenOn, output)
	if err != nil {
		Error(err.Error())
		return
	}
	workerSet.Add(input)

	signalHandler := NewSignalHandler(workerSet)
	input.Start()
	output.Start()
	signalHandler.Start()

	for _, worker := range workerSet.Slice() {
		worker.WaitForShutdown()
	}
	logger.Notice("Shutting down...")
}
