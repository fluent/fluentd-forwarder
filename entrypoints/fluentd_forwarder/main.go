package main

import (
	"crypto/x509"
	"flag"
	"fmt"
	logging "github.com/op/go-logging"
	fluentd_forwarder "github.com/treasure-data/fluentd-forwarder"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strings"
	"time"
)

type FluentdForwarderParams struct {
	RetryInterval       time.Duration
	ConnectionTimeout   time.Duration
	WriteTimeout        time.Duration
	FlushInterval       time.Duration
	JournalGroupPath    string
	MaxJournalChunkSize int64
	ListenOn            string
	OutputType          string
	ForwardTo           string
	LogLevel            logging.Level
	DatabaseName        string
	TableName           string
	ApiKey              string
	Ssl                 bool
	SslCACertBundleFile string
}

type PortWorker interface {
	fluentd_forwarder.Port
	fluentd_forwarder.Worker
}

var progName = os.Args[0]

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

func ParseArgs() *FluentdForwarderParams {
	retryInterval := (time.Duration)(0)
	connectionTimeout := (time.Duration)(0)
	writeTimeout := (time.Duration)(0)
	flushInterval := (time.Duration)(0)
	listenOn := ""
	forwardTo := ""
	journalGroupPath := ""
	maxJournalChunkSize := int64(16777216)
	logLevel := LogLevelValue(logging.INFO)
	sslCACertBundleFile := ""

	flagSet := flag.NewFlagSet(progName, flag.ExitOnError)

	flagSet.DurationVar(&retryInterval, "retry-interval", MustParseDuration("5s"), "retry interval in which connection is tried against the remote agent")
	flagSet.DurationVar(&connectionTimeout, "conn-timeout", MustParseDuration("10s"), "connection timeout")
	flagSet.DurationVar(&writeTimeout, "write-timeout", MustParseDuration("10s"), "write timeout on wire")
	flagSet.DurationVar(&flushInterval, "flush-interval", MustParseDuration("5s"), "flush interval in which the events are forwareded to the remote agent")
	flagSet.StringVar(&listenOn, "listen-on", "127.0.0.1:24224", "interface address and port on which the forwarder listens")
	flagSet.StringVar(&forwardTo, "to", "fluent://127.0.0.1:24225", "host and port to which the events are forwarded")
	flagSet.StringVar(&journalGroupPath, "buffer-path", "*", "directory / path on which buffer files are created. * may be used within the path to indicate the prefix or suffix like var/pre*suf")
	flagSet.Int64Var(&maxJournalChunkSize, "buffer-chunk-limit", 16777216, "Maximum size of a buffer chunk")
	flagSet.Var(&logLevel, "log-level", "log level (defaults to INFO)")
	flagSet.StringVar(&sslCACertBundleFile, "ca-certs", "", "path to SSL CA certificate bundle file")
	flagSet.Parse(os.Args[1:])

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
			apiKey = u.User.Username()
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
		SslCACertBundleFile: sslCACertBundleFile,
	}
}

func main() {
	logBackend := logging.NewLogBackend(os.Stderr, "[fluentd-forwarder] ", log.Ltime)
	logging.SetBackend(logBackend)
	logger := logging.MustGetLogger("fluentd-forwarder")
	params := ParseArgs()
	logging.SetLevel(params.LogLevel, "fluentd-forwarder")

	workerSet := fluentd_forwarder.NewWorkerSet()

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
			params.RetryInterval,
			params.ConnectionTimeout,
			params.WriteTimeout,
			params.FlushInterval,
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
