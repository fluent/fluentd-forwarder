package main

import (
	fluentd_forwarder "github.com/treasure-data/fluentd-forwarder"
	logging "github.com/op/go-logging"
	"flag"
	"log"
	"os"
	"fmt"
	"time"
)

type FluentdForwarderParams struct {
	RetryInterval time.Duration
	ConnectionTimeout time.Duration
	WriteTimeout time.Duration
	FlushInterval time.Duration
	JournalGroupPath string
	MaxJournalChunkSize int64
	ListenOn string
	ForwardTo string
}

var progName = os.Args[0]

func MustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}

func Error(fmtStr string, args... interface{}) {
	fmt.Fprint(os.Stderr, progName, " ")
	fmt.Fprintf(os.Stderr, fmtStr, args)
	fmt.Fprint(os.Stderr, "\n")
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

	flagSet := flag.NewFlagSet(progName, flag.ExitOnError)

	flagSet.DurationVar(&retryInterval, "retry-interval", MustParseDuration("5s"), "retry interval in which connection is tried against the remote agent")
	flagSet.DurationVar(&connectionTimeout, "conn-timeout", MustParseDuration("10s"), "connection timeout")
	flagSet.DurationVar(&writeTimeout, "write-timeout", MustParseDuration("10s"), "write timeout on wire")
	flagSet.DurationVar(&flushInterval, "flush-interval", MustParseDuration("5s"), "flush interval in which the events are forwareded to the remote agent")
	flagSet.StringVar(&listenOn, "listen-on", "127.0.0.1:24224", "interface address and port on which the forwarder listens")
	flagSet.StringVar(&forwardTo, "to", "127.0.0.1:24225", "host and port to which the events are forwarded")
	flagSet.StringVar(&journalGroupPath, "buffer-path", "*", "directory / path on which buffer files are created. * may be used within the path to indicate the prefix or suffix like var/pre*suf")
	flagSet.Int64Var(&maxJournalChunkSize, "buffer-chunk-limit", 16777216, "Maximum size of a buffer chunk")
	flagSet.Parse(os.Args[1:])

	return &FluentdForwarderParams {
		RetryInterval: retryInterval,
		ConnectionTimeout: connectionTimeout,
		WriteTimeout: writeTimeout,
		FlushInterval: flushInterval,
		ListenOn: listenOn,
		ForwardTo: forwardTo,
		JournalGroupPath: journalGroupPath,
		MaxJournalChunkSize: maxJournalChunkSize,
	}
}

func main() {
	logBackend := logging.NewLogBackend(os.Stderr, "[fluentd-forwarder] ", log.Ltime)
	logging.SetBackend(logBackend)
	logging.SetLevel(logging.INFO, "fluentd-forwarder")
	logger := logging.MustGetLogger("fluentd-forwarder")
	workerSet := fluentd_forwarder.NewWorkerSet()

	params := ParseArgs()

	output, err := fluentd_forwarder.NewForwardOutput(
		logger,
		params.ForwardTo,
		params.RetryInterval,
		params.ConnectionTimeout,
		params.WriteTimeout,
		params.FlushInterval,
		params.JournalGroupPath,
		params.MaxJournalChunkSize,
	)
	if err != nil {
		Error(err.Error())
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
