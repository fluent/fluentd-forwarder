package main

import (
	fluentd_forwarder "github.com/treasure-data/fluentd-forwarder"
	logging "github.com/op/go-logging"
	"log"
	"os"
	"time"
)

func main() {
	logBackend := logging.NewLogBackend(os.Stderr, "[fluentd-forwarder] ", log.Ltime)
	logging.SetBackend(logBackend)
	logging.SetLevel(logging.INFO, "fluentd-forwarder")
	logger := logging.MustGetLogger("fluentd-forwarder")
	workerSet := fluentd_forwarder.NewWorkerSet()
	retryInterval, err := time.ParseDuration("5s")
	if err != nil {
		logger.Error("%#v", err)
		return
	}
	connectionTimeout, err := time.ParseDuration("10s")
	if err != nil {
		logger.Error("%#v", err)
		return
	}
	writeTimeout, err := time.ParseDuration("10s")
	if err != nil {
		logger.Error("%#v", err)
		return
	}
	flushInterval, err := time.ParseDuration("5s")
	if err != nil {
		logger.Error("%#v", err)
		return
	}
	output, err := fluentd_forwarder.NewForwardOutput(
		logger,
		"127.0.0.1:24225",
		retryInterval,
		connectionTimeout,
		writeTimeout,
		flushInterval,
	)
	if err != nil {
		logger.Error("%#v", err)
		return
	}
	workerSet.Add(output)
	input, err := fluentd_forwarder.NewForwardInput(logger, "127.0.0.1:24224", output)
	if err != nil {
		logger.Error("%#v", err)
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
