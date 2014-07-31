package main

import (
	"os"
	"os/signal"
	fluentd_forwarder "github.com/treasure-data/fluentd-forwarder"
)

type SignalHandler struct {
	Workers *fluentd_forwarder.WorkerSet
	signalChan chan os.Signal
}

func (handler *SignalHandler) Start() {
	signal.Notify(handler.signalChan, os.Kill, os.Interrupt)
	go func() {
		<-handler.signalChan
		for _, worker := range handler.Workers.Slice() {
			worker.Stop()
		}
	}()
}

func NewSignalHandler(workerSet *fluentd_forwarder.WorkerSet) *SignalHandler {
	return &SignalHandler {
		workerSet,
		make(chan os.Signal, 1),
	}
}
