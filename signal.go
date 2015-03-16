package main

import (
	"os"
	"os/signal"
)

type SignalHandler struct {
	Workers    *WorkerSet
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

func NewSignalHandler(workerSet *WorkerSet) *SignalHandler {
	return &SignalHandler{
		workerSet,
		make(chan os.Signal, 1),
	}
}
