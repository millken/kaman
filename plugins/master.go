package plugins

import (
	"os"
	"sync"
	"syscall"
)

type MasterConfig struct {
	PoolSize       int
	PluginChanSize int
	MaxMsgLoops    uint
	stopping       bool
	stoppingMutex  sync.RWMutex
	BaseDir        string
	sigChan        chan os.Signal
	Hostname       string
}

func DefaultMasterConfig() (master *MasterConfig) {
	hostname, _ := os.Hostname()
	return &MasterConfig{
		PoolSize:       100,
		PluginChanSize: 50,
		MaxMsgLoops:    4,
		sigChan:        make(chan os.Signal, 1),
		Hostname:       hostname,
	}
}

func (self *MasterConfig) SigChan() chan os.Signal {
	return self.sigChan
}

func (self *MasterConfig) ShutDown() {
	go func() {
		self.sigChan <- syscall.SIGINT
	}()
}

func (self *MasterConfig) IsShuttingDown() (stopping bool) {
	self.stoppingMutex.RLock()
	stopping = self.stopping
	self.stoppingMutex.RUnlock()
	return
}

func (self *MasterConfig) stop() {
	self.stoppingMutex.Lock()
	self.stopping = true
	self.stoppingMutex.Unlock()
}
