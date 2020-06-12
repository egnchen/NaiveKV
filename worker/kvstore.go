// A in-memory KV store with Write-Ahead-Log
package worker

import (
	"fmt"
	"github.com/eyeKill/KV/common"
	"go.uber.org/zap"
	"os"
	"path"
	"sync"
)

type KVStore struct {
	rwLock   sync.RWMutex
	data     map[string]string
	version  uint64
	logFile  *os.File
	slotFile *os.File
}

func (kv *KVStore) Get(key string) (value string, ok bool) {
	// get does not require logging
	kv.rwLock.RLock()
	defer kv.rwLock.RUnlock()
	value, ok = kv.data[key]
	return
}

func (kv *KVStore) Put(key string, value string) {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()
	// critical section
	kv.writeLog(fmt.Sprintf("put %s %s\n", key, value))
	kv.data[key] = value
}

func (kv *KVStore) Delete(key string) (ok bool) {
	_, ok = kv.data[key]
	if ok {
		kv.rwLock.Lock()
		defer kv.rwLock.Unlock()
		// critical section
		kv.writeLog(fmt.Sprintf("delete %s\n", key))
		delete(kv.data, key)
	}
	return
}

func (kv *KVStore) writeLog(content string) {
	if _, err := kv.logFile.WriteString(content); err != nil {
		common.Log().Panic("Failed to write to log.", zap.Error(err))
	}
	if err := kv.logFile.Sync(); err != nil {
		common.Log().Panic("Failed to flush to log", zap.Error(err))
	}
}

func NewKVStore(pathString string) (*KVStore, error) {
	// create a log file & slot file
	var version uint64 = 0
	logFileName := path.Join(pathString, "log")
	slotFileName := path.Join(pathString, "slots.json")

	// open / create log file
	logFile, err := os.OpenFile(logFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		common.Log().Error("Failed to open/create file.",
			zap.String("path", logFileName), zap.Error(err))
		return nil, err
	}

	// open / create slot file
	slotFile, err := os.OpenFile(slotFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		common.Log().Error("Failed to open/create file.",
			zap.String("path", logFileName), zap.Error(err))
		return nil, err
	}

	return &KVStore{
		data:     make(map[string]string),
		version:  version,
		logFile:  logFile,
		slotFile: slotFile,
	}, nil
}
