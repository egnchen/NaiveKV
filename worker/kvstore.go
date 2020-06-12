// A in-memory KV store with WAL(write-ahead log)
package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/eyeKill/KV/common"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
)

const (
	LOG_FILENAME              = "log"
	SLOT_FILENAME             = "slots.json"
	SLOT_TMP_FILENAME_PATTERN = "slots.*.json"
)

type KVStore struct {
	rwLock   sync.RWMutex
	data     map[string]string
	path     string
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
	if err := kv.writeLog(fmt.Sprintf("put %s %s\n", key, value)); err != nil {
		common.Log().Panic("Failed to flush log.",
			zap.String("op", "put"),
			zap.String("key", key), zap.String("value", value),
			zap.Error(err))
	}
	kv.data[key] = value
}

func (kv *KVStore) Delete(key string) (ok bool) {
	_, ok = kv.data[key]
	if ok {
		kv.rwLock.Lock()
		defer kv.rwLock.Unlock()
		if err := kv.writeLog(fmt.Sprintf("del %s\n", key)); err != nil {
			common.Log().Panic("Failed to flush log.",
				zap.String("op", "delete"), zap.String("key", key),
				zap.Error(err))
		}
		delete(kv.data, key)
	}
	return
}

// Clear log entries, flush current kv in memory to slots.
// Call this when log file is getting too large.
func (kv *KVStore) Flush() error {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()
	// flush data into temporary slot file
	b, err := json.Marshal(kv.data)
	if err != nil {
		return err
	}
	tmpSlotFile, err := ioutil.TempFile(kv.path, SLOT_TMP_FILENAME_PATTERN)
	if err != nil {
		return err
	}
	if _, err := tmpSlotFile.Write(b); err != nil {
		return err
	}
	// truncate log
	if err := os.Truncate(path.Join(kv.path, LOG_FILENAME), 0); err != nil {
		return err
	}

	// All-or-nothing transition here
	// note that rwlock **can** be release now

	// remove previous slots file
	if err := os.Remove(path.Join(kv.path, SLOT_FILENAME)); err != nil {
		return err
	}
	// move temporary file to actual file
	if err := os.Rename(tmpSlotFile.Name(), kv.slotFile.Name()); err != nil {
		return err
	}
	return nil
}

func (kv *KVStore) writeLog(content string) error {
	if _, err := kv.logFile.WriteString(content); err != nil {
		return err
	}
	if err := kv.logFile.Sync(); err != nil {
		return err
	}
	return nil
}

func NewKVStore(pathString string) (*KVStore, error) {
	// create a log file & slot file
	log := common.Log()

	logFileName := path.Join(pathString, LOG_FILENAME)
	slotFileName := path.Join(pathString, SLOT_FILENAME)
	var logFile, slotFile *os.File

	var version uint64 = 0
	var data map[string]string = make(map[string]string)

	// open / create slot file
	if _, err := os.Stat(slotFileName); os.IsNotExist(err) {
		slotFile, err = os.Create(slotFileName)
		if err != nil {
			return nil, err
		}
		// initialize slot file
		if _, err := slotFile.Write([]byte("{}")); err != nil {
			return nil, err
		}
		if err := slotFile.Sync(); err != nil {
			return nil, err
		}
		log.Info("Created new slot file.", zap.String("path", slotFileName))
	} else {
		// read slot file
		slotFile, err = os.Open(slotFileName)
		if err != nil {
			return nil, err
		}
		b, err := ioutil.ReadAll(slotFile)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &data); err != nil {
			return nil, err
		}
		log.Info("Recovered data from previous slot file.")
	}
	// open / create log file
	if _, err := os.Stat(logFileName); os.IsNotExist(err) {
		logFile, err = os.Create(logFileName)
		if err != nil {
			return nil, err
		}
		log.Info("Created new log file.", zap.String("path", logFileName))
	} else {
		// read log file & parse it
		logFile, err = os.Open(logFileName)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(logFile)
		for scanner.Scan() {
			tokens := strings.Fields(scanner.Text())
			if len(tokens) == 0 {
				continue
			}
			switch tokens[0] {
			case "put":
				{
					if len(tokens) != 3 {
						log.Error("Invalid log line encountered, skipping.",
							zap.Strings("tokens", tokens))
						continue
					}
					data[tokens[1]] = tokens[2]
				}
			case "del":
				{
					if len(tokens) != 2 {
						log.Error("Invalid log line encountered, skipping.",
							zap.Strings("tokens", tokens))
						continue
					}
					if _, ok := data[tokens[1]]; !ok {
						log.Warn("Warning: Inconsistent del entry in log, entry does not exist.",
							zap.Strings("tokens", tokens))
						continue
					}
					delete(data, tokens[1])
				}
			default:
				log.Error("Invalid log line encountered, skipping.",
					zap.Strings("tokens", tokens))
			}
		}
		log.Info("Recovered data from log entries.")
	}

	return &KVStore{
		data:     data,
		path:     pathString,
		version:  version,
		logFile:  logFile,
		slotFile: slotFile,
	}, nil
}
