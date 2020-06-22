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
	"strconv"
	"strings"
	"sync"
)

const (
	LOG_FILENAME              = "log.txt"
	SLOT_FILENAME             = "slots.json"
	SLOT_TMP_FILENAME_PATTERN = "slots.*.json"
)

type KVStore struct {
	rwLock  sync.RWMutex
	base    map[string]string
	path    string
	version uint64
	logFile *os.File
}

func (kv *KVStore) Get(key string) (value string, ok bool) {
	// get does not require logging
	kv.rwLock.RLock()
	defer kv.rwLock.RUnlock()
	value, ok = kv.base[key]
	return
}

func (kv *KVStore) Put(key string, value string) {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()
	logEntry := fmt.Sprintf("put %q %q\n", key, value)
	if err := kv.writeLog(logEntry); err != nil {
		common.Log().Panic("Failed to flush log.",
			zap.String("logEntry", logEntry), zap.Error(err))
	}
	kv.base[key] = value
}

func (kv *KVStore) Delete(key string) (ok bool) {
	_, ok = kv.base[key]
	if ok {
		kv.rwLock.Lock()
		defer kv.rwLock.Unlock()
		logEntry := fmt.Sprintf("del %q\n", key)
		if err := kv.writeLog(logEntry); err != nil {
			common.Log().Panic("Failed to flush log.",
				zap.String("logEntry", logEntry), zap.Error(err))
		}
		delete(kv.base, key)
	}
	return
}

// Clear log entries, flush current kv in memory to slots.
// Call this when log file is getting too large.
func (kv *KVStore) Checkpoint() error {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()
	// flush base into temporary slot file
	b, err := json.Marshal(kv.base)
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

	// rename temporary slot file to actual slot file
	slotFileName := path.Join(kv.path, SLOT_FILENAME)
	if err := os.Rename(tmpSlotFile.Name(), slotFileName); err != nil {
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
	log := common.Log()
	// create the path if not exist
	if _, err := os.Stat(pathString); os.IsNotExist(err) {
		if err := os.Mkdir(pathString, 0755); err != nil {
			return nil, err
		}
	}

	// create a log file & slot file
	logFileName := path.Join(pathString, LOG_FILENAME)
	slotFileName := path.Join(pathString, SLOT_FILENAME)
	var logFile, slotFile *os.File

	var version uint64 = 0
	data := make(map[string]string)

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
		if err := slotFile.Close(); err != nil {
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
		log.Info("Recovered base from previous slot file.")
		if err := slotFile.Close(); err != nil {
			return nil, err
		}
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
		logFile, err = os.OpenFile(logFileName, os.O_RDWR|os.O_APPEND, 0644)
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
					key, err := strconv.Unquote(tokens[1])
					if err != nil {
						log.Error("Invalid log line encountered, skipping.",
							zap.Strings("tokens", tokens))
						continue
					}
					value, err := strconv.Unquote(tokens[2])
					if err != nil {
						log.Error("Invalid log line encountered, skipping.",
							zap.Strings("tokens", tokens))
						continue
					}
					data[key] = value
				}
			case "del":
				{
					if len(tokens) != 2 {
						log.Error("Invalid log line encountered, skipping.",
							zap.Strings("tokens", tokens))
						continue
					}
					key, err := strconv.Unquote(tokens[1])
					if err != nil {
						log.Error("Invalid log line encountered, skipping.",
							zap.Strings("tokens", tokens))
						continue
					}
					if _, ok := data[key]; !ok {
						log.Warn("Warning: Inconsistent del entry in log, entry does not exist.",
							zap.Strings("tokens", tokens))
						continue
					}
					delete(data, key)
				}
			default:
				log.Error("Invalid log line encountered, skipping.",
					zap.Strings("tokens", tokens))
			}
		}
		log.Info("Recovered base from log entries.")
	}

	return &KVStore{
		base:    data,
		path:    pathString,
		version: version,
		logFile: logFile,
	}, nil
}
