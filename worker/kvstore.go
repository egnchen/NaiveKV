// A in-memory KV store with WAL(write-ahead log)
// This is the data plane
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
	LOG_TMP_FILENAME_PATTERN  = "log.*.txt"
)

// interface for a kv store
type KVStore interface {
	Get(key string) (value string, ok bool)
	Put(key string, value string)
	Delete(key string) (ok bool)
	Checkpoint() error
	// Extract all values for keys that satisfies the divider function at the time this method is called.
	// When doing migration, first call extract to get all values.
	Extract(divider func(key string) bool) map[string]string
	Merge(data map[string]string) error
}

// and our implementation
// KV map is separated into two kind of maps. Base map records those contained in "slots.json",
// and latest maps contains those indicated by WAL. The array of latest maps forms a log-like data structure,
// and provide atomic undo/redo. Note that values could have been moved, so latest map could contain nil values.
type SimpleKV struct {
	// permutation is: base <- layers[0] <- layers[1] <-...<- lastLayer
	// base and layers can only be read, lastLayer can be read & write
	// so only lastLayer is locked with RWMutex
	base           map[string]string
	layers         []map[string]*string
	llLock         sync.RWMutex
	lastLayer      map[string]*string
	checkpointLock sync.Mutex
	path           string
	version        uint64
	logFile        *os.File
}

func (kv *SimpleKV) Get(key string) (string, bool) {
	// get does not require logging
	// lookup layer by layer
	kv.llLock.RLock()
	if v, ok := kv.lastLayer[key]; ok {
		kv.llLock.RUnlock()
		if v == nil {
			return "", false
		} else {
			return *v, true
		}
	}
	kv.llLock.RUnlock()

	for i := range kv.layers {
		ii := len(kv.layers) - i - 1
		if v, ok := kv.layers[ii][key]; ok {
			if v == nil {
				return "", false
			} else {
				return *v, true
			}
		}
	}
	value, ok := kv.base[key]
	return value, ok
}

func (kv *SimpleKV) Put(key string, value string) {
	kv.llLock.Lock()
	defer kv.llLock.Unlock()
	kv.writeLog("put", key, value)
	kv.syncLog()
	kv.lastLayer[key] = &value
}

// make sure that key is removed from KVStore,
// regardless of whether it exists beforehand or not.
func (kv *SimpleKV) Delete(key string) bool {
	if _, ok := kv.Get(key); !ok {
		return false
	}
	// do delete
	kv.llLock.Lock()
	defer kv.llLock.Unlock()
	kv.writeLog("del", key)
	kv.syncLog()
	kv.lastLayer[key] = nil
	return true
}

// squish last layer into layer array
// TODO BUG possible data race
func (kv *SimpleKV) saveLayer() {
	if len(kv.lastLayer) == 0 {
		return
	}
	kv.layers = append(kv.layers, kv.lastLayer)
	kv.lastLayer = make(map[string]*string)
}

// Clear log entries, flush current kv in memory to slots.
// Call this when log file is getting too large.
func (kv *SimpleKV) Checkpoint() error {
	// only one checkpoint operation can happen at a time
	kv.checkpointLock.Lock()
	defer kv.checkpointLock.Unlock()
	// make current last layer immutable
	kv.saveLayer()
	// calculate new base
	b := make(map[string]string)
	for k, v := range kv.base {
		b[k] = v
	}
	for _, l := range kv.layers {
		for k, v := range l {
			if v == nil {
				delete(b, k)
			} else {
				b[k] = *v
			}
		}
	}
	// and add the newest layer to it...
	// essentially, we are collecting the new changes made during calculation of new base
	// lock both read & write
	kv.llLock.Lock()
	for k, v := range kv.lastLayer {
		if v == nil {
			delete(b, k)
		} else {
			b[k] = *v
		}
	}
	kv.saveLayer()
	// redirect log to another temporary file
	tmpLogFile, err := ioutil.TempFile(kv.path, LOG_TMP_FILENAME_PATTERN)
	if err != nil {
		kv.llLock.Unlock()
		return err
	}
	if err := kv.logFile.Close(); err != nil {
		kv.llLock.Unlock()
		return err
	}
	kv.logFile = tmpLogFile
	kv.llLock.Unlock()
	// COMMIT POINT

	// flush into temporary slot file
	bin, err := json.Marshal(b)
	if err != nil {
		return err
	}
	tmpSlotFile, err := ioutil.TempFile(kv.path, SLOT_TMP_FILENAME_PATTERN)
	if err != nil {
		return err
	}
	if _, err := tmpSlotFile.Write(bin); err != nil {
		return err
	}
	kv.llLock.Lock()
	defer kv.llLock.Unlock()
	// rename temporary log file to actual log file
	if err := os.Rename(tmpLogFile.Name(), path.Join(kv.path, LOG_FILENAME)); err != nil {
		return err
	}
	// rename temporary slot file to actual slot file
	slotFileName := path.Join(kv.path, SLOT_FILENAME)
	if err := os.Rename(tmpSlotFile.Name(), slotFileName); err != nil {
		return err
	}
	return nil
}

// write to log asyncly
func (kv *SimpleKV) writeLog(op string, args ...string) {
	// convert to quoted strings
	var quoted []string
	for _, v := range args {
		quoted = append(quoted, strconv.Quote(v))
	}
	l := fmt.Sprintf("%s %s\n", op, strings.Join(quoted, " "))
	_, err := kv.logFile.WriteString(l)
	if err != nil {
		common.Log().Error("Failed to write log",
			zap.String("op", op), zap.Strings("args", args), zap.Error(err))
	}
}

// flush log
func (kv *SimpleKV) syncLog() {
	if err := kv.logFile.Sync(); err != nil {
		common.Log().Error("Failed to flush log.", zap.Error(err))
	}
}

func NewKVStore(pathString string) (*SimpleKV, error) {
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
	base := make(map[string]string)
	latest := make(map[string]*string)

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
		if err := json.Unmarshal(b, &base); err != nil {
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
		latest, err = ReadLog(scanner)
		if err != nil {
			return nil, err
		}
		log.Info("Recovered base from log entries.")
	}

	return &SimpleKV{
		base:      base,
		layers:    make([]map[string]*string, 0),
		lastLayer: latest,
		path:      pathString,
		version:   version,
		logFile:   logFile,
	}, nil
}

func ReadLog(scanner *bufio.Scanner) (map[string]*string, error) {
	log := common.Log()
	logErr := func(tokens []string) {
		log.Error("Invalid log line encountered, skipping.", zap.Strings("tokens", tokens))
	}
	ret := make(map[string]*string)
	tran := make(map[string]*string)
	cur := ret
	for scanner.Scan() {
		tokens := strings.Fields(scanner.Text())
		if len(tokens) == 0 {
			continue
		}
		switch tokens[0] {
		case "put":
			if len(tokens) != 3 {
				logErr(tokens)
				continue
			}
			key, err := strconv.Unquote(tokens[1])
			if err != nil {
				logErr(tokens)
				continue
			}
			value, err := strconv.Unquote(tokens[2])
			if err != nil {
				logErr(tokens)
				continue
			}
			cur[key] = &value
		case "del":
			if len(tokens) != 2 {
				logErr(tokens)
				continue
			}
			key, err := strconv.Unquote(tokens[1])
			if err != nil {
				logErr(tokens)
				continue
			}
			// TODO check multiple deletes
			cur[key] = nil
		case "transaction":
			if len(tokens) != 2 {
				logErr(tokens)
				continue
			}
			op := tokens[1]
			if op == "begin" {
				tran = make(map[string]*string)
				cur = tran
			} else if op == "end" {
				// flush back to ret
				for k, v := range tran {
					ret[k] = v
				}
				cur = ret
			} else {
				logErr(tokens)
				continue
			}
		default:
			logErr(tokens)
		}
	}
	return ret, nil
}

func (kv *SimpleKV) Extract(divider func(key string) bool) map[string]string {
	// make last layer immutable
	kv.saveLayer()
	b := make(map[string]string)
	for k, v := range kv.base {
		if divider(k) {
			b[k] = v
		}
	}
	for _, l := range kv.layers {
		for k, v := range l {
			if divider(k) {
				if v == nil {
					delete(b, k)
				} else {
					b[k] = *v
				}
			}
		}
	}
	return b
}

func (kv *SimpleKV) Merge(data map[string]string) error {
	kv.llLock.Lock()
	defer kv.llLock.Unlock()
	// use transaction pattern in log file :)
	kv.writeLog("transaction", "begin")
	for k, v := range data {
		kv.writeLog("put", k, v)
		kv.lastLayer[k] = &v
	}
	kv.writeLog("transaction", "end")
	kv.syncLog()
	return nil
}

func (kv *SimpleKV) Close() {
	log := common.Log()
	if err := kv.logFile.Sync(); err != nil {
		log.Panic("Failed to sync.", zap.Error(err))
	}
	if err := kv.logFile.Close(); err != nil {
		log.Panic("Failed to close.", zap.Error(err))
	}
}
