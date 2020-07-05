// A in-memory KV store with WAL(write-ahead log)
// This is the data plane
package worker

import (
	"bufio"
	"encoding/json"
	"errors"
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

var (
	ENOENT    = errors.New("entry does not exist")
	EINVTRANS = errors.New("invalid transaction id")
	ENOTRANS  = errors.New("no available transaction id, try again later")
)

const (
	TRANSACTION_COUNT = 16
)

// interface for a kv store
type KVStore interface {
	Get(key string, transactionId int) (value string, err error)
	Put(key string, value string, transactionId int) (version uint64, err error)
	Delete(key string, transactionId int) (version uint64, err error)
	// transactional APIs
	StartTransaction() (transactionId int, err error)
	Rollback(transactionId int) error
	Commit(transactionId int) error
	// persist kv store
	Flush()
	Checkpoint() error
	// Extract all values for keys that satisfies the divider function at the time this method is called.
	// This method should not block. When doing calculation, the KVStore should continue to serve on other threads.
	Extract(divider func(key string) bool, version uint64) map[string]ValueWithVersion
}

type ValueWithVersion struct {
	Value   *string
	Version uint64
}

func NewValueWithVersion(value string, version uint64) ValueWithVersion {
	return ValueWithVersion{
		Value:   &value,
		Version: version,
	}
}

type TransactionStruct struct {
	Lock  sync.RWMutex
	Layer map[string]ValueWithVersion
}

// and our implementation
// KV map is separated into two kind of maps. Base map records those contained in "slots.json",
// and latest maps contains those indicated by WAL. The array of latest maps forms a log-like data structure,
// and provide atomic undo/redo. Note that values could have been moved, so latest map could contain nil values.
type SimpleKV struct {
	// permutation is: base <- layers[0] <- layers[1] <-...<- different transaction layers
	// base and layers can only be read and the last layer can be read & write
	// so only transaction layer is locked with RWMutex.
	// For non-zero transactions, content in zero transactions are also read when getting data,
	// so this KV store provides read-committed transaction isolation level.
	base           map[string]ValueWithVersion
	transactions   []*TransactionStruct
	tLock          sync.RWMutex // for transactions array
	checkpointLock sync.Mutex
	path           string
	version        uint64
	logFile        *os.File
}

func (kv *SimpleKV) getTransaction(transactionId int) *TransactionStruct {
	if transactionId < 0 || transactionId >= TRANSACTION_COUNT {
		return nil
	}
	kv.tLock.RLock()
	defer kv.tLock.RUnlock()
	return kv.transactions[transactionId]
}

func (kv *SimpleKV) Get(key string, transactionId int) (string, error) {
	common.SugaredLog().Debugf("SIMPLKV GET %s %d", key, transactionId)
	// get does not require logging
	// lookup layer by layer
	t := kv.getTransaction(transactionId)
	if t == nil {
		return "", EINVTRANS
	}
	t.Lock.RLock()
	if v, ok := t.Layer[key]; ok {
		t.Lock.RUnlock()
		if v.Value == nil {
			return "", ENOENT
		} else {
			return *v.Value, nil
		}
	}
	t.Lock.RUnlock()

	if transactionId != 0 {
		// go through transaction zero too
		t0 := kv.getTransaction(0)
		t0.Lock.RLock()
		if v, ok := t.Layer[key]; ok {
			t.Lock.RUnlock()
			if v.Value == nil {
				return "", ENOENT
			} else {
				return *v.Value, nil
			}
		}
		t.Lock.RUnlock()
	}
	v, ok := kv.base[key]
	if ok && v.Value != nil {
		return *v.Value, nil
	} else {
		return "", ENOENT
	}
}

func (kv *SimpleKV) Put(key string, value string, transactionId int) (uint64, error) {
	common.SugaredLog().Debugf("KV PUT %s %s %d", key, value, transactionId)
	t := kv.getTransaction(transactionId)
	if t == nil {
		return 0, EINVTRANS
	}
	t.Lock.Lock()
	defer t.Lock.Unlock()
	if transactionId == 0 {
		kv.version += 1
		kv.writeLog("put", key, value, "0", strconv.FormatUint(kv.version, 16))
		t.Layer[key] = ValueWithVersion{Value: &value, Version: kv.version}
		return kv.version, nil
	} else {
		kv.writeLog("put", key, value, strconv.FormatInt(int64(transactionId), 16))
		t.Layer[key] = ValueWithVersion{Value: &value, Version: 0}
		return 0, nil
	}
}

// Make sure that key is removed from KVStore, regardless of whether it exists beforehand or not.
// You should check if the key exists in the KV beforehand, otherwise this API could thrash the KV.
func (kv *SimpleKV) Delete(key string, transactionId int) (uint64, error) {
	common.SugaredLog().Debugf("KV DELETE %s %d", key, transactionId)
	t := kv.getTransaction(transactionId)
	if t == nil {
		return 0, EINVTRANS
	}
	t.Lock.Lock()
	defer t.Lock.Unlock()
	if transactionId == 0 {
		kv.version += 1
		kv.writeLog("del", key, "0", strconv.FormatUint(kv.version, 16))
		t.Layer[key] = ValueWithVersion{Value: nil, Version: kv.version}
		return kv.version, nil
	} else {
		kv.writeLog("del", key, strconv.FormatInt(int64(transactionId), 16))
		t.Layer[key] = ValueWithVersion{Value: nil, Version: 0}
		return 0, nil
	}
}

func (kv *SimpleKV) StartTransaction() (transactionId int, err error) {
	// find a valid transaction id
	kv.tLock.Lock()
	defer kv.tLock.Unlock()
	for i, u := range kv.transactions {
		if u == nil {
			common.SugaredLog().Debugf("KV START %d", i)
			kv.writeLog("start", strconv.FormatInt(int64(i), 16))
			kv.transactions[i] = &TransactionStruct{
				Lock:  sync.RWMutex{},
				Layer: make(map[string]ValueWithVersion),
			}
			return i, nil
		}
	}
	return 0, ENOTRANS
}

func (kv *SimpleKV) Rollback(transactionId int) error {
	if transactionId < 1 || transactionId >= TRANSACTION_COUNT {
		return EINVTRANS
	}
	kv.tLock.Lock()
	defer kv.tLock.Unlock()
	if kv.transactions[transactionId] != nil {
		common.SugaredLog().Debugf("KV ROLLBACK %d", transactionId)
		kv.writeLog("rollback", strconv.FormatInt(int64(transactionId), 16))
		kv.transactions[transactionId] = nil
		return nil
	} else {
		return EINVTRANS
	}
}

// transaction zero can also be committed, but committing zero does not perform any operation
func (kv *SimpleKV) Commit(transactionId int) error {
	common.SugaredLog().Debugf("KV COMMIT %d", transactionId)
	if transactionId < 0 || transactionId >= TRANSACTION_COUNT {
		return EINVTRANS
	}
	if transactionId == 0 {
		return nil
	}
	kv.tLock.RLock()
	t := kv.transactions[transactionId]
	kv.tLock.RUnlock()
	if t != nil {
		// merge it into transaction zero
		kv.transactions[0].Lock.Lock()
		kv.version += 1
		kv.writeLog("commit", strconv.FormatInt(int64(transactionId), 16), strconv.FormatUint(kv.version, 16))
		t.Lock.RLock()
		for k, v := range t.Layer {
			kv.transactions[0].Layer[k] = ValueWithVersion{Value: v.Value, Version: kv.version}
		}
		t.Lock.RUnlock()
		kv.transactions[0].Lock.Unlock()
		// remove this transaction
		kv.tLock.Lock()
		kv.transactions[transactionId] = nil
		kv.tLock.Unlock()
		return nil
	} else {
		return EINVTRANS
	}
}

// Clear log entries, flush current kv in memory to slots.
// Call this when log file is getting too large.
func (kv *SimpleKV) Checkpoint() error {
	// cannot checkpoint when there is on-going transactions
	kv.tLock.RLock()
	for i, b := range kv.transactions {
		if i != 0 && b != nil {
			kv.tLock.RUnlock()
			return errors.New("have transactions going on")
		}
	}
	kv.tLock.RUnlock()

	// only one checkpoint operation can happen at a time
	kv.checkpointLock.Lock()
	common.SugaredLog().Debugf("KV CKPT")
	defer kv.checkpointLock.Unlock()
	// calculate new base
	b := make(map[string]ValueWithVersion)
	for k, v := range kv.base {
		b[k] = v
	}
	// and add the newest layer to it
	kv.tLock.RLock()
	t := kv.transactions[0]
	kv.tLock.RUnlock()
	t.Lock.Lock()
	defer t.Lock.Unlock()
	for k, v := range t.Layer {
		b[k] = v
	}
	// write new slot file
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
	// rename temporary slot file to actual slot file
	slotFileName := path.Join(kv.path, SLOT_FILENAME)
	if err := os.Rename(tmpSlotFile.Name(), slotFileName); err != nil {
		return err
	}
	// truncate log
	tmpLogFile, err := ioutil.TempFile(kv.path, LOG_TMP_FILENAME_PATTERN)
	if err != nil {
		return err
	}
	if err := os.Rename(tmpLogFile.Name(), kv.logFile.Name()); err != nil {
		return err
	}
	// update base and transactions
	kv.base = b
	kv.transactions[0].Layer = make(map[string]ValueWithVersion)
	kv.logFile = tmpLogFile
	return nil
}

// write to log(only to OS buffer)
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
func (kv *SimpleKV) Flush() {
	common.SugaredLog().Debugf("KV FLUSHING")
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
	base := make(map[string]ValueWithVersion)
	latest := make(map[string]ValueWithVersion)

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
		latest, version, err = ReadLog(scanner)
		if latest == nil {
			latest = make(map[string]ValueWithVersion)
		}
		if err != nil {
			return nil, err
		}
		log.Info("Recovered base from log entries.")
	}
	// transaction zero is always used and valid
	ts := make([]*TransactionStruct, TRANSACTION_COUNT)
	ts[0] = &TransactionStruct{
		Lock:  sync.RWMutex{},
		Layer: latest,
	}
	// others are nil
	return &SimpleKV{
		base:         base,
		transactions: ts,
		path:         pathString,
		version:      version,
		logFile:      logFile,
	}, nil
}

func readString(quoted string) string {
	ret, err := strconv.Unquote(quoted)
	if err != nil {
		panic(err)
	}
	return ret
}

func readNum(quoted string, base int) uint64 {
	s := readString(quoted)
	ret, err := strconv.ParseUint(s, base, 64)
	if err != nil {
		panic(err)
	}
	return ret
}

func ReadLog(scanner *bufio.Scanner) (map[string]ValueWithVersion, uint64, error) {
	log := common.Log()
	trans := make([]map[string]ValueWithVersion, TRANSACTION_COUNT)
	trans[0] = make(map[string]ValueWithVersion)
	var tokens []string
	var version uint64
	defer func() {
		if x := recover(); x != nil {
			log.Error("Failed to parse log.", zap.Any("error", x), zap.Strings("tokens", tokens))
		}
	}()
	for scanner.Scan() {
		tokens = strings.Fields(scanner.Text())
		if len(tokens) == 0 {
			continue
		}
		switch tokens[0] {
		case "put":
			if len(tokens) < 4 {
				panic(0)
			}
			key := readString(tokens[1])
			value := readString(tokens[2])
			transNum := int(readNum(tokens[3], 16))
			if trans[transNum] == nil {
				panic(0)
			}
			if transNum == 0 {
				// get version number
				if len(tokens) < 5 {
					panic(0)
				}
				version = readNum(tokens[4], 16)
			}
			trans[transNum][key] = ValueWithVersion{Value: &value, Version: version}
		case "del":
			if len(tokens) < 3 {
				panic(0)
			}
			key := readString(tokens[1])
			transNum := int(readNum(tokens[2], 16))
			if trans[transNum] == nil {
				panic(0)
			}
			if transNum == 0 {
				if len(tokens) < 4 {
					panic(0)
				}
				version = readNum(tokens[3], 16)
			}
			trans[transNum][key] = ValueWithVersion{Value: nil, Version: version}
		case "start":
			// start transaction
			if len(tokens) < 2 {
				panic(0)
			}
			transNum := int(readNum(tokens[1], 16))
			if trans[transNum] != nil {
				panic(0)
			}
			trans[transNum] = make(map[string]ValueWithVersion)
		case "commit":
			// commit transaction
			if len(tokens) != 3 {
				panic(0)
			}
			transNum := int(readNum(tokens[1], 16))
			if trans[transNum] == nil {
				panic(0)
			}
			// merge into trans[0]
			for k, v := range trans[transNum] {
				trans[0][k] = v
			}
			trans[transNum] = nil
			version = readNum(tokens[2], 16)
		case "rollback":
			if len(tokens) != 2 {
				panic(0)
			}
			transNum := int(readNum(tokens[1], 16))
			if trans[transNum] == nil {
				panic(0)
			}
			trans[transNum] = nil
		default:
			panic(nil)
		}
	}
	if trans[0] == nil {
		log.Warn("Nil trans[0] detected")
		trans[0] = make(map[string]ValueWithVersion)
	}
	return trans[0], version, nil
}

func (kv *SimpleKV) Extract(divider func(key string) bool, version uint64) map[string]ValueWithVersion {
	// extract content out
	b := make(map[string]ValueWithVersion)
	for k, v := range kv.base {
		if divider(k) && v.Version >= version {
			b[k] = v
		}
	}
	kv.tLock.RLock()
	t := kv.transactions[0]
	kv.tLock.RUnlock()

	t.Lock.RLock()
	defer t.Lock.RUnlock()
	for k, v := range t.Layer {
		if divider(k) && v.Version >= version {
			b[k] = v
		}
	}
	return b
}

func (kv *SimpleKV) Close() {
	log := common.Log()
	kv.Flush()
	if err := kv.logFile.Close(); err != nil {
		log.Panic("Failed to close.", zap.Error(err))
	}
}
