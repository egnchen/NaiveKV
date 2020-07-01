package worker_test

import (
	"encoding/json"
	"github.com/eyeKill/KV/worker"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
)

const pathString = "/tmp/kvstore_test"

func setUp() {
	if err := os.Mkdir(pathString, 0755); err != nil && os.IsNotExist(err) {
		panic(err)
	}
}

func tearDown() {
	if err := os.RemoveAll(pathString); err != nil {
		panic(err)
	}
}

// new kvstore without slot or log
func TestNewKVStore(t *testing.T) {
	// without anything
	setUp()
	defer tearDown()
	kv, err := worker.NewKVStore(pathString)
	// both files should be created
	_, err = os.Stat(path.Join(pathString, "slots.json"))
	assert.Nil(t, err)
	_, err = os.Stat(path.Join(pathString, "log.txt"))
	assert.Nil(t, err)

	assert.Nil(t, err)
	_, err = kv.Get("a", 0)
	assert.Equal(t, worker.ENOENT, err)
}

func TestNewKVStore2(t *testing.T) {
	setUp()
	defer tearDown()
	values := map[string]worker.ValueWithVersion{
		"a": worker.NewValueWithVersion("b", 1),
		"c": worker.NewValueWithVersion("d", 2),
	}
	logs := `put "c" "e" "0" "1"`
	b, _ := json.Marshal(values)
	_ = ioutil.WriteFile(path.Join(pathString, "slots.json"), b, 0644)
	_ = ioutil.WriteFile(path.Join(pathString, "log.txt"), []byte(logs), 0644)
	// set up complete
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	v, err := kv.Get("a", 0)
	assert.Nil(t, err)
	assert.Equal(t, "b", v)
	v, err = kv.Get("c", 0)
	assert.Nil(t, err)
	assert.Equal(t, "e", v)
}

func TestCorrectness(t *testing.T) {
	setUp()
	defer tearDown()
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	_, err = kv.Put("a", "b", 0)
	assert.Nil(t, err)
	_, err = kv.Put("c", "d", 0)
	assert.Nil(t, err)
	v, _ := kv.Get("a", 0)
	assert.Equal(t, "b", v)
	v, _ = kv.Get("c", 0)
	assert.Equal(t, "d", v)
	_, err = kv.Get("f", 0)
	assert.Equal(t, worker.ENOENT, err)
	_, err = kv.Delete("a", 0)
	assert.Nil(t, err)
	_, err = kv.Get("a", 0)
	assert.Equal(t, worker.ENOENT, err)
	kv.Close()
	// shutdown, restart worker
	kv2, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	_, err = kv2.Get("a", 0)
	assert.Equal(t, worker.ENOENT, err)
	v, err = kv2.Get("c", 0)
	assert.Nil(t, err)
	assert.Equal(t, "d", v)
}

func TestKVStore_Checkpoint(t *testing.T) {
	setUp()
	defer tearDown()
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	_, err = kv.Put("a", "b", 0)
	assert.Nil(t, err)
	_, err = kv.Put("c", "d", 0)
	assert.Nil(t, err)
	err = kv.Checkpoint()
	assert.Nil(t, err)
	kv.Close()

	// check that log.txt is empty
	info, err := os.Stat(path.Join(pathString, "log.txt"))
	if err != nil {
		panic(err)
	}
	assert.Equal(t, 0, int(info.Size()))
	// check correctness
	kv2, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	v, _ := kv2.Get("a", 0)
	assert.Equal(t, "b", v)
	v, _ = kv2.Get("c", 0)
	assert.Equal(t, "d", v)
}

// test put, delete and transactional API
func TestSimpleKV_ReadLog(t *testing.T) {
	setUp()
	defer tearDown()
	logs := `put "a" "b" "0" "0"
put "c" "e" "0" "1"
del "c" "0" "2"
start "1"
put "f" "g" "1"
start "2"
put "a" "what?" "2"
put "h" "k" "1"
commit "1" "3"
rollback "2"
del "h" "0" "4"
start "1"
put "g" "h" "1"`
	if err := ioutil.WriteFile(path.Join(pathString, "log.txt"), []byte(logs), 0644); err != nil {
		panic(err)
	}
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	v, err := kv.Get("a", 0)
	assert.Nil(t, err)
	assert.Equal(t, "b", v)
	_, err = kv.Get("c", 0)
	assert.Equal(t, worker.ENOENT, err)
	v, _ = kv.Get("f", 0)
	assert.Equal(t, "g", v)
	_, err = kv.Get("h", 0)
	assert.Equal(t, worker.ENOENT, err)
	_, err = kv.Get("g", 0)
	assert.Equal(t, worker.ENOENT, err)
}

func TestConcurrentCheckpoint(t *testing.T) {
	setUp()
	defer tearDown()
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	for i := 0; i < 65536; i++ {
		if i == 32767 {
			go func() {
				err := kv.Checkpoint()
				assert.Nil(t, err)
			}()
		}
		value := crc32.ChecksumIEEE([]byte{byte(i & 255), byte((i >> 8) & 255)})
		_, err := kv.Put(strconv.Itoa(i), strconv.Itoa(int(value)), 0)
		assert.Nil(t, err)
	}
	kv.Close()
	kv2, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	for i := 0; i < 65536; i++ {
		value := crc32.ChecksumIEEE([]byte{byte(i & 255), byte((i >> 8) & 255)})
		v, err := kv2.Get(strconv.Itoa(i), 0)
		assert.Nil(t, err)
		assert.Equal(t, strconv.Itoa(int(value)), v)
	}
}

func TestSimpleKV_TransactionCorrectness(t *testing.T) {
	setUp()
	defer tearDown()
	// commit
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	tid, err := kv.StartTransaction()
	assert.Nil(t, err)
	assert.Greater(t, tid, 0)
	_, err = kv.Put("a", "b", tid)
	assert.Nil(t, err)
	_, err = kv.Put("c", "d", tid)
	assert.Nil(t, err)
	err = kv.Commit(tid)
	assert.Nil(t, err)
	v, err := kv.Get("a", 0)
	assert.Nil(t, err)
	assert.Equal(t, "b", v)
	v, err = kv.Get("c", 0)
	assert.Nil(t, err)
	assert.Equal(t, "d", v)
	// rollback
	tid, err = kv.StartTransaction()
	assert.Nil(t, err)
	assert.Greater(t, tid, 0)
	_, err = kv.Put("a", "e", tid)
	assert.Nil(t, err)
	_, err = kv.Put("c", "f", tid)
	assert.Nil(t, err)
	// read in transaction
	v, err = kv.Get("a", tid)
	assert.Nil(t, err)
	assert.Equal(t, "e", v)
	err = kv.Rollback(tid)
	assert.Nil(t, err)
	v, err = kv.Get("a", 0)
	assert.Nil(t, err)
	assert.Equal(t, "b", v)
	v, err = kv.Get("c", 0)
	assert.Nil(t, err)
	assert.Equal(t, "d", v)
	// concurrent transactions
	tid1, err := kv.StartTransaction()
	assert.Nil(t, err)
	assert.Greater(t, tid1, 0)
	tid2, err := kv.StartTransaction()
	assert.Nil(t, err)
	assert.Greater(t, tid2, 0)

	_, err = kv.Put("a", "g", tid1)
	assert.Nil(t, err)
	_, err = kv.Put("a", "f", tid2)
	assert.Nil(t, err)
	v, err = kv.Get("a", tid1)
	assert.Nil(t, err)
	assert.Equal(t, "g", v)
	v, err = kv.Get("a", tid2)
	assert.Nil(t, err)
	assert.Equal(t, "f", v)
	err = kv.Commit(tid1)
	assert.Nil(t, err)
	v, err = kv.Get("a", 0)
	assert.Nil(t, err)
	assert.Equal(t, "g", v)
	err = kv.Rollback(tid2)
	v, err = kv.Get("a", 0)
	assert.Nil(t, err)
	assert.Equal(t, "g", v)
}

// test concurrent extract
func TestSimpleKV_Extract(t *testing.T) {
	setUp()
	defer tearDown()
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	for i := 0; i < 65536; i++ {
		v := strconv.Itoa(i)
		if i == 32768 {
			// we cannot make sure that goroutine is scheduled immediately
			go func() {
				ret := kv.Extract(func(key string) bool {
					i, _ := strconv.Atoi(key)
					return i%2 == 1
				}, 0)
				cnt := 16384
				for k, _ := range ret {
					num, _ := strconv.Atoi(k)
					assert.True(t, num%2 == 1)
					cnt--
				}
				assert.LessOrEqual(t, cnt, 0)
			}()
		}
		kv.Put(v, v, 0)
	}
}

func BenchmarkSequentialPut(b *testing.B) {
	setUp()
	defer tearDown()
	kv, err := worker.NewKVStore(pathString)
	if err != nil {
		b.FailNow()
	}
	b.SetParallelism(1)
	b.Cleanup(tearDown)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := strconv.Itoa(i)
		_, _ = kv.Put(v, v, 0)
	}
}

func BenchmarkConcurrentPut(b *testing.B) {
	setUp()
	defer tearDown()
	kv, err := worker.NewKVStore(pathString)
	if err != nil {
		b.FailNow()
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			v := strconv.Itoa(i)
			_, _ = kv.Put(v, v, 0)
			i++
		}
	})
}
