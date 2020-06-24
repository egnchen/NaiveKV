package worker_test

import (
	"encoding/json"
	"github.com/eyeKill/KV/worker"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

const pathString = "/tmp/kvstore_test"

func setUp() {
	tearDown()
	if err := os.Mkdir(pathString, 0755); err != nil {
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
	kv, err := worker.NewKVStore(pathString)
	// both files should be created
	_, err = os.Stat(path.Join(pathString, "slots.json"))
	assert.Nil(t, err)
	_, err = os.Stat(path.Join(pathString, "log.txt"))
	assert.Nil(t, err)

	assert.Nil(t, err)
	_, ok := kv.Get("a")
	assert.False(t, ok)
	tearDown()
}

func TestNewKVStore2(t *testing.T) {
	setUp()
	values := map[string]string{
		"a": "b", "c": "d",
	}
	logs := `put "c" "e"`
	b, _ := json.Marshal(values)
	_ = ioutil.WriteFile(path.Join(pathString, "slots.json"), b, 0644)
	_ = ioutil.WriteFile(path.Join(pathString, "log.txt"), []byte(logs), 0644)
	// set up complete
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	v, ok := kv.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "b", v)
	v, ok = kv.Get("c")
	assert.True(t, ok)
	assert.Equal(t, "e", v)
	tearDown()
}

func TestCorrectness(t *testing.T) {
	setUp()
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	kv.Put("a", "b")
	kv.Put("c", "d")
	v, _ := kv.Get("a")
	assert.Equal(t, "b", v)
	v, _ = kv.Get("c")
	assert.Equal(t, "d", v)
	_, ok := kv.Get("f")
	assert.False(t, ok)
	kv.Delete("a")
	_, ok = kv.Get("a")
	assert.False(t, ok)
	kv.Close()
	// shutdown, restart worker
	kv2, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	_, ok = kv2.Get("a")
	assert.False(t, ok)
	v, _ = kv2.Get("c")
	assert.Equal(t, "d", v)
	tearDown()
}

func TestKVStore_Checkpoint(t *testing.T) {
	setUp()
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	kv.Put("a", "b")
	kv.Put("c", "d")
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
	v, _ := kv2.Get("a")
	assert.Equal(t, "b", v)
	v, _ = kv2.Get("c")
	assert.Equal(t, "d", v)
}

// test put, delete and transactional API
func TestSimpleKV_ReadLog(t *testing.T) {
	setUp()
	logs := `put "a" "b"
put "c" "e"
del "c"
transaction begin
put "f" "g"
put "h" "k"
transaction end
del "h"
transaction begin
put "g" "h"`
	if err := ioutil.WriteFile(path.Join(pathString, "log.txt"), []byte(logs), 0644); err != nil {
		panic(err)
	}
	kv, err := worker.NewKVStore(pathString)
	assert.Nil(t, err)
	v, _ := kv.Get("a")
	assert.Equal(t, "b", v)
	_, ok := kv.Get("c")
	assert.False(t, ok)
	v, _ = kv.Get("f")
	assert.Equal(t, "g", v)
	_, ok = kv.Get("h")
	assert.False(t, ok)
	_, ok = kv.Get("g")
	assert.False(t, ok)
}
