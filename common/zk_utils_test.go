package common_test

import (
	"github.com/eyeKill/KV/common"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"log"
	"strconv"
	"strings"
	"testing"
)

var ZK_SERVERS = []string{"localhost:2181"}

func setUp() *zk.Conn {
	conn, err := common.ConnectToZk(ZK_SERVERS)
	if err != nil {
		panic(err)
	}
	// ensure /test exists
	if err := common.EnsurePathRecursive(conn, "/test"); err != nil {
		panic(err)
	}
	return conn
}

// remove /test
func tearDown(conn *zk.Conn) {
	if err := common.ZkDeleteRecursive(conn, "/test"); err != nil {
		log.Panic(err)
	}
}

func TestConnectToZk(t *testing.T) {
	_, err := common.ConnectToZk(ZK_SERVERS)
	assert.Nil(t, err)
}

func TestEnsurePath(t *testing.T) {
	conn := setUp()
	defer tearDown(conn)
	err := common.EnsurePath(conn, "/test/test1")
	assert.Nil(t, err)
	err = common.EnsurePath(conn, "/test/test1/test2/test3/test4")
	assert.NotNil(t, err)
	err = common.EnsurePathRecursive(conn, "/test/test1/test2/test3/test4")
	assert.Nil(t, err)
}

func TestZkCreate(t *testing.T) {
	conn := setUp()
	defer tearDown(conn)
	dat := map[string]string{"a": "b", "c": "d"}
	_, err := common.ZkCreate(conn, "/test/test_dat", dat, false, false)
	assert.Nil(t, err)
	var ret map[string]string
	err = common.ZkGet(conn, "/test/test_dat", &ret)
	assert.Nil(t, err)
	assert.Equal(t, dat, ret)
	name, err := common.ZkCreate(conn, "/test/test_es_", "asdf", true, true)
	assert.Nil(t, err)
	assert.NotEqual(t, "/test/test_es_", name)
	n := strings.Split(name, "_")
	_, err = strconv.Atoi(n[len(n)-1])
	assert.Nil(t, err)
}

func TestZkDeleteRecursive(t *testing.T) {
	conn := setUp()
	defer tearDown(conn)
	err := common.EnsurePathRecursive(conn, "/test/a/b/c/d/e")
	assert.Nil(t, err)
	_, err = common.ZkCreate(conn, "/test/a/b/c/d/e/f", "hahaha", false, false)
	assert.Nil(t, err)
	err = common.ZkDeleteRecursive(conn, "/test/a")
	assert.Nil(t, err)
	exist, _, err := conn.Exists("/test/a")
	assert.Nil(t, err)
	assert.False(t, exist)
}

func TestMultiToZk(t *testing.T) {
	conn := setUp()
	defer tearDown(conn)
	_, err := common.ZkMulti(conn, &zk.CreateRequest{
		Path:  "/test/a",
		Data:  []byte("hahaha"),
		Acl:   zk.WorldACL(zk.PermAll),
		Flags: 0,
	}, &zk.SetDataRequest{
		Path:    "/test/c",
		Data:    []byte("heiheihei"),
		Version: -1,
	})
	assert.NotNil(t, err)
	_, err = common.ZkMulti(conn, &zk.CreateRequest{
		Path:  "/test/a",
		Data:  []byte("hahaha"),
		Acl:   zk.WorldACL(zk.PermAll),
		Flags: 0,
	}, &zk.SetDataRequest{
		Path:    "/test/a",
		Data:    []byte("heiheihei"),
		Version: -1,
	})
	assert.Nil(t, err)
}

func TestDistributedAtomicInteger_Watch(t *testing.T) {
	conn := setUp()
	defer tearDown(conn)
	dai := common.DistributedAtomicInteger{
		Conn: conn,
		Path: "/test/dai",
	}
	err := dai.SetDefault(0)
	assert.Nil(t, err)
	watchCnt := 0
	go func() {
		v := &watchCnt
		err := dai.Watch(func(newValue int) bool {
			*v += 1
			return newValue != 3
		})
		assert.Nil(t, err)
	}()
	for i := 0; i < 10; i++ {
		_, err := dai.Inc()
		assert.Nil(t, err)
	}
	assert.Equal(t, 4, watchCnt)
}

func TestDistributedAtomicInteger(t *testing.T) {
	conn := setUp()
	defer tearDown(conn)
	i := common.DistributedAtomicInteger{
		Conn: conn,
		Path: "/test/dai",
	}
	err := i.SetDefault(10)
	assert.Nil(t, err)
	v, err := i.Get()
	assert.Nil(t, err)
	assert.Equal(t, 10, v)
	vi, err := i.Inc()
	assert.Nil(t, err)
	assert.Equal(t, 10, vi)
	vi, err = i.Get()
	assert.Nil(t, err)
	assert.Equal(t, 11, vi)
	vi, err = i.Dec()
	assert.Nil(t, err)
	assert.Equal(t, 11, vi)
	vi, err = i.Get()
	assert.Nil(t, err)
	assert.Equal(t, 10, vi)
	err = i.Set(100)
	assert.Nil(t, err)
	vi, err = i.Get()
	assert.Nil(t, err)
	assert.Equal(t, 100, vi)
}
