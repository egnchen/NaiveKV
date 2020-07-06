package common

import (
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"path"
	"strconv"
	"strings"
	"time"
)

func ConnectToZk(servers []string) (*zk.Conn, error) {
	conn, _, err := zk.Connect(servers, 2000*time.Millisecond)
	if err == nil {
		conn.SetLogger(&ZkLoggerAdapter{})
	}
	return conn, err
}

func EnsurePathRecursive(conn *zk.Conn, p string) error {
	// ensure p layer by layer
	dirs := strings.Split(p, "/")
	cp := "/"
	for _, d := range dirs {
		cp = path.Join(cp, d)
		exists, _, err := conn.Exists(cp)
		if err != nil {
			return err
		}
		if !exists {
			_, err = conn.Create(cp, []byte(""), 0, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists {
				return err
			}
		}
	}
	return nil
}

func EnsurePath(conn *zk.Conn, p string) error {
	exists, _, err := conn.Exists(p)
	if err != nil {
		return err
	}
	if !exists {
		_, err = conn.Create(p, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

// This implementation uses optimistic locking. This is a wrapper of zookeeper znode.
// This struct can be copied, but it only ensures distributed atomicity. Local atomicity is not provided.
// Reference: https://curator.apache.org/apidocs/org/apache/curator/framework/recipes/atomic/DistributedAtomicInteger.html
type DistributedAtomicInteger struct {
	Conn *zk.Conn
	Path string
}

func (i DistributedAtomicInteger) getWithVersion() (value int, version int32, err error) {
	data, stat, err := i.Conn.Get(i.Path)
	if err != nil {
		return 0, 0, err
	}
	value, err = strconv.Atoi(string(data))
	if err != nil {
		return 0, 0, err
	}
	return value, stat.Version, nil
}

func (i DistributedAtomicInteger) Get() (int, error) {
	value, _, err := i.getWithVersion()
	return value, err
}

func (i DistributedAtomicInteger) Inc() (int, error) {
	for {
		value, version, err := i.getWithVersion()
		if err != nil {
			return 0, err
		}
		_, err = i.Conn.Set(i.Path, []byte(strconv.Itoa(value+1)), version)
		if err == nil {
			return value, nil
		}
		if err != nil && err != zk.ErrBadVersion {
			return 0, err
		}
		// encounter with bad version, try again
	}
}

func (i DistributedAtomicInteger) Dec() (int, error) {
	for {
		value, version, err := i.getWithVersion()
		if err != nil {
			return 0, err
		}
		_, err = i.Conn.Set(i.Path, []byte(strconv.Itoa(value-1)), version)
		if err == nil {
			return value, nil
		}
		if err != nil && err != zk.ErrBadVersion {
			return 0, err
		}
	}
}

func (i DistributedAtomicInteger) Set(v int) error {
	for {
		_, version, err := i.getWithVersion()
		_, err = i.Conn.Set(i.Path, []byte(strconv.Itoa(v)), version)
		if err == nil {
			return nil
		}
		if err != nil && err != zk.ErrBadVersion {
			return err
		}
		// encounter with bad version, try again
	}
}

func (i DistributedAtomicInteger) SetDefault(v int) error {
	exists, _, err := i.Conn.Exists(i.Path)
	if err != nil {
		return err
	}
	if !exists {
		_, err := i.Conn.Create(i.Path, []byte(strconv.Itoa(v)), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}

// Watch the integer. Trigger the watcher function once change is detected.
// The watch will continue if watcher function returns true.
func (i DistributedAtomicInteger) Watch(watcher func(newValue int) bool) error {
	for {
		bin, _, eventChan, err := i.Conn.GetW(i.Path)
		if err != nil {
			return err
		}
		value, err := strconv.Atoi(string(bin))
		if err != nil {
			return err
		}
		if !watcher(value) {
			return nil
		}
		<-eventChan
	}
}

func ZkGet(conn *zk.Conn, path string, dest interface{}) error {
	content, _, err := conn.Get(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(content, dest)
}

func ZkCreate(conn *zk.Conn, path string, content interface{}, sequential bool, ephemeral bool) (string, error) {
	bin, err := json.Marshal(content)
	if err != nil {
		return "", err
	}
	var f int32 = 0
	if sequential {
		f |= zk.FlagSequence
	}
	if ephemeral {
		f |= zk.FlagEphemeral
	}
	name, err := conn.Create(path, bin, f, zk.WorldACL(zk.PermAll))
	return name, err
}

func ZkDeleteRecursive(conn *zk.Conn, p string) error {
	children, _, err := conn.Children(p)
	if err == zk.ErrNoNode {
		return nil
	} else if err != nil {
		return err
	}
	for _, c := range children {
		if err := ZkDeleteRecursive(conn, path.Join(p, c)); err != nil {
			return err
		}
	}
	if err := conn.Delete(p, -1); err != nil {
		return err
	}
	return nil
}

func ZkMulti(conn *zk.Conn, reqs ...interface{}) ([]zk.MultiResponse, error) {
	log := SugaredLog()
	resps, err := conn.Multi(reqs...)
	if err != nil {
		log.Error("Failed to update table & version number.", zap.Error(err))
		for i, resp := range resps {
			if resp.Error != nil {
				log.Errorf("Request #%d(%+v) failed: %+v", i, reqs[i], resp.Error)
				return resps, resp.Error
			}
		}
	}
	return resps, err
}
