package common

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"strconv"
	"strings"
	"time"
)

func ZkStateString(s *zk.Stat) string {
	return fmt.Sprintf("Czxid:%d, Mzxid: %d, Ctime: %d, Mtime: %d, "+
		"Version: %d, Cversion: %d, Aversion: %d, "+
		"EphemeralOwner: %d, DataLength: %d, NumChildren: %d, Pzxid: %d",
		s.Czxid, s.Mzxid, s.Ctime, s.Mtime,
		s.Version, s.Cversion, s.Aversion,
		s.EphemeralOwner, s.DataLength, s.NumChildren, s.Pzxid)
}

func ConnectToZk(servers []string) (*zk.Conn, error) {
	conn, _, err := zk.Connect(servers, time.Second*3)
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
			return value + 1, nil
		}
		if err != nil && err != zk.ErrBadVersion {
			return 0, err
		}
		// encounter with bad version, try again
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
