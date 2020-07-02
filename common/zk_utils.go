package common

import (
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
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

func GetFromZk(conn *zk.Conn, path string) []byte {
	content, _, err := conn.Get(path)
	if err != nil {
		Log().Error("Failed to retrieve content from zookeeper.", zap.String("path", path), zap.Error(err))
		return nil
	}
	return content
}

func GetWorker(conn *zk.Conn, id WorkerId) (Worker, error) {
	p := path.Join(ZK_WORKERS_ROOT, strconv.Itoa(int(id)))
	children, _, eventChan, err := conn.ChildrenW(p)
	if err != nil {
		return Worker{}, err
	}
	var worker Worker
	worker.Id = id
	worker.Watcher = eventChan
	for _, c := range children {
		bin := GetFromZk(conn, path.Join(p, c))
		if c == "config" {
			var config WorkerConfig
			if err := json.Unmarshal(bin, &config); err != nil {
				return Worker{}, err
			}
			worker.Weight = config.Weight
		}
		var node WorkerNode
		if err := json.Unmarshal(bin, &node); err != nil {
			return Worker{}, err
		}
		if strings.Contains(c, ZK_PRIMARY_WORKER_NAME) {
			// worker node
			worker.PrimaryName = c
			worker.Primary = &node
		} else if strings.Contains(c, ZK_BACKUP_WORKER_NAME) {
			worker.Backups[c] = &node
		} else if c == "config" {

		} else {
			Log().Warn("Cannot determine node, skipping", zap.String("name", c))
		}
	}
	return worker, nil
}
