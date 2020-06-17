package common

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"strings"
	"time"
)

const (
	ZK_ROOT            = "/kv"
	ZK_WORKERS_ROOT    = "/kv/nodes"
	ZK_MIGRATIONS_ROOT = "/kv/migrations"
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

func EnsurePath(conn *zk.Conn, p string) error {
	// ensure path layer by layer
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
