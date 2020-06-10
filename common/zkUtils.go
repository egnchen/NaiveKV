package common

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"log"
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
	log.Println("Connecting to zookeeper cluster...")
	conn, _, err := zk.Connect(servers, time.Second*3)
	return conn, err
}

func EnsurePath(conn *zk.Conn, path string) error {
	exists, _, err := conn.Exists(path)
	if err != nil {
		return err
	}
	if !exists {
		_, err = conn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}
