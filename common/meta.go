// decl: common Data structures & constants
package common

import (
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"path"
	"strconv"
	"strings"
)

const (
	ZK_ROOT                = "/kv"
	ZK_MASTERS_ROOT        = "/kv/masters"
	ZK_WORKERS_ROOT        = "/kv/workers"
	ZK_MIGRATIONS_ROOT     = "/kv/migrations"
	ZK_ELECTION_ROOT       = "/kv/election"
	ZK_TABLE               = "/kv/table"
	ZK_TABLE_VERSION       = "/kv/version"
	ZK_WORKER_ID           = "/kv/workerId"
	ZK_MASTER_NAME         = "master"
	ZK_PRIMARY_WORKER_NAME = "primary"
	ZK_BACKUP_WORKER_NAME  = "backup"
	ZK_WORKER_CONFIG_NAME  = "config"
	ZK_COMPLETE_SEM_NAME   = "completeSem"
)

type Node struct {
	Hostname string
	Port     uint16
}

// KV store worker id. This id is mainly for slot allocations.
type WorkerId int

type MasterNode struct {
	Host Node
}

type WorkerNode struct {
	Id     WorkerId
	Host   Node
	Status string
}

func NewMasterNode(hostname string, port uint16) MasterNode {
	return MasterNode{
		Host: Node{
			Hostname: hostname,
			Port:     port,
		},
	}
}

func NewWorkerNode(hostname string, port uint16, id WorkerId) WorkerNode {
	return WorkerNode{
		Id: id,
		Host: Node{
			Hostname: hostname,
			Port:     port,
		},
		Status: "",
	}
}

// Worker metadata
// A worker represents a set of worker nodes, including one worker node and several backup nodes.
// A worker is identified by its worker id.
type Worker struct {
	Id      WorkerId
	Weight  float32
	Watcher <-chan zk.Event
	// here worker & backup nodes are represented by corresponding znode names.
	Primaries  map[string]*WorkerNode
	Backups    map[string]*WorkerNode
	NumBackups int
}

type WorkerConfig struct {
	Weight     float32
	NumBackups int
}

// get a worker instance from zookeeper
func GetAndWatchWorker(conn *zk.Conn, id WorkerId) (Worker, error) {
	p := path.Join(ZK_WORKERS_ROOT, strconv.Itoa(int(id)))
	children, _, eventChan, err := conn.ChildrenW(p)
	if err != nil {
		return Worker{}, err
	}
	worker := Worker{
		Id:         id,
		Watcher:    eventChan,
		Primaries:  make(map[string]*WorkerNode),
		Backups:	make(map[string]*WorkerNode),
	}
	for _, c := range children {
		if strings.Contains(c, ZK_PRIMARY_WORKER_NAME) || strings.Contains(c, ZK_BACKUP_WORKER_NAME) {
			// worker node
			var node WorkerNode
			if err := ZkGet(conn, path.Join(p, c), &node); err != nil {
				return Worker{}, err
			}
			if strings.Contains(c, ZK_PRIMARY_WORKER_NAME) {
				worker.Primaries[c] = &node
			} else {
				worker.Backups[c] = &node
			}
		} else if c == ZK_WORKER_CONFIG_NAME {
			var config WorkerConfig
			if err := ZkGet(conn, path.Join(p, c), &config); err != nil {
				return Worker{}, err
			}
			worker.Weight = config.Weight
			worker.NumBackups = config.NumBackups
		} else {
			Log().Warn("Cannot determine node, skipping", zap.String("name", c))
		}
	}
	return worker, err
}
