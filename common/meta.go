// decl: common Data structures & constants
package common

import "github.com/samuel/go-zookeeper/zk"

const (
	ZK_ROOT                = "/kv"
	ZK_WORKERS_ROOT        = "/kv/workers"
	ZK_MIGRATIONS_ROOT     = "/kv/migrations"
	ZK_ELECTION_ROOT       = "/kv/election"
	ZK_MASTER_NAME         = "master"
	ZK_MASTER_ROOT         = "/kv/masters"
	ZK_VERSION_NAME        = "version"
	ZK_PRIMARY_WORKER_NAME = "worker"
	ZK_BACKUP_WORKER_NAME  = "backup"
	ZK_WORKER_ID_NAME      = "workerId"
	ZK_TABLE_NAME          = "table"
	ZK_WORKER_CONFIG_NAME  = "config"
	ZK_COMPLETE_SEM_NAME   = "completeSem"
)

type Node struct {
	Hostname string
	Port     uint16
}

// KV store worker id. This id is mainly for slot allocations.
type WorkerId int

type SlotId uint16

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
	Primary     *WorkerNode
	PrimaryName string
	Backups     map[string]*WorkerNode
}

type WorkerConfig struct {
	Weight float32
}
