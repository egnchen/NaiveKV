// decl: common data structures & constants
package common

const (
	ZK_ROOT            = "/kv"
	ZK_NODES_ROOT      = "/kv/nodes"
	ZK_WORKERS_ROOT    = "/kv/workers"
	ZK_MIGRATIONS_ROOT = "/kv/migrations"
	ZK_WORKER_ID       = "/kv/next_worker_id"
	ZK_MASTER_NAME     = "master"
	ZK_WORKER_NAME     = "worker"
)

type Node struct {
	Hostname string
	Port     uint16
}

// KV store worker id. This id is mainly for slot allocations.
// Zero is reserved as invalid.
type WorkerId uint16

type SlotId uint16

// Metadata of a worker.
type Worker struct {
	Id     WorkerId
	Host   Node
	Weight uint
	Status string
}

// Metadata of a master.
type Master struct {
	Host Node
}

func GetNewMasterNode(hostname string, port uint16) Master {
	return Master{
		Host: Node{
			Hostname: hostname,
			Port:     port,
		},
	}
}

func GetNewWorkerNode(hostname string, port uint16, id WorkerId, weight uint) Worker {
	return Worker{
		Id: id,
		Host: Node{
			Hostname: hostname,
			Port:     port,
		},
		Weight: weight,
		Status: "",
	}
}
