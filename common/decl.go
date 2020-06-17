// decl: common data structures
package common

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

func GetNewWorkerNode(hostname string, port uint16, weight uint) Worker {
	return Worker{
		Id: 0,
		Host: Node{
			Hostname: hostname,
			Port:     port,
		},
		Weight: weight,
		Status: "",
	}
}
