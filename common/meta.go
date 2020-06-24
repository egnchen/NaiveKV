// decl: common Data structures & constants
package common

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	ZK_ROOT            = "/kv"
	ZK_NODES_ROOT      = "/kv/nodes"
	ZK_WORKERS_ROOT    = "/kv/workers"
	ZK_MIGRATIONS_ROOT = "/kv/migrations"
	ZK_WORKER_ID       = "/kv/next_worker_id"
	ZK_MASTER_NAME     = "master"
	ZK_PRIMARY_NAME    = "primary"
	ZK_BACKUP_NAME     = "backup"
)

type Node struct {
	Hostname string
	Port     uint16
}

// KV store primary id. This id is mainly for slot allocations.
// Zero is reserved as unallocated
type WorkerId uint16

type SlotId uint16

const (
	TYPE_MASTER  = "Master"
	TYPE_PRIMARY = "Primary"
	TYPE_BACKUP  = "Backup"
)

// Metadata of a primary node
type PrimaryWorkerNode struct {
	Id     WorkerId
	Host   Node
	Weight float32
	Status string
}

type BackupWorkerNode struct {
	Id     WorkerId
	Host   Node
	Status string
}

// Metadata of a master node
type MasterNode struct {
	Host Node
}

// Marshal for these types
func (n *MasterNode) MarshalJSON() ([]byte, error) {
	type Alias MasterNode
	return json.Marshal(struct {
		Type string
		Data *Alias
	}{
		Type: TYPE_MASTER,
		Data: (*Alias)(n),
	})
}

func (n *PrimaryWorkerNode) MarshalJSON() ([]byte, error) {
	type Alias PrimaryWorkerNode
	return json.Marshal(struct {
		Type string
		Data *Alias
	}{
		Type: TYPE_PRIMARY,
		Data: (*Alias)(n),
	})
}

func (n *BackupWorkerNode) MarshalJSON() ([]byte, error) {
	type Alias BackupWorkerNode
	return json.Marshal(struct {
		Type string
		Data *Alias
	}{
		Type: TYPE_BACKUP,
		Data: (*Alias)(n),
	})
}

// Unmarshal a typed node metadata to the node struct of corresponding type.
// Returned value is one of *MasterNode, *PrimaryWorkerNode and *BackupWorkerNode.
func UnmarshalNode(data []byte) (interface{}, error) {
	type NodeMeta struct {
		Type string
		Data json.RawMessage
	}
	var meta NodeMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	switch meta.Type {
	case "Master":
		var ret MasterNode
		if err := json.Unmarshal(meta.Data, &ret); err != nil {
			return nil, err
		}
		return &ret, nil
	case "Primary":
		var ret PrimaryWorkerNode
		if err := json.Unmarshal(meta.Data, &ret); err != nil {
			return nil, err
		}
		return &ret, nil
	case "Backup":
		var ret BackupWorkerNode
		if err := json.Unmarshal(meta.Data, &ret); err != nil {
			return nil, err
		}
		return &ret, nil
	default:
		return nil, errors.New(fmt.Sprintf("invalid type to unmarshall: %s", meta.Type))
	}
}

func NewMasterNode(hostname string, port uint16) MasterNode {
	return MasterNode{
		Host: Node{
			Hostname: hostname,
			Port:     port,
		},
	}
}

func NewPrimaryWorkerNode(hostname string, port uint16, id WorkerId, weight float32) PrimaryWorkerNode {
	return PrimaryWorkerNode{
		Id: id,
		Host: Node{
			Hostname: hostname,
			Port:     port,
		},
		Weight: weight,
		Status: "",
	}
}

func NewBackupWorkerNode(hostname string, port uint16, id WorkerId) BackupWorkerNode {
	return BackupWorkerNode{
		Id: id,
		Host: Node{
			Hostname: hostname,
			Port:     port,
		},
		Status: "",
	}
}

// Worker metadata
// A primary represents a set of primary nodes, including one primary node and several backup nodes.
// A primary is identified by its primary id.

type NodeEntry struct {
	Name  string
	Valid bool
}

type Worker struct {
	Id     WorkerId
	Weight float32
	// here primary & backup nodes are represented by corresponding znode names.
	Primary NodeEntry
	Backups []NodeEntry
}
