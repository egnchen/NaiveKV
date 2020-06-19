package worker

import (
	"github.com/eyeKill/KV/common"
	"github.com/eyeKill/KV/proto"
)

type BackupWorker struct {
	proto.UnimplementedBackupServiceServer
	Hostname string
	Port     uint16
	filePath string
	kv       *KVStore
}

func NewBackupWorker(hostname string, port uint16, filePath string) (*BackupWorker, error) {
	kv, err := NewKVStore(filePath)
	if err != nil {
		return nil, err
	}
	return &BackupWorker{
		Hostname: hostname,
		Port:     port,
		filePath: filePath,
		kv:       kv,
	}, nil
}

// connect to worker with given worker id
func (w *BackupWorker) Connect(id common.WorkerId) {

}
