// common data structure for primary & backup workers
package worker

import (
	"github.com/eyeKill/KV/common"
	"go.uber.org/atomic"
)

type WorkerConfig struct {
	Id      common.WorkerId
	Version uint
	Weight  uint
}

type BackupWorkerId int32

const KEY_BACKUP_WORKER_ID = "backupWorkerId"

var nextBackupWorkerId atomic.Int32

func GetNextBackupWorkerId() BackupWorkerId {
	return BackupWorkerId(nextBackupWorkerId.Inc())
}
