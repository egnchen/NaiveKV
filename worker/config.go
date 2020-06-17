package worker

import "github.com/eyeKill/KV/common"

type WorkerConfig struct {
	Id      common.WorkerId
	Version uint
}
