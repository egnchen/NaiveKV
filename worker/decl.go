// common data structure for primary & backup workers
package worker

import (
	"github.com/eyeKill/KV/common"
)

type KVPair struct {
	key   string
	value string
}

type WorkerConfig struct {
	Id      common.WorkerId
	Version uint
	Weight  uint
}
