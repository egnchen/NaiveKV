// definition for hash slots
package master

import (
	"github.com/eyeKill/KV/common"
)

const DEFAULT_SLOT_COUNT = 1024

type HashSlotRing struct {
	Version uint
	Slots   []common.WorkerId
}

func NewHashSlotRing(size uint16) HashSlotRing {
	return HashSlotRing{
		Version: 0,
		Slots:   make([]common.WorkerId, size),
	}
}

func (ring HashSlotRing) Len() uint16 {
	return uint16(len(ring.Slots))
}

type MigrationTable map[common.SlotId]common.WorkerId
type SlotMigration struct {
	Version uint
	Table   MigrationTable
}

// An allocator handles slot migration. Given a set of new workers,
// it should be able to yield a valid migration table.
type HashSlotAllocator interface {
	AllocateSlots(ring *HashSlotRing,
		oldWorkers map[common.WorkerId]*common.Worker, newWorkers map[common.WorkerId]*common.Worker) (MigrationTable, error)
}
