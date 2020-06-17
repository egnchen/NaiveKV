// definition for hash slots
package master

import "github.com/eyeKill/KV/common"

const DEFAULT_SLOT_COUNT = 1024

type HashSlotRing struct {
	Version uint
	Slots   []common.WorkerId
}

type SlotMigration struct {
	Version        uint
	MigrationTable map[common.SlotId]common.WorkerId
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
