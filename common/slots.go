// slot definitions
package common

import (
	"hash/crc32"
)

const DEFAULT_SLOT_COUNT = 1024

type HashSlotRing struct {
	Version uint
	Slots   []WorkerId
}

func NewHashSlotRing(size uint16) HashSlotRing {
	return HashSlotRing{
		Version: 0,
		Slots:   make([]WorkerId, size),
	}
}

func (ring HashSlotRing) Len() int {
	return len(ring.Slots)
}

func (ring HashSlotRing) GetWorkerId(key string) WorkerId {
	return ring.Slots[ring.GetSlotId(key)]
}

func GetSlotId(key string, slotCount int) SlotId {
	h := crc32.ChecksumIEEE([]byte(key))
	return SlotId(h % uint32(slotCount))
}

func (ring HashSlotRing) GetSlotId(key string) SlotId {
	return GetSlotId(key, ring.Len())
}

type MigrationTable map[SlotId]WorkerId
type SlotMigration struct {
	Version uint
	Table   MigrationTable
}

// An allocator handles slot migration. Given a set of new workers,
// it should be able to yield a valid migration table.
type HashSlotAllocator interface {
	AllocateSlots(ring *HashSlotRing,
		oldWorkers map[WorkerId]*Worker, newWorkers map[WorkerId]*Worker) (MigrationTable, error)
}

type RingBoolVector struct {
	Vector []uint8
}

func (r RingBoolVector) GetSlotId(key string) SlotId {
	return GetSlotId(key, len(r.Vector))
}

type SingleNodeMigration struct {
	Version uint
	Id      WorkerId
	Ring    RingBoolVector
}

func NewSingleNodeMigration(id WorkerId, migration *SlotMigration) SingleNodeMigration {
	vector := make([]uint8, DEFAULT_SLOT_COUNT/8)
	for i, w := range migration.Table {
		if w == id {
			vector[i/8] |= 1 << (i % 8)
		}
	}
	return SingleNodeMigration{
		Version: migration.Version,
		Id:      id,
		Ring:    RingBoolVector{Vector: vector},
	}
}

func (v RingBoolVector) Set(id SlotId, value bool) {
	if value {
		v.Vector[id/8] |= 1 << (id % 8)
	} else {
		v.Vector[id/8] ^= ^(1 << (id % 8))
	}
}

func (v RingBoolVector) Get(id SlotId) bool {
	return v.Vector[id/8]&(1<<(id%8)) > 0
}
