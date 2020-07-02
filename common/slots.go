// slot definitions
package common

import (
	"hash/crc32"
)

const DEFAULT_SLOT_COUNT = 1024

type HashSlotRing []WorkerId

func NewHashSlotRing() HashSlotRing {
	return make([]WorkerId, DEFAULT_SLOT_COUNT)
}

func (ring HashSlotRing) GetWorkerId(key string) WorkerId {
	return ring[ring.GetSlotId(key)]
}

func (ring HashSlotRing) GetWorkerIdDirect(id SlotId) WorkerId {
	return ring[id]
}

func GetSlotId(key string, slotCount int) SlotId {
	h := crc32.ChecksumIEEE([]byte(key))
	return SlotId(h % uint32(slotCount))
}

func (ring HashSlotRing) GetSlotId(key string) SlotId {
	return GetSlotId(key, len(ring))
}

type MigrationTable map[SlotId]WorkerId
type Migration struct {
	Version uint32
	Table   MigrationTable
}

// An allocator handles slot migration. Given a set of new workers,
// it should be able to yield a valid migration table.
type HashSlotAllocator interface {
	AllocateSlots(ring *HashSlotRing, oldWorkers map[WorkerId]*Worker, newWorker *Worker) (MigrationTable, error)
}

type SingleNodeMigration struct {
	Version uint32
	Id      WorkerId
	Table   MigrationTable
	Len     int
}

func (r SingleNodeMigration) GetDestId(key string) WorkerId {
	slot := GetSlotId(key, r.Len)
	ret, ok := r.Table[slot]
	if ok {
		return ret
	} else {
		return r.Id
	}
}

func NewSingleNodeMigration(id WorkerId, original *HashSlotRing, migration *Migration) *SingleNodeMigration {
	table := make(MigrationTable)
	stat := make(map[WorkerId]int)
	for slotId, dstId := range migration.Table {
		srcId := original.GetWorkerIdDirect(slotId)
		if srcId == id {
			table[slotId] = dstId
			stat[dstId] += 1
		}
	}
	SugaredLog().Infof("Separated stat for id %d: %+v", id, stat)
	return &SingleNodeMigration{
		Version: migration.Version,
		Id:      id,
		Table:   table,
		Len:     len(*original),
	}
}

// separate an overall migration plan to small plans specific to different nodes
func (m *Migration) Separate(original *HashSlotRing) map[WorkerId]*SingleNodeMigration {
	// stat how many workers in the original hash ring
	ret := make(map[WorkerId]*SingleNodeMigration)
	for slotId, _ := range m.Table {
		srcId := original.GetWorkerIdDirect(slotId)
		if srcId == 0 {
			continue
		}
		ret[srcId] = nil
	}
	for i := range ret {
		ret[i] = NewSingleNodeMigration(i, original, m)
	}
	return ret
}
