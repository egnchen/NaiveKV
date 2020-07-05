// Slot definitions
// This file contains various data structures for hash slot.

package common

import (
	"hash/crc32"
)

type SlotId uint16

const DEFAULT_SLOT_COUNT = SlotId(1024)

// An array of worker IDs, indicating the worker each slot belongs to
type HashSlotRing []WorkerId

func NewHashSlotRing() *HashSlotRing {
	ret := make(HashSlotRing, DEFAULT_SLOT_COUNT)
	return &ret
}

func (ring HashSlotRing) GetWorkerIdByKey(key string) WorkerId {
	return ring[ring.GetSlotId(key)]
}

func (ring HashSlotRing) GetWorkerIdBySlot(id SlotId) WorkerId {
	return ring[id]
}

func GetSlotId(key string, slotCount int) SlotId {
	h := crc32.ChecksumIEEE([]byte(key))
	return SlotId(h % uint32(slotCount))
}

func (ring HashSlotRing) GetSlotId(key string) SlotId {
	return GetSlotId(key, len(ring))
}

// Slot migration data structures

// A map from slot ID to destination worker ID.
type MigrationTable map[SlotId]WorkerId

// Overall migration plan, containing each to-be-migrated slots' destination/
type Migration struct {
	Version uint32
	Table   MigrationTable
}

// Migration plan specific to one worker.
type SingleNodeMigration struct {
	Version uint32
	Id      WorkerId
	Table   MigrationTable
	Len     int
}

func (r SingleNodeMigration) GetDestWorkerId(key string) WorkerId {
	slot := GetSlotId(key, r.Len)
	ret, ok := r.Table[slot]
	if ok {
		return ret
	} else {
		return r.Id // does not need to be migrated
	}
}

func (m SingleNodeMigration) GetDestinations() []WorkerId {
	workers := make(map[WorkerId]bool)
	for _, dst := range m.Table {
		workers[dst] = true
	}
	ret := make([]WorkerId, 0, len(workers))
	for dst, _ := range workers {
		ret = append(ret, dst)
	}
	return ret
}

// Generate `id`'s specific migration plan according to the overall plan
func NewSingleNodeMigration(id WorkerId, original *HashSlotRing, migration *Migration) *SingleNodeMigration {
	table := make(MigrationTable)
	stat := make(map[WorkerId]int)
	for slotId, dstId := range migration.Table {
		srcId := original.GetWorkerIdBySlot(slotId)
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

// Separate an overall migration plan to small plans specific to different workers.
func (m Migration) Separate(original *HashSlotRing) map[WorkerId]*SingleNodeMigration {
	// stat how many workers in the original hash ring
	ret := make(map[WorkerId]*SingleNodeMigration)
	for slotId := range m.Table {
		srcId := original.GetWorkerIdBySlot(slotId)
		if srcId == 0 {
			continue
		}
		ret[srcId] = NewSingleNodeMigration(srcId, original, &m)
	}
	return ret
}

func (m Migration) GetDestinations() []WorkerId {
	workers := make(map[WorkerId]bool)
	for _, dst := range m.Table {
		workers[dst] = true
	}
	ret := make([]WorkerId, 0, len(workers))
	for dst, _ := range workers {
		ret = append(ret, dst)
	}
	return ret
}

func (m Migration) Migrate(original *HashSlotRing) *HashSlotRing {
	ret := make(HashSlotRing, len(*original))
	for slot, dst := range m.Table {
		ret[slot] = dst
	}
	return &ret
}

// Hash slot allocator's interface. A hash slot allocator should yield a migration plan
// according to the given original slot ring and the new worker to add into.
type HashSlotAllocator interface {
	AllocateSlots(ring *HashSlotRing, oldWorkers map[WorkerId]*Worker, newWorker *Worker) (MigrationTable, error)
}
