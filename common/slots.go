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
type SlotMigration struct {
	Version uint32
	Table   MigrationTable
}

// An allocator handles slot migration. Given a set of new workers,
// it should be able to yield a valid migration table.
type HashSlotAllocator interface {
	AllocateSlots(ring *HashSlotRing,
		oldWorkers map[WorkerId]*Worker, newWorkers map[WorkerId]*Worker) (MigrationTable, error)
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

func NewSingleNodeMigration(id WorkerId, original *HashSlotRing, migration *SlotMigration) *SingleNodeMigration {
	table := make(MigrationTable)
	for slot, srcId := range *original {
		if srcId == id {
			table[SlotId(slot)] = migration.Table[SlotId(slot)]
		}
	}
	return &SingleNodeMigration{
		Version: migration.Version,
		Id:      id,
		Table:   table,
		Len:     len(*original),
	}
}

// separate an overall migration plan to small plans specific to different nodes
func (m *SlotMigration) Separate(original *HashSlotRing) map[WorkerId]*SingleNodeMigration {
	// stat how many workers
	ret := make(map[WorkerId]*SingleNodeMigration)
	for _, dstId := range m.Table {
		if _, ok := ret[dstId]; !ok {
			ret[dstId] = nil
		}
	}
	for i := range ret {
		ret[i] = NewSingleNodeMigration(i, original, m)
	}
	SugaredLog().Infof("%+v", ret)
	return ret
}

//
//type RingBoolVector struct {
//	Vector []uint8
//}
//
//func (r RingBoolVector) GetSlotId(key string) SlotId {
//	return GetSlotId(key, len(r.Vector))
//}
//
//func (r RingBoolVector) Set(id SlotId, value bool) {
//	if value {
//		r.Vector[id/8] |= 1 << (id % 8)
//	} else {
//		r.Vector[id/8] ^= ^(1 << (id % 8))
//	}
//}
//
//func (r RingBoolVector) Get(id SlotId) bool {
//	return r.Vector[id/8]&(1<<(id%8)) > 0
//}
//
//func (r RingBoolVector) GetByKey(key string) bool {
//	return r.Get(r.GetSlotId(key))
//}
