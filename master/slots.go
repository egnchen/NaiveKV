// definition for hash slots
package master

import (
	"github.com/eyeKill/KV/common"
	"math/rand"
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

type MigrationTable map[common.SlotId]common.WorkerId
type SlotMigration struct {
	Version uint
	Table   MigrationTable
}

// Interface for slot allocator. An allocator handles slot migration.
// Given a set of new workers, it should be able to yield a valid migration table.
type HashSlotAllocator interface {
	AllocateSlots(ring *HashSlotRing,
		oldWorkers map[common.WorkerId]common.Worker, newWorkers []common.Worker) (MigrationTable, error)
}

func (ring HashSlotRing) Len() uint16 {
	return uint16(len(ring.Slots))
}

type RouletteAllocator struct{}

// This method is based on roulette algorithm
// We use a better strategy here, create an array of slot ids and shuffle it
// and then split the array according to the weights
func (r RouletteAllocator) AllocateSlots(ring *HashSlotRing,
	oldWorkers map[common.WorkerId]common.Worker, newWorkers []common.Worker) (MigrationTable, error) {
	slog := common.SugaredLog()
	table := MigrationTable{}

	if len(oldWorkers) == 0 {
		slog.Info("Initializing new hash ring...")
		slotArr := make([]common.SlotId, ring.Len())
		for i := 0; i < int(ring.Len()); i++ {
			slotArr[i] = common.SlotId(i)
		}
		// shuffle it
		slotArr = r.selectFrom(slotArr, len(slotArr))
		// distribute it to different new workers
		r.distributeTo(slotArr, &table, newWorkers)
	} else {
		// already allocated before, we need to add new node to the hash ring
		// to make things easier we change the data structure here...
		oldWorkerSlots := make(map[common.WorkerId][]common.SlotId)
		for s, w := range ring.Slots {
			oldWorkerSlots[w] = append(oldWorkerSlots[w], common.SlotId(s))
		}
		if _, ok := oldWorkerSlots[0]; ok {
			panic("Error, still have unallocated slots before.")
		}
		var oldWeight float32 = 0
		for _, n := range oldWorkers {
			oldWeight += n.Weight
		}
		var newWeight float32 = 0
		for _, n := range newWorkers {
			newWeight += n.Weight
		}
		toAllocate := uint32(float32(ring.Len())*newWeight/(newWeight+oldWeight) + 0.5)
		slog.Infof("To allocate: %d", toAllocate)

		var w float32 = 0
		idxStart := 0
		for id, slots := range oldWorkerSlots {
			worker := oldWorkers[id]
			w += worker.Weight
			idxEnd := int(float32(toAllocate)*w/oldWeight + 0.5)
			s := r.selectFrom(slots, idxEnd-idxStart)
			r.distributeTo(s, &table, newWorkers)
			idxStart = idxEnd
		}
	}
	return table, nil
}

// select `count` elements randomly from slots
func (r RouletteAllocator) selectFrom(slots []common.SlotId, count int) []common.SlotId {
	if count < 0 || count > len(slots) {
		panic("invalid count")
	}
	rand.Shuffle(len(slots), func(i int, j int) { slots[i], slots[j] = slots[j], slots[i] })
	return slots[:count]
}

// distribute slots to different new workers according to their weights
func (r RouletteAllocator) distributeTo(slots []common.SlotId, table *MigrationTable, newWorkers []common.Worker) {
	slog := common.SugaredLog()
	var totalWeight int64 = 0
	for _, n := range newWorkers {
		totalWeight += int64(n.Weight)
	}
	var prevWeight int64 = 0
	l := int64(len(slots))
	for _, n := range newWorkers {
		startIdx := prevWeight * l / totalWeight
		endIdx := (prevWeight + int64(n.Weight)) * l / totalWeight
		slog.Infof("Roulette: distributing %d slots to worker %d.", endIdx-startIdx, n.Id)
		for _, s := range slots[startIdx:endIdx] {
			(*table)[s] = n.Id
		}
		prevWeight += int64(n.Weight)
	}
}
