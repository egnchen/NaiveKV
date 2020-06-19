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
		workerSlots := make(map[common.WorkerId][]common.SlotId)
		for s, w := range ring.Slots {
			workerSlots[w] = append(workerSlots[w], common.SlotId(s))
		}
		if _, ok := workerSlots[0]; ok {
			panic("Error, still have unallocated slots before.")
		}
		var oldWeight int64 = 0
		for _, n := range oldWorkers {
			oldWeight += int64(n.Weight)
		}
		var newWeight int64 = 0
		for _, n := range newWorkers {
			newWeight += int64(n.Weight)
		}
		toAllocate := (int64(ring.Len())*newWeight + 1) / (newWeight + oldWeight)
		slog.Infof("to allocate: %d", toAllocate)
		for id, slots := range workerSlots {
			worker, ok := oldWorkers[id]
			if !ok {
				panic("Error: Inconsistent ring & worker map.")
			}
			s := r.selectFrom(slots, int(toAllocate*int64(worker.Weight)/oldWeight))
			r.distributeTo(s, &table, newWorkers)
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
