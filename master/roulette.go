package master

import (
	"github.com/eyeKill/KV/common"
	"math/rand"
)

type RouletteAllocator struct{}

// This method is based on roulette algorithm
// We use a better strategy here, create an array of slot ids and shuffle it
// and then split the array according to their weights
func (r RouletteAllocator) AllocateSlots(ring *common.HashSlotRing, oldWorkers map[common.WorkerId]*common.Worker, newWorker *common.Worker) (common.MigrationTable, error) {
	slog := common.SugaredLog()
	table := common.MigrationTable{}
	// print new workers for inspection
	if len(oldWorkers) == 0 {
		slog.Info("Initializing new hash ring...")
		slotArr := make([]common.SlotId, len(*ring))
		for i := 0; i < len(*ring); i++ {
			slotArr[i] = common.SlotId(i)
		}
		// shuffle it
		slotArr = r.selectFrom(slotArr, len(slotArr))
		// distribute it to different new workers
		for _, slot := range slotArr {
			table[slot] = newWorker.Id
		}
	} else {
		// already allocated before, we need to add new node to the hash ring
		// to make things easier we change the data structure here...
		oldWorkerSlots := make(map[common.WorkerId][]common.SlotId)
		for s, w := range *ring {
			oldWorkerSlots[w] = append(oldWorkerSlots[w], common.SlotId(s))
		}

		if _, ok := oldWorkerSlots[0]; ok {
			panic("Error, still have unallocated slots before.")
		}
		var oldWeight float32 = 0
		for _, n := range oldWorkers {
			oldWeight += n.Weight
		}
		var newWeight = newWorker.Weight
		toAllocate := uint32(float32(len(*ring))*newWeight/(newWeight+oldWeight) + 0.5)
		slog.Infof("Amount of allocation: %d", toAllocate)

		var w float32 = 0
		idxStart := 0
		for id, slots := range oldWorkerSlots {
			worker := oldWorkers[id]
			w += worker.Weight
			idxEnd := int(float32(toAllocate)*w/oldWeight + 0.5)
			chosen := r.selectFrom(slots, idxEnd-idxStart)
			for _, s := range chosen {
				table[s] = newWorker.Id
			}
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
