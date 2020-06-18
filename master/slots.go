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

func NewHashSlotRing(size uint16) HashSlotRing {
	return HashSlotRing{
		Version: 0,
		Slots:   make([]common.WorkerId, size),
	}
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

	// Get weight already allocated
	// this part stays intact
	var occupied int64 = 0
	if len(oldWorkers) > 0 {
		for _, w := range oldWorkers {
			occupied += int64(w.Weight)
		}
	}

	totalWeight := occupied
	// Append new workers
	for _, worker := range newWorkers {
		totalWeight += int64(worker.Weight)
	}

	// Roulette
	table := make(MigrationTable)

	// Generate pseudo-random permutation
	// this code is derived from rand.Perm
	slotArr := make([]common.SlotId, ring.Len())
	for i := uint16(1); i < ring.Len(); i++ {
		j := rand.Int31n(int32(i))
		slotArr[i] = slotArr[j]
		slotArr[j] = common.SlotId(i)
	}

	prevWeight := occupied
	for _, n := range newWorkers {
		startIdx := prevWeight * int64(ring.Len()) / totalWeight
		endIdx := (prevWeight + int64(n.Weight)) * int64(ring.Len()) / totalWeight
		slog.Infof("Roulette: distributing %d slots to worker %d.", endIdx-startIdx, n.Id)
		for _, s := range slotArr[startIdx:endIdx] {
			table[s] = n.Id
		}
		prevWeight += int64(n.Weight)
	}
	return table, nil
}
