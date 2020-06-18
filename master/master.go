package master

import (
	"context"
	"encoding/json"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"hash/crc32"
	"math/rand"
	"path"
	"sync"
)

type MasterServer struct {
	pb.UnimplementedKVMasterServer
	Hostname string
	Port     uint16

	conn    *zk.Conn
	rwLock  sync.RWMutex
	slots   HashSlotRing
	workers map[common.WorkerId]common.Worker
}

func NewMasterServer(hostname string, port uint16) MasterServer {
	return MasterServer{
		Hostname: hostname,
		Port:     port,
		slots:    NewHashSlotRing(DEFAULT_SLOT_COUNT),
	}
}

// gRPC call handler
func (m *MasterServer) GetWorker(_ context.Context, key *pb.Key) (*pb.GetWorkerResponse, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	h := crc32.ChecksumIEEE([]byte(key.Key))
	id := m.slots.Slots[h%uint32(m.slots.Len())]
	if id == 0 {
		return &pb.GetWorkerResponse{
			Status: pb.Status_ENOSERVER,
		}, nil
	} else {
		return &pb.GetWorkerResponse{
			Status: pb.Status_OK,
			Worker: &pb.Worker{
				Hostname: m.workers[id].Host.Hostname,
				Port:     int32(m.workers[id].Host.Port),
			},
		}, nil
	}
}

func (m *MasterServer) RegisterToZk(conn *zk.Conn) error {
	log := common.Log()
	// ensure all paths exist
	if err := common.EnsurePathRecursive(conn, common.ZK_WORKERS_ROOT); err != nil {
		return err
	}
	if err := common.EnsurePath(conn, common.ZK_MIGRATIONS_ROOT); err != nil {
		return err
	}

	// create worker id
	distributedId := common.DistributedAtomicInteger{Conn: conn, Path: common.ZK_WORKER_ID}
	// Initial value should be 0, if not set
	// the first valid value should be 1
	if err := distributedId.SetDefault(0); err != nil {
		log.Panic("Failed to initialize global worker id.", zap.Error(err))
	}

	nodePath := path.Join(common.ZK_ROOT, common.ZK_MASTER_NAME)
	data := common.GetNewMasterNode(m.Hostname, m.Port)
	b, err := json.Marshal(data)
	if err != nil {
		log.Panic("Failed to marshall into json object.", zap.Error(err))
	}
	if _, err = conn.CreateProtectedEphemeralSequential(nodePath, b, zk.WorldACL(zk.PermAll)); err != nil {
		log.Panic("Failed to register itself to zookeeper.", zap.Error(err))
	}
	m.conn = conn
	return nil
}

// Watch node matadata in zookeeper and update timely.
func (m *MasterServer) Watch(stopChan <-chan struct{}) {
	log := common.Log()
	log.Info("Starting watch loop.")

watchLoop:
	for {
		children, _, eventChan, err := m.conn.ChildrenW(common.ZK_WORKERS_ROOT)
		if err != nil {
			log.Info("Error occurred while watching nodes dir.", zap.Error(err))
			continue
		}

		// get largest worker id from previous worker set
		// to determine which one is new
		var oldIds common.WorkerId = 0
		for _, w := range m.workers {
			if oldIds < w.Id {
				oldIds = w.Id
			}
		}

		// update local metadata cache
		workers := make(map[common.WorkerId]common.Worker)
		newWorkers := make([]common.Worker, 0)
		for _, chName := range children {
			var data common.Worker
			chPath := path.Join(common.ZK_WORKERS_ROOT, chName)
			b, _, err := m.conn.Get(chPath)
			if err != nil {
				log.Warn("Failed to retrieve data.", zap.String("path", chPath))
				continue
			}
			if err := json.Unmarshal(b, &data); err != nil {
				log.Warn("Data invalid.", zap.String("path", chPath), zap.ByteString("content", b))
				continue
			}
			if data.Id > oldIds {
				newWorkers = append(newWorkers, data)
			} else {
				workers[data.Id] = data
			}
		}
		log.Sugar().Infof("Retrieved newest children, workers:%v, newWorkers:%v", workers, newWorkers)
		if len(newWorkers) > 0 {
			// allocate new slots to them & do migration
			migration := m.allocateSlots(newWorkers)
			m.migrate(migration)
			for _, w := range newWorkers {
				workers[w.Id] = w
			}
		}

		m.rwLock.Lock()
		m.workers = workers
		m.rwLock.Unlock()

		select {
		case event := <-eventChan:
			log.Sugar().Infof("Received event %v", event)
		case <-stopChan:
			log.Info("Stop signal received, exiting watch loop...")
			break watchLoop
		}
	}
}

func (m *MasterServer) allocateSlots(newWorkers []common.Worker) SlotMigration {
	// This method is based on roulette algorithm
	// We use a better strategy here, create an array of slot ids and shuffle it
	// and then split the array according to the weights
	slog := common.SugaredLog()

	// Get weight already allocated
	// this part stays intact
	var weight int64 = 0
	if len(m.workers) > 0 {
		for _, worker := range m.workers {
			weight += int64(worker.Weight)
		}
	}

	totalWeight := weight
	// Append new workers
	for _, worker := range newWorkers {
		totalWeight += int64(worker.Weight)
	}

	// Roulette
	migration := SlotMigration{
		Version:        m.slots.Version + 1,
		MigrationTable: make(map[common.SlotId]common.WorkerId),
	}

	// Generate pseudo-random permutation
	// this code is derived from rand.Perm
	slotArr := make([]common.SlotId, m.slots.Len())
	for i := uint16(1); i < m.slots.Len(); i++ {
		j := rand.Int31n(int32(i))
		slotArr[i] = slotArr[j]
		slotArr[j] = common.SlotId(i)
	}

	slotSlice := slotArr[weight*int64(m.slots.Len())/totalWeight:]
	for _, n := range newWorkers {
		idx := int64(n.Weight) * int64(m.slots.Len()) / totalWeight
		slog.Infof("Roulette: distributing %d slots to worker %d.", idx, n.Id)
		for _, s := range slotSlice[:idx] {
			migration.MigrationTable[s] = n.Id
		}
		slotSlice = slotSlice[idx:]
	}
	return migration
}

func (m *MasterServer) migrate(migration SlotMigration) {
	log := common.SugaredLog()
	// update the slot table
	if migration.Version != m.slots.Version+1 {
		log.Panic("Error: Migration version mismatch.",
			zap.Uint("current", m.slots.Version), zap.Uint("proposed", migration.Version))
	}
	m.rwLock.Lock()
	for slot, workerId := range migration.MigrationTable {
		m.slots.Slots[slot] = workerId
	}
	m.slots.Version = migration.Version
	m.rwLock.Unlock()
	log.Infof("Successfully migrated %d slots.", len(migration.MigrationTable))
	// print statistics
	stat := make(map[common.WorkerId]int)
	for _, id := range m.slots.Slots {
		stat[id] += 1
	}
	log.Infof("Distribution for now: %v", stat)
}
