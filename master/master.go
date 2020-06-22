package master

import (
	"context"
	"encoding/json"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"hash/crc32"
	"path"
	"strconv"
	"sync"
)

type Server struct {
	pb.UnimplementedKVMasterServer
	Hostname string
	Port     uint16

	conn      *zk.Conn
	rwLock    sync.RWMutex
	slots     HashSlotRing
	allocator HashSlotAllocator
	primaries map[string]*common.PrimaryWorkerNode
	backups   map[string]*common.BackupWorkerNode
	workers   map[common.WorkerId]*common.Worker
}

func NewMasterServer(hostname string, port uint16) Server {
	return Server{
		Hostname:  hostname,
		Port:      port,
		slots:     NewHashSlotRing(DEFAULT_SLOT_COUNT),
		allocator: RouletteAllocator{},
		primaries: make(map[string]*common.PrimaryWorkerNode),
		backups:   make(map[string]*common.BackupWorkerNode),
		workers:   make(map[common.WorkerId]*common.Worker),
	}
}

// gRPC call handler
func (m *Server) GetWorkerByKey(_ context.Context, key *pb.Key) (*pb.GetWorkerResponse, error) {
	m.rwLock.RLock()
	h := crc32.ChecksumIEEE([]byte(key.Key))
	id := m.slots.Slots[h%uint32(m.slots.Len())]
	m.rwLock.RUnlock()
	if id == 0 {
		return &pb.GetWorkerResponse{
			Status: pb.Status_ENOSERVER,
		}, nil
	} else {
		primary, ok := m.primaries[m.workers[id].Primary]
		if !ok {
			common.Log().Panic("Inconsistency encountered.", zap.Uint16("id", uint16(id)),
				zap.String("chName", m.workers[id].Primary))
		}
		return &pb.GetWorkerResponse{
			Status: pb.Status_OK,
			Worker: &pb.Worker{
				Hostname: primary.Host.Hostname,
				Port:     int32(primary.Host.Port),
			},
		}, nil
	}
}

func (m *Server) RegisterToZk(conn *zk.Conn) error {
	log := common.Log()
	// ensure all paths exist
	if err := common.EnsurePathRecursive(conn, common.ZK_NODES_ROOT); err != nil {
		return err
	}
	if err := common.EnsurePath(conn, common.ZK_MIGRATIONS_ROOT); err != nil {
		return err
	}
	if err := common.EnsurePath(conn, common.ZK_WORKERS_ROOT); err != nil {
		return err
	}
	// create primary id
	distributedId := common.DistributedAtomicInteger{Conn: conn, Path: common.ZK_WORKER_ID}
	// Initial value should be 0, if not set
	// the first valid value should be 1
	if err := distributedId.SetDefault(0); err != nil {
		log.Panic("Failed to initialize global primary id.", zap.Error(err))
	}

	nodePath := path.Join(common.ZK_ROOT, common.ZK_MASTER_NAME)
	data := common.NewMasterNode(m.Hostname, m.Port)
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

// Watch node metadata in zookeeper and update accordingly.
func (m *Server) Watch(stopChan <-chan struct{}) {
	log := common.Log()
	log.Info("Starting watch loop.")

watchLoop:
	for {
		children, _, eventChan, err := m.conn.ChildrenW(common.ZK_NODES_ROOT)
		if err != nil {
			log.Info("Error occurred while watching nodes dir.", zap.Error(err))
			continue
		}

		// contents under ZK_NODES_ROOT will not change
		// we only need to track creating/deleting of individual nodes
		// log changes locally
		lostPrimaries := make(map[string]*common.PrimaryWorkerNode)
		for k, v := range m.primaries {
			lostPrimaries[k] = v
		}
		lostBackups := make(map[string]*common.BackupWorkerNode)
		for k, v := range m.backups {
			lostBackups[k] = v
		}
		var newPrimaries = make(map[string]*common.PrimaryWorkerNode)
		var newBackups = make(map[string]*common.BackupWorkerNode)

		for _, chName := range children {
			chPath := path.Join(common.ZK_NODES_ROOT, chName)
			b, _, err := m.conn.Get(chPath)
			if err != nil {
				log.Warn("Failed to retrieve data.", zap.String("path", chPath))
				continue
			}
			n, err := common.UnmarshalNode(b)
			if err != nil {
				log.Warn("Data invalid.", zap.String("path", chPath), zap.ByteString("content", b))
				continue
			}
			switch n.(type) {
			case *common.MasterNode:
				log.Warn("Found master node under ZK_NODES_ROOT", zap.String("name", chName))
				continue
			case *common.PrimaryWorkerNode:
				node := n.(*common.PrimaryWorkerNode)
				if _, ok := lostPrimaries[chName]; ok {
					delete(lostBackups, chName)
					continue
				} else {
					newPrimaries[chName] = node
				}
			case *common.BackupWorkerNode:
				node := n.(*common.BackupWorkerNode)
				if _, ok := lostBackups[chName]; ok {
					delete(lostBackups, chName)
					continue
				} else {
					newBackups[chName] = node
				}
			}
		}
		log.Info("Fetched update from zookeeper.")

		update := make(map[common.WorkerId]*common.Worker)
		// do migration according to lost & new nodes
		for chName, node := range lostPrimaries {
			log.Warn("Primary down detected.",
				zap.String("name", chName), zap.Uint16("id", uint16(node.Id)))
			cpy := *m.workers[node.Id]
			cpy.Primary = ""
			update[node.Id] = &cpy
			// TODO bump a backup node up
		}
		for chName, node := range lostBackups {
			log.Warn("Backup down detected.",
				zap.String("name", chName), zap.Uint16("id", uint16(node.Id)))
			if _, ok := update[node.Id]; !ok {
				cpy := *m.workers[node.Id]
				update[node.Id] = &cpy
			}
			// modify in place
			update[node.Id].Backups = common.RemoveElement(update[node.Id].Backups, chName)
		}
		newWorkers := make(map[common.WorkerId]*common.Worker)
		for chName, node := range newPrimaries {
			log.Info("New primary detected.", zap.String("name", chName), zap.Uint16("id", uint16(node.Id)))
			update[node.Id] = &common.Worker{
				Id:      node.Id,
				Weight:  node.Weight,
				Primary: chName,
				Backups: nil,
			}
			newWorkers[node.Id] = update[node.Id]
		}
		for chName, node := range newBackups {
			// TODO what if it is doing migration?
			log.Info("New backup detected.", zap.String("name", chName), zap.Uint16("id", uint16(node.Id)))
			if _, ok := m.workers[node.Id]; !ok {
				if _, ok := update[node.Id]; !ok {
					log.Error("Inconsistency: Backup have an invalid worker id.",
						zap.Int("id", int(node.Id)), zap.String("chName", chName))
					continue
				} else {
					// update entry in update[node.Id]
					update[node.Id].Backups = append(update[node.Id].Backups, chName)
				}
			} else {
				cpy := *m.workers[node.Id]
				cpy.Backups = append(cpy.Backups, chName)
				update[node.Id] = &cpy
			}
		}

		// allocate slots for new primaries
		table, err := m.allocateSlots(newWorkers)
		if err != nil {
			log.Error("Failed to calculate migration table.", zap.Error(err))
		}
		m.migrate(table)
		// add new workers to primary table
		m.rwLock.Lock()
		for k, v := range newWorkers {
			m.workers[k] = v
		}
		m.rwLock.Unlock()

		// modify nodes persisted in zookeeper
		m.rwLock.RLock()
		log.Info("Writing primary metadata back to zookeeper...")
		var ops []interface{}
		for id, w := range update {
			log.Sugar().Infof("Updating %d to %+v", id, *w)
			dat, err := json.Marshal(w)
			if err != nil {
				log.Error("Failed to marshall metadata.", zap.Error(err))
			}
			ops = append(ops, &zk.SetDataRequest{
				Path:    path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(id))),
				Data:    dat,
				Version: -1,
			})
		}
		resp, err := m.conn.Multi(ops...)
		if err != nil {
			log.Error("Failed to rewrite primary metadata.", zap.Error(err))
		}
		for i, r := range resp {
			if r.Error != nil {
				log.Error("Failed to rewrite single node.",
					zap.Any("request", ops[i]), zap.String("response", r.String))
			}
		}
		m.rwLock.RUnlock()

		// COMMIT POINT
		// overwrite local cache
		for chName := range lostPrimaries {
			delete(m.primaries, chName)
		}
		for chName := range lostBackups {
			delete(m.backups, chName)
		}
		for chName, node := range newPrimaries {
			m.primaries[chName] = node
		}
		for chName, node := range newBackups {
			m.backups[chName] = node
		}
		for id, worker := range update {
			m.workers[id] = worker
		}

		select {
		case event := <-eventChan:
			log.Sugar().Infof("Received event %v", event)
		case <-stopChan:
			log.Info("Stop signal received, exiting watch loop...")
			break watchLoop
		}
	}
}

func (m *Server) allocateSlots(newWorkers map[common.WorkerId]*common.Worker) (*SlotMigration, error) {
	table, err := m.allocator.AllocateSlots(&m.slots, m.workers, newWorkers)
	if err != nil {
		return nil, err
	}
	return &SlotMigration{
		Version: m.slots.Version + 1,
		Table:   table,
	}, nil
}

func (m *Server) migrate(migration *SlotMigration) {
	log := common.SugaredLog()
	// update the slot table
	if migration.Version != m.slots.Version+1 {
		log.Panic("Error: Migration version mismatch.",
			zap.Uint("current", m.slots.Version), zap.Uint("proposed", migration.Version))
	}
	for slot, workerId := range migration.Table {
		m.slots.Slots[slot] = workerId
	}
	m.slots.Version = migration.Version
	log.Infof("Successfully migrated %d slots.", len(migration.Table))
	// print statistics
	stat := make(map[common.WorkerId]int)
	for _, id := range m.slots.Slots {
		stat[id] += 1
	}
	log.Infof("Distribution for now: %v", stat)
}
