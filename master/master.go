package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"path"
	"reflect"
	"strconv"
	"sync"
)

type Server struct {
	pb.UnimplementedKVMasterServer
	Hostname string
	Port     uint16

	conn      *zk.Conn
	rwLock    sync.RWMutex
	slots     common.HashSlotRing
	allocator common.HashSlotAllocator
	primaries *common.ZNodeChildrenCache
	backups   *common.ZNodeChildrenCache
	workers   map[common.WorkerId]*common.Worker
}

func NewMasterServer(hostname string, port uint16) Server {
	return Server{
		Hostname:  hostname,
		Port:      port,
		slots:     common.NewHashSlotRing(common.DEFAULT_SLOT_COUNT),
		allocator: RouletteAllocator{},
		workers:   make(map[common.WorkerId]*common.Worker),
	}
}

// get cache
func (m *Server) getPrimaryCache(key string) (*common.PrimaryWorkerNode, error) {
	var ret common.PrimaryWorkerNode
	b, ok := m.primaries.Get(key)
	if !ok {
		return nil, errors.New("cache entry not found")
	}
	if err := json.Unmarshal(b, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func (m *Server) getBackupCache(key string) (*common.BackupWorkerNode, error) {
	var ret common.BackupWorkerNode
	b, ok := m.backups.Get(key)
	if !ok {
		return nil, errors.New("cache entry not found")
	}
	if err := json.Unmarshal(b, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func getPrimary(content []byte) (*common.PrimaryWorkerNode, error) {
	node, err := common.UnmarshalNode(content)
	if err != nil {
		return nil, err
	}
	switch node.(type) {
	case *common.PrimaryWorkerNode:
		return node.(*common.PrimaryWorkerNode), nil
	default:
		return nil, errors.New(fmt.Sprintf("Wrong type, expecting PrimaryWorkerNode, found %s", reflect.TypeOf(node)))
	}
}

func getBackup(content []byte) (*common.BackupWorkerNode, error) {
	node, err := common.UnmarshalNode(content)
	if err != nil {
		return nil, err
	}
	switch node.(type) {
	case *common.BackupWorkerNode:
		return node.(*common.BackupWorkerNode), nil
	default:
		return nil, errors.New(fmt.Sprintf("Wrong type, expecting BackupWorkerNode, found %s", reflect.TypeOf(node)))
	}
}

// gRPC call handler
func (m *Server) GetWorkerByKey(_ context.Context, key *pb.Key) (*pb.GetWorkerResponse, error) {
	log := common.Log()
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	id := m.slots.GetWorkerId(key.Key)
	if id > 0 {
		if m.workers[id].Primary.Valid {
			primary, err := m.getPrimaryCache(m.workers[id].Primary.Name)
			if err != nil {
				log.Error("Failed to get primary node.", zap.Error(err))
			} else {
				return &pb.GetWorkerResponse{
					Status: pb.Status_OK,
					Worker: &pb.Worker{
						Hostname: primary.Host.Hostname,
						Port:     int32(primary.Host.Port),
					},
				}, nil
			}
		}
		// try backups
		for _, b := range m.workers[id].Backups {
			if b.Valid {
				backup, err := m.getBackupCache(b.Name)
				if err != nil {
					log.Error("Failed to get backup node.", zap.Error(err))
				} else {
					return &pb.GetWorkerResponse{
						Status: pb.Status_OK,
						Worker: &pb.Worker{
							Hostname: backup.Host.Hostname,
							Port:     int32(backup.Host.Port),
						},
					}, nil
				}
			}
		}
	}
	return &pb.GetWorkerResponse{
		Status: pb.Status_ENOSERVER,
		Worker: nil,
	}, nil
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

	// now register our cache objects...
	primary, err := common.NewZnodeChildrenCache(common.ZK_NODES_ROOT, common.ZK_PRIMARY_NAME, conn)
	if err != nil {
		log.Panic("Failed to create children cache for primaries.", zap.Error(err))
	}
	m.primaries = primary
	backup, err := common.NewZnodeChildrenCache(common.ZK_NODES_ROOT, common.ZK_BACKUP_NAME, conn)
	if err != nil {
		log.Panic("Failed to create children cache for backups.", zap.Error(err))
	}
	m.backups = backup
	return nil
}

// Watch node metadata in zookeeper and update accordingly.
func (m *Server) Watch(stopChan <-chan struct{}) {
	log := common.Log()
	log.Info("Starting watch loop.")
	pStop := make(chan struct{})
	bStop := make(chan struct{})
	go m.primaries.Watch(pStop)
	go m.backups.Watch(bStop)

watchLoop:
	for {
		// dirty WorkerNodes
		updates := make(map[common.WorkerId]struct{})
		select {
		case updated := <-m.primaries.AddChan:
			primary, err := getPrimary(updated.Content)
			if err != nil {
				log.Error("Failed to get primary.", zap.Error(err))
			}
			// add new worker to worker set
			newWorkers := map[common.WorkerId]*common.Worker{
				primary.Id: {
					Id:     primary.Id,
					Weight: primary.Weight,
					Primary: common.NodeEntry{
						Name:  updated.Name,
						Valid: true,
					},
					Backups: nil,
				},
			}
			// do migration
			// TODO make this async
			table, err := m.allocateSlots(newWorkers)
			if err != nil {
				log.Error("Failed to calculate migration table.", zap.Error(err))
			}
			m.migrate(table)
			// add new workers to primary table
			m.rwLock.Lock()
			// TODO persist migration table into zookeeper
			for k, v := range newWorkers {
				m.workers[k] = v
				updates[k] = struct{}{}
			}
			m.rwLock.Unlock()
		case updated := <-m.backups.AddChan:
			// just add it to the configuration :)
			backup, err := getBackup(updated.Content)
			if err != nil {
				log.Error("Failed to get backup.", zap.Error(err))
			}
			m.rwLock.Lock()

			m.workers[backup.Id].Backups = append(m.workers[backup.Id].Backups, common.NodeEntry{
				Name:  updated.Name,
				Valid: true,
			})
			updates[backup.Id] = struct{}{}
			m.rwLock.Unlock()
		case updated := <-m.primaries.RemoveChan:
			// we do not support removing workers for now, so primary have to stay intact
			// just set the primary to invalid
			primary, err := getPrimary(updated.Content)
			if err != nil {
				log.Error("Failed to get primary.", zap.Error(err))
			}
			m.rwLock.Lock()
			if m.workers[primary.Id].Primary.Name != updated.Name {
				log.Error("Inconsistency.", zap.String("have", m.workers[primary.Id].Primary.Name),
					zap.String("got", updated.Name))
			}
			m.workers[primary.Id].Primary.Valid = false
			updates[primary.Id] = struct{}{}
			m.rwLock.Unlock()
		case updated := <-m.backups.RemoveChan:
			backup, err := getBackup(updated.Content)
			if err != nil {
				log.Error("Failed to get backup.", zap.Error(err))
			}
			m.rwLock.Lock()
			for _, n := range m.workers[backup.Id].Backups {
				if n.Name == updated.Name {
					n.Valid = false
					updates[backup.Id] = struct{}{}
					break
				}
			}
			m.rwLock.Unlock()

		case <-stopChan:
			log.Info("Stop signal received, exiting watch loop...")
			close(pStop)
			close(bStop)
			break watchLoop
		}
		// modify nodes persisted in zookeeper
		m.rwLock.RLock()
		log.Info("Writing primary metadata back to zookeeper...")
		var ops []interface{}
		for id := range updates {
			log.Sugar().Infof("Updating %d to %+v", id, m.workers[id])
			dat, err := json.Marshal(m.workers[id])
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
					zap.Any("request", ops[i]), zap.String("response", r.String), zap.Error(r.Error))
			}
		}
		m.rwLock.RUnlock()

		// COMMIT POINT
		log.Sugar().Info(m.primaries)
		log.Sugar().Info(m.backups)
	}
}

func (m *Server) allocateSlots(newWorkers map[common.WorkerId]*common.Worker) (*common.SlotMigration, error) {
	table, err := m.allocator.AllocateSlots(&m.slots, m.workers, newWorkers)
	if err != nil {
		return nil, err
	}
	return &common.SlotMigration{
		Version: m.slots.Version + 1,
		Table:   table,
	}, nil
}

func (m *Server) migrate(migration *common.SlotMigration) {
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
