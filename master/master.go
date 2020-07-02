package master

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"path"
	"reflect"
	"sort"
	"strconv"
	"sync"
)

type Server struct {
	pb.UnimplementedKVMasterServer
	Hostname string
	Port     uint16

	conn          *zk.Conn
	version       uint32
	versionId     common.DistributedAtomicInteger
	migrationLock sync.Mutex
	slots         common.HashSlotRing
	allocator     common.HashSlotAllocator
	rwLock        sync.RWMutex
	rootWatch     <-chan zk.Event
	workers       map[common.WorkerId]*common.Worker
}

func NewMasterServer(hostname string, port uint16) Server {
	return Server{
		Hostname:  hostname,
		Port:      port,
		slots:     common.NewHashSlotRing(),
		allocator: RouletteAllocator{},
		workers:   make(map[common.WorkerId]*common.Worker),
	}
}

// Migration procedure. First calculate migration table and commit it to zookeeper,
// then wait for all worker worker to pick up & complete migration(in sync with the destination worker),
// finally commit the new version number & new hash slot table to zookeeper & start serving the latest slot table.
// Commit point is after the new version number & hash slot table being committed to zookeeper.
func (m *Server) doMigration(newWorkers map[common.WorkerId]*common.Worker) {
	log := common.SugaredLog()
	m.migrationLock.Lock()
	defer m.migrationLock.Unlock()
	// allocate slots
	migration, err := m.calcMigration(newWorkers)
	if err != nil {
		log.Error("Failed to allocate slots.", zap.Error(err))
	}
	completeSem, err := m.syncMigration(migration)
	if err != nil {
		log.Error("Failed to sync migration information to zookeeper.", zap.Error(err))
	}
	log.Infof("Migration plan version %d successfully uploaded.", migration.Version)
	if err := completeSem.Watch(func(newValue int) bool { return newValue > 0 }); err != nil {
		log.Error("Failed to watch semaphore.", zap.Error(err))
	}
	log.Infof("Successfully migrated %d slots, committing...", len(migration.Table))

	// completed, commit hash slot ring to zookeeper
	newSlot := common.NewHashSlotRing()
	copy(newSlot, m.slots)
	for slot, dstId := range migration.Table {
		newSlot[slot] = dstId
	}
	bin, _ := json.Marshal(newSlot)
	if _, err := m.conn.Multi(
		&zk.SetDataRequest{
			Path:    path.Join(common.ZK_ROOT, common.ZK_VERSION_NAME),
			Data:    []byte(strconv.Itoa(int(migration.Version + 1))),
			Version: -1,
		},
		&zk.SetDataRequest{
			Path:    path.Join(common.ZK_ROOT, common.ZK_TABLE_NAME),
			Data:    bin,
			Version: -1,
		}); err != nil {
		log.Error("Failed to commit change to migration.", zap.Error(err))
	}
	log.Infof("Migration reached commit point.")
	// COMMIT POINT
	bin, err = json.Marshal(newSlot)
	if err != nil {
		return
	}
	resps, err := m.conn.Multi(&zk.SetDataRequest{
		Path:    path.Join(common.ZK_ROOT, common.ZK_VERSION_NAME),
		Data:    []byte(strconv.Itoa(int(m.version + 1))),
		Version: -1,
	}, &zk.SetDataRequest{
		Path:    path.Join(common.ZK_ROOT, common.ZK_TABLE_NAME),
		Data:    bin,
		Version: -1,
	})
	if err != nil {
		log.Error("Failed to update table & version number.", zap.Error(err))
		for _, resp := range resps {
			if resp.Error != nil {
				log.Error("Error:", zap.Error(err))
			}
		}
	}
	// update local migration
	m.rwLock.Lock()
	m.slots = newSlot
	m.version += 1
	// add new workers
	for k, v := range newWorkers {
		m.workers[k] = v
	}
	m.rwLock.Unlock()
	// print statistics
	stat := make(map[common.WorkerId]int)
	for _, id := range m.slots {
		stat[id] += 1
	}
	log.Infof("Distribution for now: %v", stat)
}

// upload migration information to zookeeper
// contains individual migration plan and a semaphore with value set
func (m *Server) syncMigration(migration *common.Migration) (*common.DistributedAtomicInteger, error) {
	log := common.SugaredLog()
	if migration.Version != m.version {
		return nil, errors.New("migration version mismatch")
	}
	p := path.Join(common.ZK_MIGRATIONS_ROOT, strconv.Itoa(int(migration.Version)))
	var ops []interface{}
	ops = append(ops, &zk.CreateRequest{Path: p, Data: []byte(""), Acl: zk.WorldACL(zk.PermAll), Flags: 0})
	separatedTable := migration.Separate(&m.slots)
	for srcId, table := range separatedTable {
		bin, err := json.Marshal(table)
		if err != nil {
			return nil, err
		}
		ops = append(ops, &zk.CreateRequest{
			Path:  path.Join(p, strconv.Itoa(int(srcId))),
			Data:  bin,
			Acl:   zk.WorldACL(zk.PermAll),
			Flags: 0,
		})
	}
	// create a semaphore, wait for all workers to complete
	ops = append(ops, &zk.CreateRequest{
		Path:  path.Join(p, common.ZK_COMPLETE_SEM_NAME),
		Data:  []byte(strconv.Itoa(len(separatedTable))),
		Acl:   zk.WorldACL(zk.PermAll),
		Flags: 0,
	})
	log.Info("Uploading migration plan...")
	resps, err := m.conn.Multi(ops...)
	if err != nil {
		log.Error("Error occurred while uploading migration plan.", zap.Error(err))
		for _, r := range resps {
			if r.Error != nil {
				return nil, r.Error
			}
		}
	}
	completeSem := common.DistributedAtomicInteger{
		Conn: m.conn,
		Path: path.Join(p, common.ZK_COMPLETE_SEM_NAME),
	}
	return &completeSem, nil
}

// gRPC call handler
func (m *Server) GetWorkerByKey(_ context.Context, key *pb.Key) (*pb.GetWorkerResponse, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	id := m.slots.GetWorkerId(key.Key)
	if id == 0 {
		return &pb.GetWorkerResponse{
			Status: pb.Status_ENOSERVER,
			Worker: nil,
		}, nil
	}
	worker := m.workers[id]
	if worker.Primary != nil {
		return &pb.GetWorkerResponse{
			Status: pb.Status_OK,
			Worker: &pb.Worker{
				Hostname: worker.Primary.Host.Hostname,
				Port:     int32(worker.Primary.Host.Port),
			},
		}, nil
	} else {
		return &pb.GetWorkerResponse{
			Status: pb.Status_ENOSERVER,
			Worker: nil,
		}, nil
	}
}

// TODO Handle master single-point failure recovery, like /kv/worker directory
func (m *Server) RegisterToZk(conn *zk.Conn) error {
	log := common.Log()
	m.conn = conn

	// register itself
	nodePath := path.Join(common.ZK_MASTER_ROOT, common.ZK_MASTER_NAME)
	data := common.NewMasterNode(m.Hostname, m.Port)
	b, err := json.Marshal(data)
	if err != nil {
		log.Panic("Failed to marshall into json object.", zap.Error(err))
	}
	n, err := conn.Create(nodePath, b, zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Panic("Failed to register itself to zookeeper.", zap.Error(err))
	}
	name := path.Base(n)
	// now make sure that we are the lock holder
	for {
		children, _, eventChan, err := m.conn.ChildrenW(common.ZK_MASTER_ROOT)
		if err != nil {
			log.Panic("Failed to get all lock holders.")
		}
		sort.Strings(children)
		if children[0] == name {
			// lock holder is me!
			break
		}
		_ = <-eventChan
	}
	// ensure all paths exist
	if err := common.EnsurePathRecursive(conn, common.ZK_WORKERS_ROOT); err != nil {
		return err
	}
	if err := common.EnsurePath(conn, common.ZK_MIGRATIONS_ROOT); err != nil {
		return err
	}
	if err := common.EnsurePath(conn, common.ZK_MASTER_ROOT); err != nil {
		return err
	}
	if err := common.EnsurePath(conn, common.ZK_ELECTION_ROOT); err != nil {
		return err
	}
	// initialize version id & worker id
	versionId := common.DistributedAtomicInteger{
		Conn: conn,
		Path: path.Join(common.ZK_ROOT, common.ZK_VERSION_NAME),
	}
	err := versionId.SetDefault(0)
	if err != nil {
		return err
	}
	version, err := versionId.Get()
	if err != nil {
		return err
	}
	m.version = uint32(version)
	m.versionId = versionId
	workerId := common.DistributedAtomicInteger{
		Conn: conn,
		Path: path.Join(common.ZK_ROOT, common.ZK_WORKER_ID_NAME),
	}
	if err := workerId.SetDefault(1); err != nil {
		return err
	}
	// ensure table exists in zookeeper
	exists, _, err := conn.Exists(path.Join(common.ZK_ROOT, common.ZK_TABLE_NAME))
	if err != nil {
		return err
	}
	if !exists {
		// create one
		ring := common.NewHashSlotRing()
		bin, err := json.Marshal(ring)
		if err != nil {
			return err
		}
		_, err = conn.Create(path.Join(common.ZK_ROOT, common.ZK_TABLE_NAME), bin, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	} else {
		// retrieve the latest ring from zookeeper
		bin, _, err := conn.Get(path.Join(common.ZK_ROOT, common.ZK_TABLE_NAME))
		if err != nil {
			log.Error("Failed to get slot ring from zookeeper", zap.Error(err))
			return err
		}
		var slot common.HashSlotRing
		if err := json.Unmarshal(bin, &slot); err != nil {
			log.Error("Failed to parse slot ring in zookeeper", zap.Error(err))
			return err
		}
		m.slots = slot
		log.Info("Fetched latest distribution from zookeeper.")
	}

	return nil
}

// fetch all worker nodes & save it into local cache
// return a collection of event channels
func (m *Server) updateAll() error {
	workers, _, watch, err := m.conn.ChildrenW(common.ZK_WORKERS_ROOT)
	if err != nil {
		return err
	}
	// update all local worker cache
	newWorkers := make(map[common.WorkerId]*common.Worker)
	for _, w := range workers {
		id, err := strconv.Atoi(w)
		if err != nil {
			return err
		}
		worker, err := common.GetWorker(m.conn, common.WorkerId(id))
		if err != nil {
			return err
		}
		newWorkers[worker.Id] = &worker
	}
	m.rwLock.Lock()
	m.rootWatch = watch
	m.workers = newWorkers
	m.rwLock.Unlock()
	return nil
}

// Watch node metadata in zookeeper and update accordingly.
func (m *Server) Watch(stopChan <-chan struct{}) {
	log := common.Log()
	log.Info("Starting watch loop.")

	err := m.updateAll()
	if err != nil {
		log.Error("Failed to update all workers.", zap.Error(err))
		return
	}

	for {
		// construct select cases
		m.rwLock.RLock()
		var selectCases []reflect.SelectCase
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(m.rootWatch),
		})
		for _, c := range m.workers {
			selectCases = append(selectCases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(c.Watcher),
			})
		}
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(stopChan),
		})
		m.rwLock.RUnlock()
		log.Sugar().Infof("Watching %+v\n", selectCases)
		chosen, recv, recvOK := reflect.Select(selectCases)
		if chosen == 0 {
			if !recvOK {
				log.Info("Worker node watcher closed.")
			}
			// change in worker root, new worker worker
			log.Info("Change in worker root detected.")
			children, _, w, err := m.conn.ChildrenW(common.ZK_WORKERS_ROOT)
			if err != nil {
				log.Error("Failed to watch worker root again.", zap.Error(err))
			}
			newWorkers := make(map[common.WorkerId]*common.Worker)
			for _, c := range children {
				value, err := strconv.Atoi(c)
				if err != nil {
					log.Error("Invalid ZNode name.", zap.String("name", c))
				}
				id := common.WorkerId(value)
				if _, ok := m.workers[id]; !ok {
					// got a new worker
					worker, err := common.GetWorker(m.conn, id)
					if err != nil {
						log.Error("Failed to retrieve newest node.", zap.Error(err))
					}
					newWorkers[id] = &worker
				}
				m.rwLock.Lock()
				m.rootWatch = w
				m.rwLock.Unlock()
			}
			m.doMigration(newWorkers)
			// workers will never disappear
		} else if chosen == len(selectCases)-1 {
			// stop chan
			return
		} else {
			if !recvOK {
				log.Info("Worker node watcher closed.")
			}
			// one of the workers changed
			event := recv.Interface().(zk.Event)
			log.Info("Worker node change detected.", zap.String("path", event.Path))
			if event.Type == zk.EventNodeChildrenChanged {
				// update single node
				_, name := path.Split(event.Path)
				v, _ := strconv.Atoi(name) // IGNORED
				id := common.WorkerId(v)
				worker, err := common.GetWorker(m.conn, id)
				if err != nil {
					log.Error("Failed to retrieve node update.", zap.Error(err))
				}
				m.rwLock.Lock()
				m.workers[id] = &worker
				m.rwLock.Unlock()
			} else {
				log.Warn("Event type not right.", zap.String("type", event.Type.String()))
			}
		}
	}
}

// allocate slots for new workers
// this function supports multiple new workers
func (m *Server) calcMigration(newWorkers map[common.WorkerId]*common.Worker) (*common.Migration, error) {
	slog := common.SugaredLog()
	slots := common.NewHashSlotRing()
	finalTable := make(common.MigrationTable)
	copy(slots, m.slots)
	for _, nw := range newWorkers {
		slog.Infof("Allocating slots for new worker %d", nw.Id)
		table, err := m.allocator.AllocateSlots(&slots, m.workers, nw)
		if err != nil {
			return nil, err
		}
		// do modification on the local copy
		for k, v := range table {
			slots[k] = v
			finalTable[k] = v
		}
	}
	return &common.Migration{
		Version: m.version,
		Table:   finalTable,
	}, nil
}
