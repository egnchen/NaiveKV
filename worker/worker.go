package worker

// primary worker control plane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type PrimaryServer struct {
	pb.UnimplementedKVWorkerServer
	Hostname         string
	Port             uint16
	FilePath         string
	Id               common.WorkerId
	conn             *zk.Conn
	kv               KVStore
	SlotTableVersion uint32
	config           common.WorkerConfig
	backupCh         chan *pb.BackupEntry
	lock             sync.RWMutex
	backups          map[string]*SyncRoutine
	migrations       map[string]*SyncRoutine
	condition        *sync.Cond
	version          uint64
}

// the following two are for migration.

// Transfer bunch of data with transaction
func (s *PrimaryServer) Transfer(server pb.KVBackup_TransferServer) error {
	// start a new transaction
	tid, err := s.kv.StartTransaction()
	var version uint64
	if err != nil {
		// try again later
		return server.SendAndClose(&pb.BackupReply{
			Status:  pb.Status_EFAILED,
			Version: 0,
		})
	}
	for {
		ent, err := server.Recv()
		if err == io.EOF {
			// commit transaction
			if err := s.kv.Commit(tid); err != nil {
				return server.SendAndClose(&pb.BackupReply{
					Status:  pb.Status_EFAILED,
					Version: version,
				})
			} else {
				return server.SendAndClose(&pb.BackupReply{
					Status:  pb.Status_OK,
					Version: version,
				})
			}
		} else if err != nil {
			return err
		}
		switch ent.Op {
		case pb.Operation_PUT:
			_, err := s.kv.Put(ent.Key, ent.Value, tid)
			if err != nil {
				goto fail
			}
		case pb.Operation_DELETE:
			_, err := s.kv.Delete(ent.Key, tid)
			if err != nil {
				goto fail
			}
		default:
			goto fail
		}
		continue
	fail:
		return server.SendAndClose(&pb.BackupReply{
			Status:  pb.Status_EFAILED,
			Version: version,
		})
	}
}

// Transfer data piece by piece in a loss less manner
func (s *PrimaryServer) Sync(server pb.KVBackup_SyncServer) error {
	var version uint64
	for {
		ent, err := server.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		switch ent.Op {
		case pb.Operation_PUT:
			_, err := s.kv.Put(ent.Key, ent.Value, 0)
			if err != nil {
				if err := server.Send(&pb.BackupReply{
					Status:  pb.Status_EFAILED,
					Version: ent.Version,
				}); err != nil {
					return err
				}
			} else {
				if version < ent.Version {
					version = ent.Version
				}
				if err := server.Send(&pb.BackupReply{
					Status:  pb.Status_OK,
					Version: ent.Version,
				}); err != nil {
					return err
				}
			}
		case pb.Operation_DELETE:
			_, err := s.kv.Delete(ent.Key, 0)
			if err != nil {
				if err := server.Send(&pb.BackupReply{
					Status:  pb.Status_EFAILED,
					Version: ent.Version,
				}); err != nil {
					return err
				}
			} else {
				if version < ent.Version {
					version = ent.Version
				}
				if err := server.Send(&pb.BackupReply{
					Status:  pb.Status_OK,
					Version: ent.Version,
				}); err != nil {
					return err
				}
			}
		}
	}
}

func NewPrimaryServer(hostname string, port uint16, filePath string, id common.WorkerId) (*PrimaryServer, error) {
	kv, err := NewKVStore(filePath)
	if err != nil {
		return nil, err
	}
	return &PrimaryServer{
		Hostname:  hostname,
		Port:      port,
		FilePath:  filePath,
		Id:        id,
		kv:        kv,
		config:    common.WorkerConfig{},
		backupCh:  make(chan *pb.BackupEntry),
		backups:   make(map[string]*SyncRoutine),
		condition: sync.NewCond(&sync.Mutex{}),
	}, nil
}

// sync latest entries.
func (s *PrimaryServer) syncEntry(entry *pb.BackupEntry) {
	v := entry.Version
	s.backupCh <- entry
	s.condition.L.Lock()
	for s.version < v {
		s.condition.Wait()
	}
	s.condition.L.Unlock()
}

// broadcast backup entry to every sync routine, return when all of them are ready.
func (s *PrimaryServer) DoSync() {
	for {
		entry := <-s.backupCh
		s.lock.RLock()
		// sync all backups and migrations
		for _, routine := range s.backups {
			if routine.Mask(entry.Key) {
				routine.EntryCh <- entry
			}
		}
		for _, routine := range s.migrations {
			if routine.Mask(entry.Key) {
				routine.EntryCh <- entry
			}
		}
		s.lock.RUnlock()
		for _, routine := range s.backups {
			if !routine.Syncing {
				continue
			}
			routine.Condition.L.Lock()
			for routine.Version < entry.Version {
				routine.Condition.Wait()
			}
			routine.Condition.L.Unlock()
		}
		for _, routine := range s.migrations {
			if !routine.Syncing {
				continue
			}
			routine.Condition.L.Lock()
			for routine.Version < entry.Version {
				routine.Condition.Wait()
			}
			routine.Condition.L.Unlock()
		}
		s.condition.L.Lock()
		s.version = entry.Version
		s.condition.L.Unlock()
		s.condition.Broadcast()
	}
}

func (s *PrimaryServer) Put(_ context.Context, pair *pb.KVPair) (*pb.PutResponse, error) {
	version, err := s.kv.Put(pair.Key, pair.Value, 0)
	if err != nil {
		return &pb.PutResponse{Status: pb.Status_ENOENT}, nil
	}
	ent := pb.BackupEntry{
		Op:      pb.Operation_PUT,
		Key:     pair.Key,
		Value:   pair.Value,
		Version: version,
	}
	s.syncEntry(&ent)
	s.kv.Flush()
	return &pb.PutResponse{Status: pb.Status_OK}, nil
}

func (s *PrimaryServer) Get(_ context.Context, key *pb.Key) (*pb.GetResponse, error) {
	value, err := s.kv.Get(key.Key, 0)
	if err == nil {
		return &pb.GetResponse{
			Status: pb.Status_OK,
			Value:  value,
		}, nil
	} else {
		return &pb.GetResponse{
			Status: pb.Status_ENOENT,
			Value:  "",
		}, nil
	}
}

func (s *PrimaryServer) Delete(_ context.Context, key *pb.Key) (*pb.DeleteResponse, error) {
	if _, err := s.kv.Get(key.Key, 0); err != nil {
		return &pb.DeleteResponse{Status: pb.Status_ENOENT}, nil
	}
	version, err := s.kv.Delete(key.Key, 0)
	if err != nil {
		return &pb.DeleteResponse{Status: pb.Status_ENOENT}, nil
	}
	ent := pb.BackupEntry{
		Op:      pb.Operation_DELETE,
		Key:     key.Key,
		Version: version,
	}
	s.syncEntry(&ent)
	s.kv.Flush()
	return &pb.DeleteResponse{Status: pb.Status_OK}, nil
}

func (s *PrimaryServer) Checkpoint(_ context.Context, _ *empty.Empty) (*pb.FlushResponse, error) {
	if err := s.kv.Checkpoint(); err != nil {
		common.Log().Error("KV flush failed.", zap.Error(err))
		return &pb.FlushResponse{Status: pb.Status_EFAILED}, nil
	}
	return &pb.FlushResponse{Status: pb.Status_OK}, nil
}

func (s *PrimaryServer) RegisterToZk(conn *zk.Conn, weight float32, numBackups int) error {
	log := common.SugaredLog()
	s.conn = conn
	ops := make([]interface{}, 0)
	// first ensure the worker path exist
	p := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)))
	configPath := path.Join(p, common.ZK_WORKER_CONFIG_NAME)
	exists, _, err := s.conn.Exists(p)
	if err != nil {
		return err
	}
	// get version id
	v, _, err := s.conn.Get(path.Join(common.ZK_ROOT, common.ZK_VERSION_NAME))
	if err != nil {
		return err
	}
	version, err := strconv.Atoi(string(v))
	s.SlotTableVersion = uint32(version)
	if err != nil {
		return err
	}
	if !exists {
		// apparently it is a new worker, add all of them atomically
		ops = append(ops, &zk.CreateRequest{
			Path:  p,
			Data:  []byte(""),
			Acl:   zk.WorldACL(zk.PermAll),
			Flags: 0,
		})
		config := common.WorkerConfig{
			Weight:     weight,
			NumBackups: numBackups,
		}
		bin, err := json.Marshal(config)
		if err != nil {
			return err
		}
		ops = append(ops, &zk.CreateRequest{
			Path:  configPath,
			Data:  bin,
			Acl:   zk.WorldACL(zk.PermAll),
			Flags: 0,
		})
		// and register myself
		node := common.NewWorkerNode(s.Hostname, s.Port, s.Id)
		bin, err = json.Marshal(node)
		if err != nil {
			return err
		}
		ops = append(ops, &zk.CreateRequest{
			Path:  path.Join(p, common.ZK_PRIMARY_WORKER_NAME),
			Data:  bin,
			Acl:   zk.WorldACL(zk.PermAll),
			Flags: zk.FlagEphemeral | zk.FlagSequence,
		})
		resps, err := s.conn.Multi(ops...)
		if err != nil {
			log.Error("Failed to issue multiple requests.", zap.Error(err))
			for _, resp := range resps {
				if resp.Error != nil {
					return resp.Error
				}
			}
		}
	} else {
		// update configuration
		bin, _, err := s.conn.Get(configPath)
		if err != nil {
			return err
		}
		var c common.WorkerConfig
		if err := json.Unmarshal(bin, &c); err != nil {
			return err
		}
		log.Infof("Retrieved configuration from zookeeper: %+v", c)
		s.config = c
		// register itself
		node := common.NewWorkerNode(s.Hostname, s.Port, s.Id)
		bin, err = json.Marshal(node)
		if err != nil {
			return err
		}
		_, err = s.conn.Create(path.Join(p, common.ZK_PRIMARY_WORKER_NAME), bin,
			zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	log.Info("Configuration initialized & registered to zookeeper", zap.Uint16("id", uint16(s.Id)))
	return nil
}

func (s *PrimaryServer) Watch(stopChan <-chan struct{}) {
	log := common.Log()
	log.Info("Starting to watch worker nodes...", zap.Int("id", int(s.Id)))
	p := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)))
	for {
		children, _, eventChan, err := s.conn.ChildrenW(p)
		if err != nil {
			log.Error("Failed to watch.", zap.String("path", p), zap.Error(err))
			break
		}
		// get backups
		backups := common.FilterString(children, func(i int) bool {
			return strings.Contains(children[i], common.ZK_BACKUP_WORKER_NAME)
		})
		sort.Strings(backups)
		// check the backups and make sure connection with sync backups are intact
		if len(backups) < s.config.NumBackups {
			if err := s.SyncBackups(backups); err != nil {
				log.Error("Failed to sync backups.", zap.Error(err))
				return
			}
		} else {
			if err := s.SyncBackups(backups[:s.config.NumBackups]); err != nil {
				log.Error("Failed to sync backups.", zap.Error(err))
				return
			}
		}
		select {
		case <-eventChan:
			log.Info("Event captured.")
		case <-stopChan:
			break
		}
	}
}

// sync current backup routines with new backup array
// outdated backup nodes will be closed and removed, new backups will be connected and added
func (s *PrimaryServer) SyncBackups(newBackups []string) error {
	b := make(map[string]bool)
	for _, back := range newBackups {
		b[back] = true
	}
	// check lost backups
	for name := range s.backups {
		if _, ok := b[name]; !ok {
			if err := s.RemoveBackupRoutine(name); err != nil {
				return err
			}
		}
	}
	// check new backups
	for name := range b {
		if _, ok := s.backups[name]; !ok {
			if err := s.AddBackupRoutine(name); err != nil {
				return err
			}
		}
	}
	return nil
}

// watch for migration stuff...
func (s *PrimaryServer) WatchMigration(stopChan <-chan struct{}) {
	log := common.Log()
watchLoop:
	for {
		p := path.Join(common.ZK_MIGRATIONS_ROOT, strconv.Itoa(int(s.SlotTableVersion)), strconv.Itoa(int(s.Id)))
		log.Info("Watching migration node...", zap.String("path", p))
		exists, _, eventChan, err := s.conn.ExistsW(p)
		if err != nil {
			log.Error("Failed to watch migration node.", zap.Error(err))
			return
		}
		if exists {
			log.Info("Migration detected...", zap.Int("version", int(s.SlotTableVersion)))
			if err := s.doMigration(stopChan); err != nil {
				log.Error("Failed to do migration.", zap.Error(err))
			}
		} else {
			// keep waiting
			select {
			case <-eventChan:
				continue
			case <-stopChan:
				break watchLoop
			}
		}
	}
}

// actual migration routine
func (s *PrimaryServer) doMigration(stopChan <-chan struct{}) error {
	log := common.Log()
	p := path.Join(common.ZK_MIGRATIONS_ROOT, strconv.Itoa(int(s.SlotTableVersion)), strconv.Itoa(int(s.Id)))
	// fetch migration content
	bin, _, err := s.conn.Get(p)
	if err != nil {
		return err
	}
	var migration common.SingleNodeMigration
	if err := json.Unmarshal(bin, &migration); err != nil {
		return err
	}
	// collect destinations
	destinations := make(map[common.WorkerId]bool)
	for _, dst := range migration.Table {
		destinations[dst] = true
	}
	wg := sync.WaitGroup{}
	wg.Add(len(destinations))
	for dst := range destinations {
		dst := dst // capture loop variable
		go func() {
			log.Info("Migration started.", zap.Int("dest", int(dst)))
			// get primary of the destination
			bin, _, err := s.conn.Get(path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(dst))))
			if err != nil {
				log.Error("Failed to get worker data.", zap.Int("id", int(dst)), zap.Error(err))
			}
			var worker common.Worker
			_ = json.Unmarshal(bin, &worker) // IGNORED

			// connect through gRPC
			connString := fmt.Sprintf("%s:%d", worker.Primary.Host.Hostname, worker.Primary.Host.Port)
			conn, err := common.ConnectGrpc(connString)
			if err != nil {
				log.Error("Failed to connect to destination.", zap.Error(err))
			}
			client := pb.NewKVBackupClient(conn)
			// mask
			mask := func(key string) bool {
				return migration.GetDestId(key) == dst
			}
			// register replication strategy
			routine := SyncRoutine{
				name:    worker.PrimaryName,
				conn:    client,
				Mask:    mask,
				Syncing: false,
				EntryCh: make(chan *pb.BackupEntry),
				StopCh:  make(chan struct{}),
			}
			// register it to start recording new requests that should be broadcasted
			s.migrations[worker.PrimaryName] = &routine
			log.Sugar().Infof("Sync routine for %d registered.", dst)
			// keep preparing until success
			for {
				extracted := s.kv.Extract(mask, 0)
				if err := routine.Prepare(extracted); err == nil {
					break
				}
			}
			// make it sync
			routine.Syncing = true
			wg.Done()
			routine.Sync()
		}()
	}
	wg.Wait()
	log.Info("Migration: all sync.")
	// all migration targets are in sync
	// reduce semaphore by one
	semPath := path.Join(common.ZK_MIGRATIONS_ROOT, strconv.Itoa(int(s.SlotTableVersion)), common.ZK_COMPLETE_SEM_NAME)
	sem := common.DistributedAtomicInteger{
		Conn: s.conn,
		Path: semPath,
	}
	if _, err := sem.Dec(); err != nil {
		return err
	}
	// wait for version number to change
waitLoop:
	for {
		bin, _, eventChan, err := s.conn.GetW(path.Join(common.ZK_ROOT, common.ZK_VERSION_NAME))
		if err != nil {
			return err
		}
		// get version
		val, err := strconv.Atoi(string(bin))
		v := uint32(val)
		if err != nil {
			return err
		}
		if v == s.SlotTableVersion+1 {
			// ok
			log.Info("Migration completed, changing local migration version number.")
			s.SlotTableVersion += 1
			break
		}
		select {
		case <-eventChan:
			continue
		case <-stopChan:
			break waitLoop
		}
	}
	return nil
}

func (s *PrimaryServer) AddBackupRoutine(backupNodeName string) error {
	bin, _, err := s.conn.Get(path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)), backupNodeName))
	if err != nil {
		return err
	}
	var node common.WorkerNode
	if err := json.Unmarshal(bin, &node); err != nil {
		return err
	}
	connString := fmt.Sprintf("%s:%d", node.Host.Hostname, node.Host.Port)
	grpcConn, err := common.ConnectGrpc(connString)
	if err != nil {
		return err
	}
	routine := NewSyncRoutine(backupNodeName, pb.NewKVBackupClient(grpcConn), func(_ string) bool { return true })
	go func() {
		for {
			content := s.kv.Extract(func(_ string) bool { return true }, 0)
			if err := routine.Prepare(content); err == nil {
				break
			}
		}
		routine.Syncing = true
		routine.Sync()
	}()
	s.lock.Lock()
	s.backups[backupNodeName] = &routine
	s.lock.Unlock()
	return nil
}

func (s *PrimaryServer) RemoveBackupRoutine(backupNodeName string) error {
	routine, ok := s.backups[backupNodeName]
	if !ok {
		return errors.New("backup node name does not exist")
	}
	close(routine.StopCh)
	s.lock.Lock()
	delete(s.backups, backupNodeName)
	s.lock.Unlock()
	return nil
}
