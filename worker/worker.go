package worker

// primary/backup control plane

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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MODE_PRIMARY = "Primary"
	MODE_BACKUP  = "Backup"
)

type WorkerServer struct {
	pb.UnimplementedKVWorkerServer
	Hostname         string
	Port             uint16
	FilePath         string
	Id               common.WorkerId
	conn             *zk.Conn
	kv               KVStore
	SlotTableVersion uint32
	NodeName         string
	config           common.WorkerConfig
	backupCh         chan *pb.BackupEntry
	lock             sync.RWMutex
	backups          map[string]*SyncRoutine
	backupVersion    uint64
	backupCond       *sync.Cond
	migrations       map[string]*SyncRoutine
	mode             string
	version          uint64
	versionCond      *sync.Cond
	modeChangeCond   *sync.Cond

	// three goroutines: watch workers, watch migration, do sync.
	// watch workers & watch migration should be updated once mode changes
	WatchWorkerStopChan    chan struct{}
	WatchMigrationStopChan chan struct{}
	SyncStopChan           chan struct{}

	// original mode, speicified when being first run.
	// this help determine whether it should step down as the temporary primary
	origMode string
}

// the following two are for migration.

// Transfer bunch of data with transaction
func (s *WorkerServer) Transfer(server pb.KVBackup_TransferServer) error {
	// start a new transaction
	tid, err := s.kv.StartTransaction()
	log := common.Log()
	var version uint64 = 0
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
				log.Error("Failed to commit", zap.Error(err))
				return server.SendAndClose(&pb.BackupReply{
					Status:  pb.Status_EFAILED,
					Version: version,
				})
			} else {
				log.Info("Successfully committed")
				s.versionCond.L.Lock()
				s.version = version
				s.versionCond.L.Unlock()
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
			if version < ent.Version {
				version = ent.Version
			}
		case pb.Operation_DELETE:
			_, err := s.kv.Delete(ent.Key, tid)
			if err != nil {
				goto fail
			}
			if version < ent.Version {
				version = ent.Version
			}
		default:
			goto fail
		}
		continue
	fail:
		log.Error("Failed to operate", zap.Error(err))
		return server.SendAndClose(&pb.BackupReply{
			Status:  pb.Status_EFAILED,
			Version: version,
		})
	}
}

// Transfer data piece by piece in a loss less manner
func (s *WorkerServer) Sync(server pb.KVBackup_SyncServer) error {
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

func NewPrimaryServer(hostname string, port uint16, filePath string, id common.WorkerId) (*WorkerServer, error) {
	kv, err := NewKVStore(filePath)
	if err != nil {
		return nil, err
	}
	return &WorkerServer{
		Hostname:               hostname,
		Port:                   port,
		FilePath:               filePath,
		Id:                     id,
		kv:                     kv,
		mode:                   MODE_PRIMARY,
		origMode:               MODE_PRIMARY,
		lock:                   sync.RWMutex{},
		backups:                make(map[string]*SyncRoutine),
		backupCh:               make(chan *pb.BackupEntry),
		migrations:             make(map[string]*SyncRoutine),
		versionCond:            sync.NewCond(&sync.Mutex{}),
		modeChangeCond:         sync.NewCond(&sync.Mutex{}),
		backupCond:             sync.NewCond(&sync.Mutex{}),
		WatchMigrationStopChan: make(chan struct{}),
		WatchWorkerStopChan:    make(chan struct{}),
		SyncStopChan:           make(chan struct{}),
	}, nil
}

func NewBackupServer(hostname string, port uint16, filePath string, id common.WorkerId) (*WorkerServer, error) {
	kv, err := NewKVStore(filePath)
	if err != nil {
		return nil, err
	}
	return &WorkerServer{
		Hostname:               hostname,
		Port:                   port,
		FilePath:               filePath,
		Id:                     id,
		kv:                     kv,
		mode:                   MODE_BACKUP,
		origMode:               MODE_BACKUP,
		lock:                   sync.RWMutex{},
		backups:                make(map[string]*SyncRoutine),
		backupCh:               make(chan *pb.BackupEntry),
		migrations:             make(map[string]*SyncRoutine),
		versionCond:            sync.NewCond(&sync.Mutex{}),
		modeChangeCond:         sync.NewCond(&sync.Mutex{}),
		backupCond:             sync.NewCond(&sync.Mutex{}),
		WatchMigrationStopChan: make(chan struct{}),
		WatchWorkerStopChan:    make(chan struct{}),
		SyncStopChan:           make(chan struct{}),
	}, nil
}

// sync latest entries.
func (s *WorkerServer) syncEntry(entry *pb.BackupEntry) {
	v := entry.Version
	s.backupCh <- entry
	common.SugaredLog().Infof("SENDED TO SYNC REMOTELY!")
	s.versionCond.L.Lock()
	for s.version < v {
		s.versionCond.Wait()
	}
	s.versionCond.L.Unlock()
}

// broadcast backup entry to every sync routine, return when all of them are ready.
// Does not matter if it runs under primary or backup
func (s *WorkerServer) DoSync() {
	log := common.SugaredLog()
	log.Infof("DOSYNC is up and running!")
	for {
		select {
		case entry := <-s.backupCh:
			log.Infof("WRITING TO BACKUPS AND MIGRATIONS!")
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

			log.Infof("WAITING FOR BACKUPS!")
			// backup use semi-sync transfer, which means
			// only one of them have to ack before going on.
			if len(s.backups) > 0 {
				s.backupCond.L.Lock()
				for {
					// update s.version
					for _, routine := range s.backups {
						if routine.Version > s.backupVersion {
							s.backupVersion = routine.Version
						}
					}
					if s.backupVersion < entry.Version {
						s.backupCond.Wait()
					} else {
						s.backupCond.L.Unlock()
						break
					}
				}
			}

			log.Infof("WAITING FOR MIGRATIONS!")
			// migration use loss less transfer, which means each migration should complete
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
			s.versionCond.L.Lock()
			s.version = entry.Version
			s.versionCond.L.Unlock()
			s.versionCond.Broadcast()
		case <-s.SyncStopChan:
			return
		}
	}
}

func (s *WorkerServer) Put(_ context.Context, pair *pb.KVPair) (*pb.PutResponse, error) {
	if s.mode != MODE_PRIMARY {
		return &pb.PutResponse{Status: pb.Status_ENOSERVER}, nil
	}
	log := common.SugaredLog()
	version, err := s.kv.Put(pair.Key, pair.Value, 0)
	if err != nil {
		return &pb.PutResponse{Status: pb.Status_ENOENT}, nil
	}
	log.Infof("%s->%s WRITTEN LOCALLY!", pair.Key, pair.Value)
	ent := pb.BackupEntry{
		Op:      pb.Operation_PUT,
		Key:     pair.Key,
		Value:   pair.Value,
		Version: version,
	}
	s.syncEntry(&ent)
	log.Infof("SYNCED REMOTELY!")
	s.kv.Flush()
	return &pb.PutResponse{Status: pb.Status_OK}, nil
}

func (s *WorkerServer) Get(_ context.Context, key *pb.Key) (*pb.GetResponse, error) {
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

func (s *WorkerServer) Delete(_ context.Context, key *pb.Key) (*pb.DeleteResponse, error) {
	if s.mode != MODE_PRIMARY {
		return &pb.DeleteResponse{Status: pb.Status_EINVSERVER}, nil
	}
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

func (s *WorkerServer) Checkpoint(_ context.Context, _ *empty.Empty) (*pb.FlushResponse, error) {
	if s.mode != MODE_PRIMARY {
		return &pb.FlushResponse{Status: pb.Status_EINVSERVER}, nil
	}
	if err := s.kv.Checkpoint(); err != nil {
		common.Log().Error("KV flush failed.", zap.Error(err))
		return &pb.FlushResponse{Status: pb.Status_EFAILED}, nil
	}
	return &pb.FlushResponse{Status: pb.Status_OK}, nil
}

func (s *WorkerServer) RegisterToZk(conn *zk.Conn, weight float32) error {
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
		if s.mode == MODE_BACKUP {
			return errors.New("worker does not exist in zookeeper yet")
		}
		// apparently it is a new worker, add all of them atomically
		ops = append(ops, &zk.CreateRequest{
			Path:  p,
			Data:  []byte(""),
			Acl:   zk.WorldACL(zk.PermAll),
			Flags: 0,
		})
		config := common.WorkerConfig{
			Weight: weight,
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
		// get from the last resp
		fullName := resps[len(resps)-1].String
		s.NodeName = path.Base(fullName)
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
		var nodePath string
		if s.mode == MODE_PRIMARY {
			nodePath = path.Join(p, common.ZK_PRIMARY_WORKER_NAME)
		} else {
			nodePath = path.Join(p, common.ZK_BACKUP_WORKER_NAME)
		}
		fullName, err := s.conn.Create(nodePath, bin, zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
		s.NodeName = path.Base(fullName)
	}
	log.Info("Configuration initialized & registered to zookeeper", zap.Uint16("id", uint16(s.Id)))
	return nil
}

// watch other nodes belonging to the same worker,
// both primary and backup should do this
func (s *WorkerServer) Watch(stopChan chan struct{}) {
	log := common.Log()
	log.Info("Starting to watch worker nodes...", zap.Int("id", int(s.Id)))
	p := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)))
	for {
		children, _, eventChan, err := s.conn.ChildrenW(p)
		if err != nil {
			log.Error("Failed to watch.", zap.String("path", p), zap.Error(err))
			break
		}
		if s.mode == MODE_PRIMARY {
			// get primaries
			primaries := common.FilterString(children, func(i int) bool {
				return strings.Contains(children[i], common.ZK_PRIMARY_WORKER_NAME)
			})
			// get backups
			backups := common.FilterString(children, func(i int) bool {
				return strings.Contains(children[i], common.ZK_BACKUP_WORKER_NAME)
			})
			// check the backups and make sure connection with sync backups are intact
			for _, b := range backups {
				if _, ok := s.backups[b]; !ok {
					if err := s.AddBackupRoutine(b); err != nil {
						log.Error("Failed to connect to backup, retry in the next cycle.", zap.Error(err))
					}
				}
			}
			for b := range s.backups {
				for _, b1 := range backups {
					if b == b1 {
						goto found
					}
				}
				// not found
				if err := s.RemoveBackupRoutine(b); err != nil {
					log.Error("Failed to remove backup routine, retry in the next cycle.", zap.Error(err))
				}
			found:
			}
			// now check primaries
			sort.Strings(primaries)
			if len(primaries) > 1 && s.origMode != MODE_PRIMARY {
				var newPrim string
				for _, p := range primaries {
					if p != s.NodeName {
						newPrim = p
						break
					}
				}
				go func() {
					log.Info("Migrating to new primary.", zap.String("nodeName", newPrim))
					bin, _, err := s.conn.Get(path.Join(p, newPrim))
					if err != nil {
						log.Error("Failed to get new primary's metadata.", zap.Error(err))
					}
					var n common.WorkerNode
					if err := json.Unmarshal(bin, &n); err != nil {
						log.Error("Failed to unmarshal data.", zap.Error(err))
					}
					// connect through gRPC
					connString := fmt.Sprintf("%s:%d", n.Host.Hostname, n.Host.Port)
					conn, err := common.ConnectGrpc(connString)
					if err != nil {
						log.Error("Failed to connect to destination.", zap.Error(err))
					}
					client := pb.NewKVBackupClient(conn)
					// mask
					mask := func(key string) bool { return true }
					// register replication strategy
					routine := SyncRoutine{
						name:    newPrim,
						conn:    client,
						Mask:    mask,
						Syncing: false,
						EntryCh: make(chan *pb.BackupEntry),
						StopCh:  make(chan struct{}),
					}
					// register it to start recording new requests that should be broadcasted
					s.migrations[newPrim] = &routine
					log.Sugar().Infof("Sync routine for %s registered.", newPrim)
					// keep preparing until success
					for {
						extracted := s.kv.Extract(mask, 0)
						if err := routine.Prepare(extracted); err == nil {
							break
						}
						println(reflect.TypeOf(err))
						log.Error("Preparation failed, retrying...", zap.Error(err))
						time.Sleep(2 * time.Second)
					}
					// make it sync
					routine.Syncing = true
					routine.Sync()
					time.Sleep(1 * time.Second)
					// now it's time to step down
					if err := s.transformTo(MODE_BACKUP); err != nil {
						log.Error("Failed to step down.", zap.Error(err))
					}
				}()
			}
		} else {
			// backup logic
			// check worker exist or not and begin election process
			primaries := common.FilterString(children, func(i int) bool {
				return strings.Contains(children[i], common.ZK_PRIMARY_WORKER_NAME)
			})
			if len(primaries) == 0 {
				log.Info("Primary down detected, beginning new primaries election...")
				// worker down, begin election
				if err := s.BackupElection(); err != nil {
					log.Error("Failed to perform election.", zap.Error(err))
				}
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

func (s *WorkerServer) BackupElection() error {
	log := common.SugaredLog()
	electionPath := path.Join(common.ZK_ELECTION_ROOT, strconv.Itoa(int(s.Id)))
	if err := common.EnsurePath(s.conn, electionPath); err != nil {
		log.Error("Failed to ensure path.", zap.Error(err))
		return err
	}
	// register itself and watch
	myName := fmt.Sprintf("%020d%s", s.version, s.NodeName)
	log.Infof("Name to assign: %s\n", myName)
	_, err := s.conn.Create(path.Join(electionPath, myName), []byte(s.NodeName), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Error("Failed to create node.", zap.Error(err))
		return err
	}
	// get number of backup nodes
	workerPath := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)))
	workers, _, err := s.conn.Children(workerPath)
	if err != nil {
		return err
	}
	backups := common.FilterString(workers, func(i int) bool { return strings.Contains(workers[i], common.ZK_BACKUP_WORKER_NAME) })
	// watch the znode
	log.Infof("Should have %d backups.", len(backups))
	for {
		children, _, eventChan, err := s.conn.ChildrenW(electionPath)
		if err != nil {
			log.Error("Failed to watch election path", zap.Error(err))
			return nil
		}
		if len(children) == len(backups) {
			sort.Strings(children)
			if children[0] == myName {
				// i am the lucky guy
				log.Infof("Seems like I am the lucky guy, upgrading...")
				if err := s.transformTo(MODE_PRIMARY); err != nil {
					log.Error("Transformation failed.", zap.Error(err))
					return err
				}
				log.Infof("Transformation successful.")
			} else {
				log.Infof("Not so lucky this time...")
			}
			break
		}
		_ = <-eventChan
	}
	return nil
}

func (s *WorkerServer) transformTo(mode string) error {
	if mode != MODE_PRIMARY && mode != MODE_BACKUP {
		return errors.New("invalid mode")
	}
	if mode == s.mode {
		return errors.New("no need for transformation")
	}
	// re-register itself in zookeeper
	log := common.Log()
	n := common.NewWorkerNode(s.Hostname, s.Port, s.Id)
	bin, err := json.Marshal(n)
	if err != nil {
		return err
	}
	parentPath := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)))
	var nodePath string
	if mode == MODE_PRIMARY {
		nodePath = path.Join(parentPath, common.ZK_PRIMARY_WORKER_NAME)
	} else {
		nodePath = path.Join(parentPath, common.ZK_BACKUP_WORKER_NAME)
	}
	log.Sugar().Infof("original path is %s", path.Join(parentPath, s.NodeName))
	log.Sugar().Infof("new node path is %s", nodePath)
	if err := s.conn.Delete(path.Join(parentPath, s.NodeName), -1); err != nil {
		return err
	}
	name, err := s.conn.Create(nodePath, bin, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	s.NodeName = path.Base(name)
	// successful, now restart goroutines
	log.Info("Restarting goroutines...")
	s.mode = mode
	close(s.WatchMigrationStopChan)
	close(s.WatchWorkerStopChan)
	s.WatchMigrationStopChan = make(chan struct{})
	s.WatchWorkerStopChan = make(chan struct{})
	go s.Watch(s.WatchWorkerStopChan)
	go s.WatchMigration(s.WatchWorkerStopChan)
	return nil
}

// sync current backup routines with new backup array
// outdated backup nodes will be closed and removed, new backups will be connected and added
func (s *WorkerServer) SyncBackups(newBackups []string) error {
	if s.mode != MODE_PRIMARY {
		return nil
	}
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
func (s *WorkerServer) WatchMigration(stopChan chan struct{}) {
	common.Log().Info("Watching migration...")
	if s.mode == MODE_BACKUP {
		// do nothing in backup mode
		<-s.WatchMigrationStopChan
		return
	}
	log := common.Log()
watchLoop:
	for {
		p := path.Join(common.ZK_MIGRATIONS_ROOT, strconv.Itoa(int(s.SlotTableVersion)))
		log.Info("Watching migration node...", zap.String("path", p))
		exists, _, eventChan, err := s.conn.ExistsW(p)
		if err != nil {
			log.Error("Failed to watch migration node.", zap.Error(err))
			return
		}
		if exists {
			log.Info("Migration detected...", zap.Int("version", int(s.SlotTableVersion)))
			if err := s.doMigration(); err != nil {
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

// worker's migration logic
func (s *WorkerServer) doMigration() error {
	if s.mode != MODE_PRIMARY {
		return nil
	}
	log := common.Log()
	p := path.Join(common.ZK_MIGRATIONS_ROOT, strconv.Itoa(int(s.SlotTableVersion)), strconv.Itoa(int(s.Id)))
	// fetch migration content
	bin, _, err := s.conn.Get(p)
	if err == zk.ErrNoNode {
		// nothing to do
		log.Info("Migration complete, nothing to do.")
		s.SlotTableVersion += 1
		return nil
	} else if err != nil {
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
			// get worker of the destination
			worker, err := common.GetWorker(s.conn, dst)
			if err != nil {
				log.Error("Failed to get worker data.", zap.Int("id", int(dst)), zap.Error(err))
			}
			// connect through gRPC
			connString := fmt.Sprintf("%s:%d", worker.Primary.Host.Hostname, worker.Primary.Host.Port)
			conn, err := common.ConnectGrpc(connString)
			if err != nil {
				log.Error("Failed to connect to destination.", zap.Error(err))
			}
			client := pb.NewKVBackupClient(conn)
			// mask
			mask := func(key string) bool {
				return migration.GetDestWorkerId(key) == dst
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
				println(reflect.TypeOf(err))
				log.Error("Preparation failed, retrying...", zap.Error(err))
				time.Sleep(2 * time.Second)
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
			log.Info("Migration completed, changing local version number and close all migration connections.")
			s.SlotTableVersion += 1
			for _, routine := range s.migrations {
				close(routine.StopCh)
			}
			s.migrations = make(map[string]*SyncRoutine) // clear
			break
		}
		select {
		case <-eventChan:
			continue
		}
	}
	return nil
}

func (s *WorkerServer) AddBackupRoutine(backupNodeName string) error {
	log := common.SugaredLog()
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
	routine := NewSyncRoutine(backupNodeName, pb.NewKVBackupClient(grpcConn), func(_ string) bool { return true }, s.backupCond)
	s.lock.Lock()
	s.backups[backupNodeName] = &routine
	s.lock.Unlock()
	go func() {
		log.Infof("Backing up with %s", backupNodeName)
		for {
			content := s.kv.Extract(func(_ string) bool { return true }, 0)
			if err := routine.Prepare(content); err == nil {
				break
			}
			log.Infof("Backing up with %s failed, retrying...", backupNodeName)
			time.Sleep(2 * time.Second)
		}
		routine.Syncing = true
		routine.Sync()
	}()
	return nil
}

func (s *WorkerServer) RemoveBackupRoutine(backupNodeName string) error {
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
