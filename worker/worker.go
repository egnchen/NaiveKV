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
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type PrimaryServer struct {
	pb.UnimplementedKVWorkerServer
	Hostname   string
	Port       uint16
	FilePath   string
	Id         common.WorkerId
	conn       *zk.Conn
	kv         KVStore
	config     common.WorkerConfig
	backupCh   chan *pb.BackupEntry
	lock       sync.RWMutex
	backups    map[string]*SyncRoutine
	migrations map[string]*SyncRoutine
	condition  sync.Cond
	version    uint64
}

func NewPrimaryServer(hostname string, port uint16, filePath string, id common.WorkerId) (*PrimaryServer, error) {
	kv, err := NewKVStore(filePath)
	if err != nil {
		return nil, err
	}
	return &PrimaryServer{
		Hostname: hostname,
		Port:     port,
		FilePath: filePath,
		Id:       id,
		kv:       kv,
		config:   common.WorkerConfig{},
		backupCh: make(chan *pb.BackupEntry),
		backups:  make(map[string]*SyncRoutine),
	}, nil
}

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
// TODO conditional variable broadcasting here has low performance
func (s *PrimaryServer) DoSync() {
	for {
		entry := <-s.backupCh
		s.lock.RLock()
		// lossless sync with backup
		for _, routine := range s.backups {
			if routine.Mask(entry.Key) {
				routine.EntryCh <- entry
			}
		}
		// async with migration targets
		for _, routine := range s.migrations {
			if routine.Mask(entry.Key) {
				routine.EntryCh <- entry
			}
		}
		s.lock.RUnlock()
		// wait for all backups to complete
		for _, routine := range s.backups {
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

// Update configuration. Read configuration from zookeeper if configuration exists,
// add a new configuration which given parameters if not.
func (s *PrimaryServer) updateConfig(weight float32, numBackups int) error {
	// get primary id & configuration
	log := common.SugaredLog()
	configPath := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)), common.ZK_WORKER_CONFIG_NAME)
	bin, _, err := s.conn.Get(configPath)
	if err != nil && err != zk.ErrNoNode {
		return err
	} else if err == zk.ErrNoNode {
		// does not exist before, create new config node
		c := common.WorkerConfig{
			Version:    0,
			Weight:     weight,
			NumBackups: numBackups,
		}
		cb, _ := json.Marshal(c)
		_, err := s.conn.Create(configPath, cb, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
		s.config = c
		log.Infof("Updated configuration to %+v", c)
	} else {
		// have configuration before
		var c common.WorkerConfig
		if err := json.Unmarshal(bin, &c); err != nil {
			return err
		}
		log.Infof("Retrieved configuration from zookeeper: %+v", c)
		s.config = c
	}
	return nil
}

func (s *PrimaryServer) RegisterToZk(conn *zk.Conn, weight float32, numBackups int) error {
	log := common.Log()
	s.conn = conn

	// first ensure the worker path exist
	p := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)))
	if err := common.EnsurePath(s.conn, p); err != nil {
		log.Error("Failed to create worker znode.", zap.Error(err))
		return err
	}

	// update configuration
	if err := s.updateConfig(weight, numBackups); err != nil {
		log.Warn("Failed to update configuration.", zap.Error(err))
		return err
	}
	log.Info("Initialized configuration", zap.Uint16("id", uint16(s.Id)))

	// register itself
	node := common.NewWorkerNode(s.Hostname, s.Port, s.Id)
	bin, _ := json.Marshal(node)
	_, err := s.conn.Create(path.Join(p, common.ZK_PRIMARY_WORKER_NAME), bin, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
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
		if err := s.SyncBackups(backups[:s.config.NumBackups]); err != nil {
			log.Error("Failed to sync backups.", zap.Error(err))
			return
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
		p := path.Join(common.ZK_MIGRATIONS_ROOT, strconv.Itoa(int(s.config.Version)), strconv.Itoa(int(s.Id)))
		log.Info("Watching migration node...", zap.String("path", p))
		exists, _, eventChan, err := s.conn.ExistsW(p)
		if err != nil {
			log.Error("Failed to watch migration node.", zap.Error(err))
			return
		}
		if exists {
			// do migration
			// fetch migration content
			bin, _, err := s.conn.Get(p)
			if err != nil {
				log.Error("Failed to get migration content.")
				return
			}
			var migration common.SingleNodeMigration
			if err := json.Unmarshal(bin, &migration); err != nil {
				log.Error("Invalid migration content.", zap.ByteString("content", bin), zap.Error(err))
				return
			}
			// collect destinations
			destinations := make(map[common.WorkerId]bool)
			for _, dst := range migration.Table {
				destinations[dst] = true
			}
			wg := sync.WaitGroup{}
			wg.Add(len(destinations))
			for dst := range destinations {
				dst := dst // capture Loop variable
				go func() {
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

					// register replication strategy
					routine := SyncRoutine{
						name:    worker.PrimaryName,
						conn:    client,
						EntryCh: make(chan *pb.BackupEntry),
						StopCh:  make(chan struct{}),
					}
					s.migrations[worker.PrimaryName] = &routine
					// make a mask
					extracted := s.kv.Extract(func(key string) bool {
						return migration.GetDestId(key) == dst
					})
					for k, v := range extracted {
						routine.EntryCh <- &pb.BackupEntry{
							Op:      pb.Operation_PUT,
							Version: 0, // does not matter here
							Key:     k,
							Value:   v,
						}
					}
				}()
			}
		}
		select {
		case <-eventChan:
			continue
		case <-stopChan:
			break watchLoop
		}
	}
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
	routine := SyncRoutine{
		conn:    pb.NewKVBackupClient(grpcConn),
		EntryCh: make(chan *pb.BackupEntry),
		name:    backupNodeName,
		StopCh:  make(chan struct{}),
	}
	go routine.Loop()
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
