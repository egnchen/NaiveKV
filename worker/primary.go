package worker

// primary control plane

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
	"strconv"
	"sync"
	"time"
)

// Transfer bunch of data with transaction
func (s *WorkerServer) Transfer(server pb.KVBackup_TransferServer) error {
	// There's a difference between "sync version number" and "async version number"
	// "Sync version number" is for sync between backups.
	// "Async version number" is for sync in migrations.

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

// sync latest entries.
func (s *WorkerServer) syncEntry(entry *pb.BackupEntry) {
	v := entry.Version
	s.backupCh <- entry
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
	log.Info("Sync goroutine is up and running...")
	for {
		select {
		case entry := <-s.backupCh:
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

func (s *WorkerServer) registerPrimary(weight float32) error {
	node := common.NewWorkerNode(s.Hostname, s.Port, s.Id)
	// primary have to ensure that worker path exists
	p := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)))
	nodePath := path.Join(p, common.ZK_PRIMARY_WORKER_NAME)
	exists, _, err := s.conn.Exists(p)
	if err != nil {
		return err
	}
	if !exists {
		// atomically add worker path, config znode and register itself
		bin, err := json.Marshal(node)
		if err != nil {
			return err
		}
		s.config.Weight = weight
		cBin, err := json.Marshal(s.config)
		if err != nil {
			return err
		}
		if _, err := common.ZkMulti(s.conn, &zk.CreateRequest{
			Path:  p,
			Data:  []byte(""),
			Acl:   zk.WorldACL(zk.PermAll),
			Flags: 0,
		}, &zk.CreateRequest{
			Path:  nodePath,
			Data:  bin,
			Acl:   zk.WorldACL(zk.PermAll),
			Flags: zk.FlagEphemeral | zk.FlagSequence,
		}, &zk.CreateRequest{
			Path:  path.Join(p, common.ZK_WORKER_CONFIG_NAME),
			Data:  cBin,
			Acl:   zk.WorldACL(zk.PermAll),
			Flags: 0,
		}); err != nil {
			return err
		}
	} else {
		// get configuration from zookeeper
		var config common.WorkerConfig
		if err := common.ZkGet(s.conn, path.Join(p, common.ZK_WORKER_CONFIG_NAME), &config); err != nil {
			return err
		}
		s.config = config
		// register itself
		if _, err := common.ZkCreate(s.conn, nodePath, node, true, true); err != nil {
			return err
		}
	}
	return nil
}

// Migrate to the "real" primary when I'm the replacement primary.
func (s *WorkerServer) migrateToNewPrimary(newPrimary string, node *common.WorkerNode) {
	log := common.SugaredLog()
	log.Info("Migrating to new primary.", zap.String("nodeName", newPrimary))
	// connect through gRPC
	connString := fmt.Sprintf("%s:%d", node.Host.Hostname, node.Host.Port)
	conn, err := common.ConnectGrpc(connString)
	if err != nil {
		log.Error("Failed to connect to destination.", zap.Error(err))
	}
	client := pb.NewKVBackupClient(conn)
	// mask
	mask := func(key string) bool { return true }
	// register replication strategy
	routine := SyncRoutine{
		name:    newPrimary,
		conn:    client,
		Mask:    mask,
		Syncing: false,
		EntryCh: make(chan *pb.BackupEntry),
		StopCh:  make(chan struct{}),
	}
	// register it to start recording new requests
	s.migrations[newPrimary] = &routine
	log.Infof("Sync routine for %s registered.", newPrimary)
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
}

func (s *WorkerServer) primaryWatch(worker common.Worker) {
	log := common.SugaredLog()
	// check the backups, make sure backup connections are intact
	for name, node := range worker.Backups {
		if _, ok := s.backups[name]; !ok {
			if err := s.AddBackupRoutine(name, node); err != nil {
				log.Error("Failed to connect to backup, retry in the next cycle.", zap.Error(err))
			}
		}
	}
	for name := range s.backups {
		if _, ok := worker.Backups[name]; !ok {
			// not found, remove this backup routine
			if err := s.RemoveBackupRoutine(name); err != nil {
				log.Error("Failed to remove backup routine, retry in the next cycle.", zap.Error(err))
			}
		}
	}
	// now check additional primaries
	if len(worker.Primaries) > 1 && s.origMode != MODE_PRIMARY {
		// get the second primary, which should be the primary to migrate to
		var name string
		var node *common.WorkerNode
		for k, v := range worker.Primaries {
			if k == s.NodeName {
				continue
			}
			name = k
			node = v
			break
		}
		go s.migrateToNewPrimary(name, node)
	}
}

// migration logic
func (s *WorkerServer) doMigration() error {
	log := common.SugaredLog()
	p := path.Join(common.ZK_MIGRATIONS_ROOT, strconv.Itoa(int(s.SlotTableVersion)), strconv.Itoa(int(s.Id)))

	// fetch migration content
	var migration common.SingleNodeMigration
	err := common.ZkGet(s.conn, p, &migration)
	if err == zk.ErrNoNode {
		log.Info("Migration complete, nothing to do.")
		s.SlotTableVersion += 1
		return nil
	} else if err != nil {
		return err
	}

	// Separate the migration plan to specific plans to different destinations
	// Start goroutines and wait for them to complete
	destinations := migration.GetDestinations()
	wg := sync.WaitGroup{}
	wg.Add(len(destinations))
	for _, dst := range destinations {
		dst := dst // capture loop variable
		go func() {
			log.Infof("Migration to worker %d started.", dst)
			// get worker's primary node
			var primaryName string
			var primary *common.WorkerNode
			// keep watching, until we get a unique primary node
			for {
				worker, err := common.GetAndWatchWorker(s.conn, dst)
				if err != nil {
					log.Error("Failed to get worker data.", zap.Int("id", int(dst)), zap.Error(err))
				}
				if len(worker.Primaries) == 1 {
					for n, p := range worker.Primaries {
						primaryName = n
						primary = p
					}
					break
				}
				_ = <-worker.Watcher
			}

			// connect through gRPC
			connString := fmt.Sprintf("%s:%d", primary.Host.Hostname, primary.Host.Port)
			conn, err := common.ConnectGrpc(connString)
			if err != nil {
				log.Error("Failed to connect to destination.", zap.Error(err))
			}
			client := pb.NewKVBackupClient(conn)

			// create mask & register replication strategy
			mask := func(key string) bool {
				return migration.GetDestWorkerId(key) == dst
			}
			routine := SyncRoutine{
				name:    primaryName,
				conn:    client,
				Mask:    mask,
				Syncing: false,
				EntryCh: make(chan *pb.BackupEntry),
				StopCh:  make(chan struct{}),
			}
			// start recording new requests that should broadcast
			s.migrations[primaryName] = &routine
			log.Infof("Sync routine for #%d registered.", dst)
			// keep preparing until success
			for {
				extracted := s.kv.Extract(mask, 0)
				if err := routine.Prepare(extracted); err == nil {
					break
				}
				log.Error("Preparation failed, retrying in 2 seconds...", zap.Error(err))
				time.Sleep(2 * time.Second)
			}
			// make it sync
			routine.Syncing = true
			wg.Done()
			routine.Sync()
		}()
	}
	wg.Wait()
	log.Info("Migration: all sync, reducing semaphore")
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
		bin, _, eventChan, err := s.conn.GetW(common.ZK_TABLE_VERSION)
		if err != nil {
			return err
		}
		// get version
		val, err := strconv.Atoi(string(bin))
		if err != nil {
			return err
		}
		v := uint32(val)
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

func (s *WorkerServer) AddBackupRoutine(nodeName string, node *common.WorkerNode) error {
	log := common.SugaredLog()
	connString := fmt.Sprintf("%s:%d", node.Host.Hostname, node.Host.Port)
	conn, err := common.ConnectGrpc(connString)
	if err != nil {
		return err
	}
	routine := NewSyncRoutine(nodeName, pb.NewKVBackupClient(conn), func(_ string) bool { return true }, s.backupCond)
	s.lock.Lock()
	s.backups[nodeName] = &routine
	s.lock.Unlock()
	go func() {
		log.Infof("Backing up with %s", nodeName)
		for {
			content := s.kv.Extract(func(_ string) bool { return true }, 0)
			if err := routine.Prepare(content); err == nil {
				break
			}
			if len(routine.StopCh) > 0 {
				_ = <-routine.StopCh
				log.Infof("Stopping...")
				break
			}
			log.Infof("Backing up with %s failed, retrying in 2 seconds...", nodeName)
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
