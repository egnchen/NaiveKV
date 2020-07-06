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
	"google.golang.org/grpc/metadata"
	"io"
	"path"
	"reflect"
	"strconv"
	"sync"
	"time"
)

func GetMigrationVersionKey(id common.WorkerId) string {
	return fmt.Sprintf("$migration-worker-%d", id)
}

// Transfer bunch of data with transaction
// In migration version number between hosts are not sync, we have to save transaction number explicitly.
// In sync between replacement primary and new primary(me), we do not have to explicitly save transaction number.
// In fact, we have to keep them in sync, like backups would do.
func (s *WorkerServer) MigrateTransfer(server pb.KVBackup_TransferServer, remoteId common.WorkerId) error {
	log := common.SugaredLog()
	log.Infof("Receiving bulk data from worker #%d...", remoteId)
	// send last version number
	value, err := s.kv.Get(GetMigrationVersionKey(remoteId), 0)
	var startVersion uint64 = 0
	if err == nil {
		startVersion, err = strconv.ParseUint(value, 16, 64)
		if err != nil {
			return err
		}
	}
	header := metadata.Pairs(HEADER_VERSION_NUMBER, strconv.FormatUint(startVersion, 16))
	if err := server.SendHeader(header); err != nil {
		return err
	}
	// start a new transaction
	tid, err := s.kv.StartTransaction()
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
			}
			// explicitly record remote version id
			key := GetMigrationVersionKey(remoteId)
			strVersion := strconv.FormatUint(version, 16)
			if _, err := s.kv.Put(key, strVersion, 0); err != nil {
				log.Error("Failed to commit version number.", zap.Error(err))
				return server.SendAndClose(&pb.BackupReply{
					Status:  pb.Status_EFAILED,
					Version: version,
				})
			}
			log.Info("Successfully committed")
			s.versionCond.L.Lock()
			s.version = version
			s.versionCond.L.Unlock()
			return server.SendAndClose(&pb.BackupReply{
				Status:  pb.Status_OK,
				Version: version,
			})
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
		// update version
		if ent.Version > version {
			version = ent.Version
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

// Transfer data piece by piece in a loss less manner.
// We have to maintain remote version number explicitly
func (s *WorkerServer) MigrateSync(server pb.KVBackup_SyncServer) error {
	log := common.SugaredLog()
	// receive client's worker id
	md, ok := metadata.FromIncomingContext(server.Context())
	if !ok {
		return errors.New("failed to get remote worker id")
	}
	ks := md.Get(HEADER_CLIENT_WORKER_ID)
	if len(ks) != 1 {
		return errors.New("failed to get remote worker id")
	}
	rid, err := strconv.Atoi(ks[0])
	remoteId := common.WorkerId(rid)
	if err != nil {
		return errors.New("failed to get remote worker id")
	}
	log.Infof("In sync with worker #%d...", remoteId)
	// get latest version
	versionKey := GetMigrationVersionKey(remoteId)
	strVersion, err := s.kv.Get(versionKey, 0)
	var version uint64 = 0
	if err != nil && err != ENOENT {
		return err
	} else {
		version, err = strconv.ParseUint(strVersion, 16, 64)
		if err != nil {
			return err
		}
	}

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
			}
			if version < ent.Version {
				version = ent.Version
				strVersion := strconv.FormatUint(version, 16)
				if _, err := s.kv.Put(versionKey, strVersion, 0); err != nil {
					if err := server.Send(&pb.BackupReply{
						Status:  pb.Status_EFAILED,
						Version: ent.Version,
					}); err != nil {
						return err
					}
				}
			}
			if err := server.Send(&pb.BackupReply{
				Status:  pb.Status_OK,
				Version: ent.Version,
			}); err != nil {
				return err
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
			}
			if version < ent.Version {
				version = ent.Version
				strVersion := strconv.FormatUint(version, 16)
				if _, err := s.kv.Put(versionKey, strVersion, 0); err != nil {
					if err := server.Send(&pb.BackupReply{
						Status:  pb.Status_EFAILED,
						Version: ent.Version,
					}); err != nil {
						return err
					}
				}
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

func (s *WorkerServer) RegisterBackup(name string, routine *SyncRoutine) {
	s.backups[name] = routine
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
// Does not matter if it runs under primary or backup, since backup does not have any sync routine
func (s *WorkerServer) DoSync() {
	log := common.SugaredLog()
	log.Info("Sync goroutine is up and running...")
	for {
		select {
		case entry := <-s.backupCh:
			s.backupLock.RLock()
			// sync all backups and migrations
			for _, routine := range s.backups {
				if routine.GetMask()(entry.Key) {
					routine.EntryCh <- entry
				}
			}
			for _, routine := range s.migrations {
				if routine.GetMask()(entry.Key) {
					routine.EntryCh <- entry
				}
			}
			s.backupLock.RUnlock()

			// backup use semi-sync transfer, which means
			// only one of them have to ack before going on.
			if len(s.backups) > 0 {
				log.Infof("WAITING FOR BACKUPS")
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

			if len(s.migrations) > 0 {
				log.Infof("WAITING FOR MIGRATIONS")
				// migration use loss less transfer, which means all of them should complete
				for _, routine := range s.migrations {
					if routine.Syncing && routine.GetMask()(entry.Key) {
						routine.Condition.L.Lock()
						for routine.Version < entry.Version {
							routine.Condition.Wait()
						}
						routine.Condition.L.Unlock()
					}
				}
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
	if s.mode != MODE_PRIMARY || s.readOnly {
		return &pb.PutResponse{Status: pb.Status_ENOSERVER}, nil
	}
	log := common.SugaredLog()
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
	log.Infof("SYNCED REMOTELY")
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
	if s.mode != MODE_PRIMARY || s.readOnly {
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
		resps, err := common.ZkMulti(s.conn, &zk.CreateRequest{
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
		})
		if err != nil {
			return err
		}
		// get node name from last response's response
		s.NodeName = path.Base(resps[len(resps)-1].String)
	} else {
		// get configuration from zookeeper
		var config common.WorkerConfig
		if err := common.ZkGet(s.conn, path.Join(p, common.ZK_WORKER_CONFIG_NAME), &config); err != nil {
			return err
		}
		s.config = config
		// register itself
		name, err := common.ZkCreate(s.conn, nodePath, node, true, true)
		if err != nil {
			return err
		}
		s.NodeName = path.Base(name)
	}
	return nil
}

// Migrate to the "real" primary when I'm the replacement primary.
func (s *WorkerServer) migrateToNewPrimary(newPrimary string) {
	log := common.SugaredLog()
	log.Info("Migrating to new primary.", zap.String("nodeName", newPrimary))
	// an always true mask
	mask := func(key string) bool { return true }
	// register replication strategy
	routine, err := NewSyncRoutine(s, newPrimary, mask, sync.NewCond(&sync.Mutex{}))
	if err != nil {
		log.Errorf("Failed to get sync routine for %s: %+v", newPrimary, err)
		return
	}
	// register it to start recording new requests
	s.backups[newPrimary] = routine
	log.Infof("Sync routine for %s registered.", newPrimary)
	// keep preparing until success
	for {
		if err := routine.Prepare(); err == nil {
			break
		}
		println(reflect.TypeOf(err))
		log.Error("Preparation failed, retrying...", zap.Error(err))
		time.Sleep(2 * time.Second)
	}
	// make it sync
	log.Info("New primary is now sync with backup.")
	routine.Syncing = true
	go routine.Sync()
	// now it's time to step down
	if err := s.transformTo(MODE_BACKUP); err != nil {
		log.Error("Failed to step down.", zap.Error(err))
	}
}

func (s *WorkerServer) primaryWatch(worker common.Worker) {
	log := common.SugaredLog()
	// check the backups, make sure backup connections are intact
	for name := range worker.Backups {
		if _, ok := s.backups[name]; !ok {
			if err := s.AddBackupRoutine(name); err != nil {
				log.Error("Failed to connect to backup, retry in the next cycle.", zap.Error(err))
			}
		}
	}
	for name := range s.backups {
		if _, ok := worker.Backups[name]; !ok {
			// not found, remove this backup routine
			log.Infof("Removing backup routine with %s...", name)
			if err := s.RemoveBackupRoutine(name); err != nil {
				log.Error("Failed to remove backup routine, retry in the next cycle.", zap.Error(err))
			}
		}
	}
	// update number of backups to config file
	s.config.NumBackups = len(s.backups)
	configPath := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)), common.ZK_WORKER_CONFIG_NAME)
	bin, _ := json.Marshal(s.config)
	if _, err := s.conn.Set(configPath, bin, -1); err != nil {
		log.Error("Failed to update config file.", zap.Error(err))
	}
	// now check additional primaries
	if len(worker.Primaries) > 1 && s.origMode != MODE_PRIMARY {
		// get the second primary, which should be the primary to migrate to
		log.Info("Another primary detected, stepping down...")
		var name string
		for k := range worker.Primaries {
			if k == s.NodeName {
				continue
			}
			name = k
			break
		}
		// get full name
		fullName := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)), name)
		go s.migrateToNewPrimary(fullName)
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
			// keep watching, until we get a unique primary node
			var primaryName string
			for {
				worker, err := common.GetAndWatchWorker(s.conn, dst)
				if err != nil {
					log.Error("Failed to get worker data.", zap.Int("id", int(dst)), zap.Error(err))
				}
				if len(worker.Primaries) == 1 {
					for n := range worker.Primaries {
						primaryName = n
					}
					break
				}
				_ = <-worker.Watcher
			}
			fullName := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(dst)), primaryName)

			// create mask & register replication strategy
			mask := func(key string) bool {
				return migration.GetDestWorkerId(key) == dst
			}
			routine, err := NewSyncRoutine(s, fullName, mask, sync.NewCond(&sync.Mutex{}))
			if err != nil {
				log.Errorf("Failed to get sync routine for %s: %+v", primaryName, err)
				return
			}
			// start recording new requests that should broadcast
			s.migrations[primaryName] = routine
			log.Infof("Sync routine for #%d registered.", dst)
			// keep preparing until success
			for {
				if err := routine.Prepare(); err == nil {
					break
				}
				println(err)
				println(reflect.TypeOf(err))
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

func (s *WorkerServer) AddBackupRoutine(nodeName string) error {
	log := common.SugaredLog()
	fullName := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)), nodeName)
	routine, err := NewSyncRoutine(s, fullName, func(_ string) bool { return true }, s.backupCond)
	if err != nil {
		return err
	}
	s.backupLock.Lock()
	s.backups[nodeName] = routine
	s.backupLock.Unlock()
	go func() {
		log.Infof("Backing up with %s", fullName)
		for {
			if err := routine.Prepare(); err == nil {
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
	s.backupLock.Lock()
	delete(s.backups, backupNodeName)
	s.backupLock.Unlock()
	return nil
}
