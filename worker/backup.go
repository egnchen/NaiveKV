package worker

import (
	"fmt"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (s *WorkerServer) registerBackup() error {
	// backup do not have to ensure that path exists
	// just register itself will do
	node := common.NewWorkerNode(s.Hostname, s.Port, s.Id)
	nodePath := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)), common.ZK_BACKUP_WORKER_NAME)
	name, err := common.ZkCreate(s.conn, nodePath, node, true, true)
	if err != nil {
		return err
	}
	s.NodeName = path.Base(name)
	return nil
}

// Watch current worker node. Fire up backup election if primary worker is down.
func (s *WorkerServer) backupWatch(worker common.Worker) {
	log := common.SugaredLog()
	if len(worker.Primaries) == 0 {
		log.Info("Primary down detected, beginning primary election.")
		if err := s.backupElection(); err != nil {
			log.Error("Failed to perform backup election.", zap.Error(err))
		}
	}
}

func (s *WorkerServer) backupElection() error {
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
				// wait for a little while for primary to complete
				time.Sleep(500 * time.Millisecond)
			}
			break
		}
		_ = <-eventChan
	}
	return nil
}

// do nothing
func (s *WorkerServer) backupWatchMigration(stopChan chan struct{}) {
	<-stopChan
	return
}

func (s *WorkerServer) BackupTransfer(server pb.KVBackup_TransferServer) error {
	log := common.SugaredLog()
	// don't need to get client's worker id
	// send last version number
	startVersion := s.kv.GetVersion()
	header := metadata.Pairs(HEADER_VERSION_NUMBER, strconv.FormatUint(startVersion, 16))
	if err := server.SendHeader(header); err != nil {
		return err
	}
	log.Infof("Starting backup from version %x.", startVersion)
	// start a new transaction
	tid, err := s.kv.StartTransaction()
	var version = startVersion
	if err != nil {
		// try again later
		return server.SendAndClose(&pb.BackupReply{
			Status:  pb.Status_EFAILED,
			Version: 0,
		})
	}
	numEntries := 0
	for {
		ent, err := server.Recv()
		if err == io.EOF {
			if numEntries == 0 {
				// rollback to avoid exceeding version number
				if err := s.kv.Rollback(tid); err != nil {
					log.Error("Failed to rollback", zap.Error(err))
					return server.SendAndClose(&pb.BackupReply{
						Status:  pb.Status_EFAILED,
						Version: version,
					})
				}
				return server.SendAndClose(&pb.BackupReply{
					Status:  pb.Status_OK,
					Version: version,
				})
			}
			// commit transaction
			if err := s.kv.Commit(tid); err != nil {
				log.Error("Failed to commit", zap.Error(err))
				return server.SendAndClose(&pb.BackupReply{
					Status:  pb.Status_EFAILED,
					Version: version,
				})
			}
			if err := s.kv.SetVersion(version); err != nil {
				log.Errorf("Failed to set version to %x, version now is %x.", version, s.kv.GetVersion())
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
		numEntries += 1
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

// loss less sync, with sync version number
func (s *WorkerServer) BackupSync(server pb.KVBackup_SyncServer) error {
	// get latest version
	log := common.SugaredLog()
	for {
		ent, err := server.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		var newVersion uint64
		switch ent.Op {
		case pb.Operation_PUT:
			newVersion, err = s.kv.Put(ent.Key, ent.Value, 0)
		case pb.Operation_DELETE:
			newVersion, err = s.kv.Delete(ent.Key, 0)
		}
		if err != nil {
			if err := server.Send(&pb.BackupReply{
				Status:  pb.Status_EFAILED,
				Version: ent.Version,
			}); err != nil {
				return err
			}
		}
		if newVersion != ent.Version {
			log.Errorf("Version number mismatch, expecting %x, found %x.", ent.Version, newVersion)
			if err := server.Send(&pb.BackupReply{
				Status:  pb.Status_EFAILED,
				Version: ent.Version,
			}); err != nil {
				return err
			}
		}
		s.version = ent.Version
		if err := server.Send(&pb.BackupReply{
			Status:  pb.Status_OK,
			Version: ent.Version,
		}); err != nil {
			return err
		}
	}
}
