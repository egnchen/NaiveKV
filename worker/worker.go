package worker

import (
	"errors"
	"fmt"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"path"
	"strconv"
	"sync"
	"time"
)

const (
	MODE_PRIMARY = common.ZK_PRIMARY_WORKER_NAME
	MODE_BACKUP  = common.ZK_BACKUP_WORKER_NAME
)

const (
	HEADER_VERSION_NUMBER   = "versionNumber"
	HEADER_CLIENT_WORKER_ID = "workerId"
)

type WorkerServer struct {
	pb.UnimplementedKVWorkerServer
	pb.UnimplementedKVWorkerInternalServer
	pb.UnimplementedKVBackupServer
	Hostname         string
	Port             uint16
	FilePath         string
	Id               common.WorkerId
	conn             *zk.Conn
	kv               KVStore
	SlotTableVersion atomic.Uint32
	NodeName         string
	config           common.WorkerConfig

	// for backup routine
	backupCh      chan *pb.BackupEntry
	backupLock    sync.RWMutex
	backups       map[string]*SyncRoutine
	backupVersion uint64
	backupCond    *sync.Cond

	// for migration
	migrations  map[string]*SyncRoutine
	version     uint64
	versionCond *sync.Cond

	// for mode switching
	mode           string
	modeChangeCond *sync.Cond
	// specified when first being run, help determine whether it should step down as the temporary primary
	origMode string
	readOnly bool

	// three goroutines: watch workers, watch migration, do sync.
	// watch workers & watch migration should be updated once mode changes
	WatchWorkerStopChan    chan struct{}
	WatchMigrationStopChan chan struct{}
	SyncStopChan           chan struct{}
}

// initialize a server
func NewServer(hostname string, port uint16, filePath string, id common.WorkerId, mode string) (*WorkerServer, error) {
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
		mode:                   mode,
		origMode:               mode,
		backupLock:             sync.RWMutex{},
		backups:                make(map[string]*SyncRoutine),
		backupCh:               make(chan *pb.BackupEntry),
		migrations:             make(map[string]*SyncRoutine),
		versionCond:            sync.NewCond(&sync.Mutex{}),
		modeChangeCond:         sync.NewCond(&sync.Mutex{}),
		backupCond:             sync.NewCond(&sync.Mutex{}),
		WatchMigrationStopChan: make(chan struct{}, 4),
		WatchWorkerStopChan:    make(chan struct{}, 4),
		SyncStopChan:           make(chan struct{}, 4),
		readOnly:               false,
	}, nil
}

func NewPrimaryServer(hostname string, port uint16, filePath string, id common.WorkerId) (*WorkerServer, error) {
	return NewServer(hostname, port, filePath, id, MODE_PRIMARY)
}

func NewBackupServer(hostname string, port uint16, filePath string, id common.WorkerId) (*WorkerServer, error) {
	return NewServer(hostname, port, filePath, id, MODE_BACKUP)
}

// Register oneself to zookeeper.
// Behavior varies depending on whether it is primary worker or backup worker.
func (s *WorkerServer) RegisterToZk(conn *zk.Conn, weight float32) error {
	s.conn = conn
	// get slot table version
	var version uint32
	if err := common.ZkGet(s.conn, common.ZK_TABLE_VERSION, &version); err != nil {
		return err
	}
	s.SlotTableVersion = version
	if s.mode == MODE_PRIMARY {
		return s.registerPrimary(weight)
	} else if s.mode == MODE_BACKUP {
		return s.registerBackup()
	} else {
		return errors.New(fmt.Sprintf("invalid worker mode %s", s.mode))
	}
}

// watch other nodes belonging to the same worker,
// both primary and backup should do this
func (s *WorkerServer) Watch() {
	log := common.SugaredLog()
	for {
		log.Infof("Watching other workers...")
		worker, err := common.GetAndWatchWorker(s.conn, s.Id)
		if err != nil {
			log.Error("Failed to watch worker node.", zap.Error(err))
		}
		if s.mode == MODE_PRIMARY {
			s.primaryWatch(worker)
		} else {
			s.backupWatch(worker)
		}
		select {
		case <-worker.Watcher:
			continue
		case _, ok := <-s.WatchWorkerStopChan:
			if !ok {
				return
			}
			continue
		}
	}
}

// Watch for next migration plan
func (s *WorkerServer) WatchMigration() {
	log := common.SugaredLog()
	for {
		p := path.Join(common.ZK_MIGRATIONS_ROOT, strconv.Itoa(int(s.SlotTableVersion)))
		// keep waiting until migration node shows up
		for {
			log.Info("Watching migration node...", zap.String("path", p))
			exists, _, eventChan, err := s.conn.ExistsW(p)
			if err != nil {
				log.Error("Failed to watch migration node.", zap.Error(err))
			}
			if exists {
				break
			}
			select {
			case <-eventChan:
				continue
			case _, ok := <-s.WatchMigrationStopChan:
				if !ok {
					return
				}
			}
		}
		// only primary workers have to watch migration
		if s.mode == MODE_PRIMARY {
			log.Infof("Doing migration #%d...", s.SlotTableVersion)
			if err := s.doMigration(); err != nil {
				log.Error("Failed to do migration.", zap.Error(err))
			}
		} else {
			// just commit it
			s.SlotTableVersion += 1
		}
	}
}

func (s *WorkerServer) transformTo(mode string) error {
	s.readOnly = false
	if mode != MODE_PRIMARY && mode != MODE_BACKUP {
		return errors.New("invalid mode")
	}
	if mode == s.mode {
		return errors.New("no need for transformation")
	}
	// first re-register itself in zookeeper
	log := common.SugaredLog()
	n := common.NewWorkerNode(s.Hostname, s.Port, s.Id)
	p := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)))
	var nodePath string
	if mode == MODE_PRIMARY {
		nodePath = path.Join(p, common.ZK_PRIMARY_WORKER_NAME)
	} else {
		// transform to backup
		log.Info("Waiting for all migration routines to close...")
		for len(s.migrations) != 0 {
			time.Sleep(500 * time.Millisecond)
		}
		// close all migration & backup routines
		log.Info("Closing all backup routines...")
		for n := range s.backups {
			if err := s.RemoveBackupRoutine(n); err != nil {
				return err
			}
		}
		nodePath = path.Join(p, common.ZK_BACKUP_WORKER_NAME)
	}
	originalPath := path.Join(p, s.NodeName)
	log.Infof("Deleting %s, creating %s", originalPath, nodePath)
	println(originalPath)
	if err := s.conn.Delete(originalPath, -1); err != nil {
		return err
	}
	name, err := common.ZkCreate(s.conn, nodePath, &n, true, true)
	if err != nil {
		return err
	}
	s.NodeName = path.Base(name)
	s.mode = mode
	if s.mode == MODE_PRIMARY {
		worker, err := common.GetAndWatchWorker(s.conn, s.Id)
		if err != nil {
			log.Info("Failed to get worker info.", zap.Error(err))
		}
		// get number of actual backups from election path
		electionPath := path.Join(common.ZK_ELECTION_ROOT, strconv.Itoa(int(s.Id)))
		children, _, err := s.conn.Children(electionPath)
		// primary should be responsible for cleaning the election directory
		if err := common.ZkDeleteRecursive(s.conn, electionPath); err != nil {
			log.Error("Failed to clear election path", zap.Error(err))
			return err
		}
		// use read-only mode if numBackup in config & actual number of backups does not match
		if len(children) != worker.NumBackups {
			log.Infof("Backup number and config number, config num = %d, actual = %d",
				worker.NumBackups, len(children))
			s.readOnly = true
		}
	}
	// now issue stop and restart all goroutines
	log.Info("Restarting goroutines...")
	s.WatchMigrationStopChan <- struct{}{}
	s.WatchWorkerStopChan <- struct{}{}
	return nil
}

// backup server implementations

// Transfer bunch of data with transaction
func (s *WorkerServer) Transfer(server pb.KVBackup_TransferServer) error {
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

	if s.mode == MODE_PRIMARY && remoteId != s.Id {
		return s.MigrateTransfer(server, remoteId)
	} else if remoteId == s.Id {
		return s.BackupTransfer(server)
	} else {
		return errors.New("worker mode invalid")
	}
}

// loss less sync
func (s *WorkerServer) Sync(server pb.KVBackup_SyncServer) error {
	// same logic, receive remote id
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

	if s.mode == MODE_PRIMARY && remoteId != s.Id {
		return s.MigrateSync(server)
	} else if remoteId == s.Id {
		return s.BackupSync(server)
	} else {
		return errors.New("worker mode invalid")
	}
}
