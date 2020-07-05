package worker

import (
	"errors"
	"fmt"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"path"
	"strconv"
	"sync"
)

const (
	MODE_PRIMARY = common.ZK_PRIMARY_WORKER_NAME
	MODE_BACKUP  = common.ZK_BACKUP_WORKER_NAME
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

	// original mode, specified when being first run.
	// this help determine whether it should step down as the temporary primary
	origMode string
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
		}
	}
}

// Watch for next migration plan
func (s *WorkerServer) WatchMigration() {
	log := common.SugaredLog()
	for {
		p := path.Join(common.ZK_MIGRATIONS_ROOT, strconv.Itoa(int(s.SlotTableVersion)))
		log.Info("Watching migration node...", zap.String("path", p))
		// keep waiting until migration node shows up
		for {
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
			log.Infof("Primary worker is dealing with migration #%d's logic...", s.SlotTableVersion)
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
	if mode != MODE_PRIMARY && mode != MODE_BACKUP {
		return errors.New("invalid mode")
	}
	if mode == s.mode {
		return errors.New("no need for transformation")
	}
	// first re-register itself in zookeeper
	log := common.Log()
	n := common.NewWorkerNode(s.Hostname, s.Port, s.Id)
	p := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)))
	var nodePath string
	if mode == MODE_PRIMARY {
		nodePath = path.Join(p, common.ZK_PRIMARY_WORKER_NAME)
	} else {
		nodePath = path.Join(p, common.ZK_BACKUP_WORKER_NAME)
	}
	originalPath := path.Join(p, s.NodeName)
	if err := s.conn.Delete(originalPath, -1); err != nil {
		return err
	}
	name, err := common.ZkCreate(s.conn, nodePath, &n, true, true)
	if err != nil {
		return err
	}
	s.NodeName = path.Base(name)
	s.mode = mode
	// now issue stop and restart all goroutines
	log.Info("Restarting goroutines...")
	s.WatchMigrationStopChan <- struct{}{}
	s.WatchWorkerStopChan <- struct{}{}
	return nil
}
