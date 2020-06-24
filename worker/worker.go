package worker

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
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
)

type BackupRoutine struct {
	name   string
	conn   pb.KVBackupClient
	ch     chan *pb.BackupEntry
	stopCh chan struct{}
}

func (s *BackupRoutine) loop() {
	log := common.Log()
	// start gRPC bi-directional stream
	stream, err := s.conn.Backup(context.Background())
	if err != nil {
		log.Error("Failed to open grpc stream.", zap.Error(err))
		return
	}
	log.Sugar().Infof("Backup routine for %s started", s.name)
backupRoutine:
	for {
		select {
		case ent := <-s.ch:
			if err := stream.Send(ent); err != nil {
				log.Error("Failed to send backup entry.", zap.Error(err))
				break backupRoutine
			}
		case <-s.stopCh:
			break backupRoutine
		}
	}
}

type PrimaryServer struct {
	pb.UnimplementedKVWorkerServer
	pb.UnimplementedKVBackupServer
	Hostname    string
	Port        uint16
	FilePath    string
	conn        *zk.Conn
	kv          KVStore
	config      WorkerConfig
	workerCache common.Worker
	backupCh    chan *pb.BackupEntry
	lock        sync.Mutex
	backups     map[string]*BackupRoutine
}

const (
	CONFIG_FILENAME = "config.json"
)

func NewPrimaryServer(hostname string, port uint16, filePath string) (*PrimaryServer, error) {
	kv, err := NewKVStore(filePath)
	if err != nil {
		return nil, err
	}
	return &PrimaryServer{
		Hostname: hostname,
		Port:     port,
		FilePath: filePath,
		kv:       kv,
		config:   WorkerConfig{},
		backupCh: make(chan *pb.BackupEntry),
		backups:  make(map[string]*BackupRoutine),
	}, nil
}

// Broadcast backup entry to channels of different backup servers.
func (s *PrimaryServer) DoBackup(stopChan <-chan struct{}) {
routine:
	for {
		select {
		case ent := <-s.backupCh:
			s.lock.Lock()
			for _, r := range s.backups {
				r.ch <- ent
			}
			s.lock.Unlock()
		case <-stopChan:
			break routine
		}
	}
}

func (s *PrimaryServer) Put(_ context.Context, pair *pb.KVPair) (*pb.PutResponse, error) {
	// commit to slave first
	ent := pb.BackupEntry{
		Op:    pb.Operation_PUT,
		Key:   pair.Key,
		Value: pair.Value,
	}
	s.backupCh <- &ent
	s.kv.Put(pair.Key, pair.Value)
	return &pb.PutResponse{Status: pb.Status_OK}, nil
}

func (s *PrimaryServer) Get(_ context.Context, key *pb.Key) (*pb.GetResponse, error) {
	value, ok := s.kv.Get(key.Key)
	if ok {
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
	ent := pb.BackupEntry{
		Op:  pb.Operation_DELETE,
		Key: key.Key,
	}
	s.backupCh <- &ent
	if s.kv.Delete(key.Key) {
		return &pb.DeleteResponse{Status: pb.Status_OK}, nil
	} else {
		return &pb.DeleteResponse{Status: pb.Status_ENOENT}, nil
	}
}

func (s *PrimaryServer) Checkpoint(_ context.Context, _ *empty.Empty) (*pb.FlushResponse, error) {
	if err := s.kv.Checkpoint(); err != nil {
		common.Log().Error("KV flush failed.", zap.Error(err))
		return &pb.FlushResponse{Status: pb.Status_EFAILED}, nil
	}
	return &pb.FlushResponse{Status: pb.Status_OK}, nil
}

// Update configuration. Read configuration if file exists, and generate a new one otherwise.
func (s *PrimaryServer) updateConfig() error {
	// get primary id & configuration
	b, err := ioutil.ReadFile(path.Join(s.FilePath, CONFIG_FILENAME))
	if err != nil && os.IsExist(err) {
		return err
	} else if os.IsNotExist(err) {
		// get new primary id
		distributedInteger := common.DistributedAtomicInteger{Conn: s.conn, Path: common.ZK_WORKER_ID}
		v, err := distributedInteger.Inc()
		if err != nil {
			return err
		}
		s.config = WorkerConfig{Id: common.WorkerId(v)}
	} else {
		var config WorkerConfig
		if err := json.Unmarshal(b, &config); err != nil {
			return err
		}
		s.config = config
	}
	return nil
}

func (s *PrimaryServer) RegisterToZk(conn *zk.Conn) error {
	log := common.Log()

	// workers don't have to ensure that path exists.
	nodePath := path.Join(common.ZK_NODES_ROOT, common.ZK_WORKER_NAME)
	exists, _, err := conn.Exists(common.ZK_NODES_ROOT)
	if err != nil {
		return err
	} else if !exists {
		return errors.New("root node in zookeeper does not exist, start master node first")
	}
	s.conn = conn

	if err := s.updateConfig(); err != nil {
		log.Warn("Failed to update configuration.", zap.Error(err))
		return err
	}
	log.Info("Initialized configuration", zap.Uint16("id", uint16(s.config.Id)))
	data := common.NewPrimaryWorkerNode(s.Hostname, s.Port, s.config.Id, 10)
	b, err := json.Marshal(&data)
	if err != nil {
		return err
	}
	name, err := conn.CreateProtectedEphemeralSequential(nodePath, b, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	log.Info("Registration complete.", zap.String("name", name))
	// save config file
	b, err = json.Marshal(s.config)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(path.Join(s.FilePath, CONFIG_FILENAME), b, 0644); err != nil {
		return err
	}
	// start another goroutine to watch
	log.Info("Configuration file saved.")
	return nil
}

func (s *PrimaryServer) Watch(stopChan <-chan struct{}) {
	log := common.Log()
	log.Info("Starting to watch worker znode...", zap.Int("id", int(s.config.Id)))
	workerNodePath := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.config.Id)))
	for {
		exists, _, eventChan, err := s.conn.ExistsW(workerNodePath)
		if err != nil {
			log.Error("Failed to watch.", zap.String("path", workerNodePath), zap.Error(err))
			break
		}
		if exists {
			b, _, err := s.conn.Get(workerNodePath)
			if err != nil {
				log.Error("Failed to get znode content.", zap.String("path", workerNodePath), zap.Error(err))
				return
			}
			var worker common.Worker
			if err := json.Unmarshal(b, &worker); err != nil {
				log.Error("Invalid content.", zap.String("content", string(b)), zap.Error(err))
				return
			}
			// compare new metadata with local cache
			// for now we only handle adding backups
			// TODO handle missing backups
			log.Info(string(b))
			if len(worker.Backups) > len(s.workerCache.Backups) {
				// figure out the new one
				bak := worker.Backups
				bak = common.RemoveElements(bak, s.workerCache.Backups...)
				for _, b := range bak {
					if err := s.ConnectToBackup(b); err != nil {
						log.Error("Failed to connect to backup server.", zap.Error(err))
					} else {
						log.Info("Successfully connected to new backup server.", zap.String("name", b))
					}
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

func (s *PrimaryServer) ConnectToBackup(backupNodeName string) error {
	b, _, err := s.conn.Get(path.Join(common.ZK_NODES_ROOT, backupNodeName))
	if err != nil {
		return err
	}
	node, err := common.UnmarshalNode(b)
	if err != nil {
		return err
	}
	switch node.(type) {
	case *common.BackupWorkerNode:
		n := node.(*common.BackupWorkerNode)
		connString := fmt.Sprintf("%s:%d", n.Host.Hostname, n.Host.Port)
		grpcConn, err := common.ConnectGrpc(connString)
		if err != nil {
			return err
		}
		routine := BackupRoutine{
			conn:   pb.NewKVBackupClient(grpcConn),
			ch:     make(chan *pb.BackupEntry),
			name:   backupNodeName,
			stopCh: make(chan struct{}),
		}
		go routine.loop()
		s.lock.Lock()
		s.backups[backupNodeName] = &routine
		s.lock.Unlock()
		return nil
	default:
		return errors.New("not a backup node")
	}
}
