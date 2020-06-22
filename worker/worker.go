package worker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

type PrimaryServer struct {
	pb.UnimplementedKVWorkerServer
	pb.UnimplementedKVBackupServer
	Hostname    string
	Port        uint16
	FilePath    string
	conn        *zk.Conn
	kv          *KVStore
	config      WorkerConfig
	backupChans map[BackupWorkerId]chan *pb.BackupEntry
}

const (
	CONFIG_FILENAME = "config.json"
)

func NewWorkerServer(hostname string, port uint16, filePath string) (*PrimaryServer, error) {
	kv, err := NewKVStore(filePath)
	if err != nil {
		return nil, err
	}
	return &PrimaryServer{
		Hostname:    hostname,
		Port:        port,
		FilePath:    filePath,
		kv:          kv,
		config:      WorkerConfig{},
		backupChans: make(map[BackupWorkerId]chan *pb.BackupEntry),
	}, nil
}

func (s *PrimaryServer) Put(_ context.Context, pair *pb.KVPair) (*pb.PutResponse, error) {
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
	ok := s.kv.Delete(key.Key)
	if ok {
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

func (s *PrimaryServer) Register(_ context.Context, _ *pb.BackupClientAuth) (*pb.BackupClientToken, error) {
	// TODO add some authentication mechanism
	id := GetNextBackupWorkerId()
	s.backupChans[id] = make(chan *pb.BackupEntry)
	return &pb.BackupClientToken{Id: int32(id)}, nil
}

func (s *PrimaryServer) Backup(srv pb.KVBackup_BackupServer) error {
	log := common.Log()
	ctx := srv.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	warning := "failed to read worker id from metadata context"
	if !ok {
		return errors.New(warning)
	}
	vals := md.Get(KEY_BACKUP_WORKER_ID)
	if len(vals) != 1 {
		return errors.New(warning)
	}
	id, err := strconv.Atoi(vals[0])
	if err != nil {
		return err
	}
	ch, ok := s.backupChans[BackupWorkerId(id)]
	if !ok {
		return errors.New("primary did not register")
	}

	recvCh := make(chan *pb.BackupReply)
	// use a goroutine to convert srv.Recv() to a channel
	go func() {
		for {
			repl, err := srv.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Error("Backup reply stream received error.",
					zap.Any("id", id), zap.Error(err))
				break
			}
			recvCh <- repl
		}
	}()
	for {
		select {
		case <-ctx.Done():
			log.Info("Backup server terminated through server context.")
			return ctx.Err()
		case ent := <-ch:
			if err := srv.Send(ent); err != nil {
				log.Error("Sync failed.", zap.Error(err))
			}
		case <-recvCh:
			// TODO deal with reply
		default:
		}
	}
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

	// WorkerNode don't have to ensure that path exists.
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
	log.Info("Configuration file saved.")
	return nil
}
