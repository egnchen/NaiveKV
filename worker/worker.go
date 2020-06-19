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
	"io/ioutil"
	"os"
	"path"
)

type Server struct {
	pb.UnimplementedKVWorkerServer
	Hostname string
	Port     uint16
	FilePath string
	conn     *zk.Conn
	kv       *KVStore
	config   WorkerConfig
}

const (
	CONFIG_FILENAME = "config.json"
)

func NewWorkerServer(hostname string, port uint16, filePath string) (*Server, error) {
	kv, err := NewKVStore(filePath)
	if err != nil {
		return nil, err
	}
	return &Server{
		Hostname: hostname,
		Port:     port,
		FilePath: filePath,
		kv:       kv,
		config:   WorkerConfig{},
	}, nil
}

func (s *Server) Put(_ context.Context, pair *pb.KVPair) (*pb.PutResponse, error) {
	s.kv.Put(pair.Key, pair.Value)
	return &pb.PutResponse{Status: pb.Status_OK}, nil
}

func (s *Server) Get(_ context.Context, key *pb.Key) (*pb.GetResponse, error) {
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

func (s *Server) Delete(_ context.Context, key *pb.Key) (*pb.DeleteResponse, error) {
	ok := s.kv.Delete(key.Key)
	if ok {
		return &pb.DeleteResponse{Status: pb.Status_OK}, nil
	} else {
		return &pb.DeleteResponse{Status: pb.Status_ENOENT}, nil
	}
}

func (s *Server) Checkpoint(_ context.Context, _ *empty.Empty) (*pb.FlushResponse, error) {
	if err := s.kv.Checkpoint(); err != nil {
		common.Log().Error("KV flush failed.", zap.Error(err))
		return &pb.FlushResponse{Status: pb.Status_EFAILED}, nil
	}
	return &pb.FlushResponse{Status: pb.Status_OK}, nil
}

// Update configuration. Read configuration if file exists, and generate a new one otherwise.
func (s *Server) updateConfig() error {
	// get worker id & configuration
	b, err := ioutil.ReadFile(path.Join(s.FilePath, CONFIG_FILENAME))
	if err != nil && os.IsExist(err) {
		return err
	} else if os.IsNotExist(err) {
		// get new worker id
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

func (s *Server) RegisterToZk(conn *zk.Conn) error {
	log := common.Log()

	// Worker don't have to ensure that path exists.
	nodePath := path.Join(common.ZK_NODES_ROOT, common.ZK_WORKER_NAME)
	exists, _, err := conn.Exists(common.ZK_NODES_ROOT)
	if err != nil {
		return err
	} else if !exists {
		return errors.New("root node in zookeeper does not exist, start master node first")
	}

	if err := s.updateConfig(); err != nil {
		log.Warn("Failed to update configuration.", zap.Error(err))
		return err
	}
	log.Info("Initialized configuration", zap.Uint16("id", uint16(s.config.Id)))
	data := common.GetNewWorkerNode(s.Hostname, s.Port, s.config.Id, 10)
	b, err := json.Marshal(data)
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
