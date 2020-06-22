package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eyeKill/KV/common"
	"github.com/eyeKill/KV/proto"
	pb "github.com/eyeKill/KV/proto"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"io"
	"path"
	"strconv"
	"time"
)

type BackupServer struct {
	proto.UnimplementedKVBackupServer
	WorkerId   common.WorkerId
	FilePath   string
	primary    *common.Node
	conn       *zk.Conn
	backupConn pb.KVBackupClient
	auth       BackupWorkerId
	kv         *KVStore
}

func NewBackupWorker(filePath string, id common.WorkerId) (*BackupServer, error) {
	kv, err := NewKVStore(filePath)
	if err != nil {
		return nil, err
	}
	return &BackupServer{
		FilePath: filePath,
		WorkerId: id,
		kv:       kv,
	}, nil
}

// connect to primary with given primary id
func (s *BackupServer) Connect(conn *zk.Conn) error {
	log := common.Log()
	if conn == nil {
		log.Error("Connection invalid.")
	}
	s.conn = conn

	workerPath := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.WorkerId)))
	b, _, err := conn.Get(workerPath)
	if err != nil {
		return err
	}
	var workerData common.Worker
	if err := json.Unmarshal(b, &workerData); err != nil {
		return err
	}
	primaryName := workerData.Primary
	b, _, err = conn.Get(path.Join(common.ZK_NODES_ROOT, primaryName))
	if err != nil {
		return err
	}
	node, err := common.UnmarshalNode(b)
	if err != nil {
		return err
	}
	switch node.(type) {
	case *common.PrimaryWorkerNode:
		s.primary = &node.(*common.PrimaryWorkerNode).Host
		// connect through gRPC
		connString := fmt.Sprintf("%s:%d", s.primary.Hostname, s.primary.Port)
		grpcConn, err := common.ConnectGrpc(connString)
		if err != nil {
			log.Panic("Failed to connect through gRPC.",
				zap.Int("id", int(s.WorkerId)), zap.String("conn", connString), zap.Error(err))
		}
		s.backupConn = pb.NewKVBackupClient(grpcConn)
		// register itself
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := s.backupConn.Register(ctx, &pb.BackupClientAuth{})
		if err != nil {
			log.Panic("Failed to register to primary node.", zap.Error(err))
		}
		s.auth = BackupWorkerId(resp.Id)
		log.Info("Registered to primary.", zap.Any("auth token", s.auth))
	default:
		return errors.New(fmt.Sprintf("Expecting primary node, found %v", node))
	}

	return nil
}

// open up gRPC connection and keep listening
// note that backup slaves are clients, primaries are servers.
func (s *BackupServer) DoBackup() {
	log := common.Log()
	m := metadata.New(map[string]string{KEY_BACKUP_WORKER_ID: strconv.Itoa(int(s.auth))})
	ctx := metadata.NewOutgoingContext(context.Background(), m)
	stream, err := s.backupConn.Backup(ctx)
	if err != nil {
		log.Panic("Failed to get stream.", zap.Error(err))
	}
	for {
		ent, err := stream.Recv()
		if err == io.EOF {
			break
		} else {
			log.Panic("Stream failed.", zap.Error(err))
		}
		switch ent.Op {
		case pb.Operation_PUT:
			s.kv.Put(ent.Key, ent.Value)
		case pb.Operation_DELETE:
			s.kv.Delete(ent.Key)
		default:
			log.Error("Operation not supported.", zap.Int("num", int(ent.Op)),
				zap.String("name", pb.Operation_name[int32(ent.Op)]))
		}
	}
}
