package worker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/eyeKill/KV/common"
	"github.com/eyeKill/KV/proto"
	pb "github.com/eyeKill/KV/proto"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"io"
	"path"
	"strconv"
)

type BackupServer struct {
	proto.UnimplementedKVBackupServer
	Hostname   string
	Port       uint16
	WorkerId   common.WorkerId
	FilePath   string
	primary    *common.Node
	conn       *zk.Conn
	backupConn pb.KVBackupClient
	kv         KVStore
}

func NewBackupWorker(hostname string, port uint16, filePath string, id common.WorkerId) (*BackupServer, error) {
	kv, err := NewKVStore(filePath)
	if err != nil {
		return nil, err
	}
	return &BackupServer{
		Hostname: hostname,
		Port:     port,
		FilePath: filePath,
		WorkerId: id,
		kv:       kv,
	}, nil
}

// backup routine

func (s *BackupServer) Sync(srv pb.KVBackup_SyncServer) error {
	log := common.Log()
	log.Info("Syncing with primary...")
	for {
		ent, err := srv.Recv()
		if err == io.EOF {
			log.Info("EOF received, exiting backup routine")
			return nil
		} else if err == context.Canceled {
			log.Info("Canceled received, exiting backup routine")
			return nil
		} else if err != nil {
			log.Info("Failed to receive backup entry.", zap.Error(err))
			return err
		}
		switch ent.Op {
		case pb.Operation_PUT:
			s.kv.Put(ent.Key, ent.Value, 0)
		case pb.Operation_DELETE:
			s.kv.Delete(ent.Key, 0)
		default:
			log.Sugar().Warnf("Unsupported operation %s", pb.Operation_name[int32(ent.Op)])
		}
	}
}

func (s *BackupServer) RegisterToZk(conn *zk.Conn) error {
	log := common.Log()

	// backup does not have to ensure that path exists.
	workerPath := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.WorkerId)))
	exists, _, err := conn.Exists(workerPath)
	if err != nil {
		return err
	} else if !exists {
		return errors.New("root node in zookeeper does not exist, start master node first")
	}
	s.conn = conn

	log.Info("Initialized configuration", zap.Uint16("id", uint16(s.WorkerId)))
	data := common.NewWorkerNode(s.Hostname, s.Port, s.WorkerId)
	b, err := json.Marshal(&data)
	if err != nil {
		return err
	}
	nodePath := path.Join(workerPath, common.ZK_BACKUP_WORKER_NAME)
	name, err := conn.CreateProtectedEphemeralSequential(nodePath, b, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	log.Info("Registration complete.", zap.String("name", name))
	return nil
}
