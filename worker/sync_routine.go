package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"strconv"
	"sync"
)

type SyncRoutine struct {
	name         string
	conn         pb.KVBackupClient
	mask         func(string) bool
	kv           KVStore
	id           common.WorkerId
	EntryCh      chan *pb.BackupEntry
	Version      uint64
	Condition    *sync.Cond
	Syncing      bool
	StopCh       chan struct{}
	prepareBegin atomic.Bool
}

func NewSyncRoutine(s *WorkerServer, name string, mask func(string) bool, c *sync.Cond) (*SyncRoutine, error) {
	// get connection
	var node common.WorkerNode
	if err := common.ZkGet(s.conn, name, &node); err != nil {
		return nil, err
	}
	connString := fmt.Sprintf("%s:%d", node.Host.Hostname, node.Host.Port)
	conn, err := common.ConnectGrpc(connString)
	if err != nil {
		return nil, err
	}
	client := pb.NewKVBackupClient(conn)
	return &SyncRoutine{
		name:      name,
		conn:      client,
		mask:      mask,
		kv:        s.kv,
		id:        s.Id,
		EntryCh:   make(chan *pb.BackupEntry),
		Version:   0,
		Condition: c,
		Syncing:   false,
		StopCh:    make(chan struct{}),
	}, nil
}

// get mask, return an always-false mask if preparation is not ready
func (s *SyncRoutine) GetMask() func(string) bool {
	if !s.prepareBegin.Load() {
		return func(_ string) bool { return false }
	} else {
		return s.mask
	}
}

// do bulk transfer before the loss less sync process
func (s *SyncRoutine) Prepare() error {
	log := common.Log()
	ctx := context.Background()
	strWorkerId := strconv.Itoa(int(s.id))
	ctx = metadata.AppendToOutgoingContext(ctx, HEADER_CLIENT_WORKER_ID, strWorkerId)
	client, err := s.conn.Transfer(ctx)
	if err != nil {
		log.Error("Failed to get stream.", zap.Error(err))
		return err
	}
	// retrieve starting version number from server-side context(header)
	header, err := client.Header()
	if err != nil {
		log.Error("Failed to get header.", zap.Error(err))
		return err
	}
	versions := header.Get(HEADER_VERSION_NUMBER)
	if len(versions) != 1 {
		return errors.New("server did not send exactly one starting version number")
	}
	version, err := strconv.ParseUint(versions[0], 16, 64)
	if err != nil {
		return errors.New("invalid starting version number")
	}
	// register itself, begin extraction
	s.prepareBegin.Store(true)
	content := s.kv.Extract(s.mask, version)
	for k, v := range content {
		var ent pb.BackupEntry
		ent.Version = v.Version
		ent.Key = k
		if v.Value == nil {
			ent.Op = pb.Operation_DELETE
		} else {
			ent.Op = pb.Operation_PUT
			ent.Value = *v.Value
		}
		if err := client.Send(&ent); err != nil {
			log.Warn("Failed to transfer entry, closing connection...", zap.Error(err))
			return err
		}
		if version < v.Version {
			version = v.Version
		}
	}
	// ...and those entries during replication
	for len(s.EntryCh) > 0 {
		ent := <-s.EntryCh
		if err := client.Send(ent); err != nil {
			log.Warn("Connection interrupted.", zap.Error(err))
			break
		}
		if version < ent.Version {
			version = ent.Version
		}
	}
	repl, err := client.CloseAndRecv()
	if err != nil {
		log.Error("Close & recv got error.", zap.Error(err))
		return err
	}
	if repl.Version == version {
		return nil
	} else {
		return errors.New("version does not match")
	}
}

// do loss less sync transfer
func (s *SyncRoutine) Sync() {
	log := common.Log()
	// start gRPC bi-directional stream
	ctx := metadata.AppendToOutgoingContext(context.Background(), HEADER_CLIENT_WORKER_ID, strconv.Itoa(int(s.id)))
	stream, err := s.conn.Sync(ctx)
	if err != nil {
		log.Error("Failed to open gRPC stream.", zap.Error(err))
		return
	}
	log.Sugar().Infof("Sync routine for %s started", s.name)
	stopChan := make(chan struct{})
	// goroutine to receive reply
	go func() {
		for {
			select {
			case <-stopChan:
				return
			default:
				reply, err := stream.Recv()
				if err != nil {
					log.Warn("Failed to retrieve reply.", zap.Error(err))
					return
				}
				s.Condition.L.Lock()
				if s.Version < reply.Version {
					s.Version = reply.Version
					s.Condition.Broadcast()
					s.Condition.L.Unlock()
				} else {
					s.Condition.L.Unlock()
				}
			}
		}
	}()
	for {
		select {
		case ent := <-s.EntryCh:
			if err := stream.Send(ent); err != nil {
				log.Error("Failed to send backup entry.", zap.Error(err))
				close(stopChan)
				return
			}
		case <-s.StopCh:
			close(stopChan)
			return
		}
	}
}
