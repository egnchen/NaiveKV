package worker

import (
	"context"
	"errors"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"go.uber.org/zap"
	"sync"
)

type SyncRoutine struct {
	name      string
	conn      pb.KVBackupClient
	Mask      func(string) bool
	EntryCh   chan *pb.BackupEntry
	Version   uint64
	Condition *sync.Cond
	Syncing   bool
	StopCh    chan struct{}
}

func NewSyncRoutine(name string, client pb.KVBackupClient, mask func(string) bool) SyncRoutine {
	return SyncRoutine{
		name:      name,
		conn:      client,
		Mask:      mask,
		EntryCh:   make(chan *pb.BackupEntry),
		Version:   0,
		Condition: sync.NewCond(&sync.Mutex{}),
		Syncing:   false,
		StopCh:    make(chan struct{}),
	}
}

// do bulk transfer
// the transferred data would be content plus newly added items from the entry channel
func (s *SyncRoutine) Prepare(content map[string]ValueWithVersion) error {
	log := common.Log()
	log.Info("Doing bulk transfer.", zap.Int("length", len(content)), zap.String("destination", s.name))
	ctx := context.Background()
	client, err := s.conn.Transfer(ctx)
	if err != nil {
		return err
	}
	var version uint64 = 0
	for k, v := range content {
		if err := client.Send(&pb.BackupEntry{
			Op:      pb.Operation_PUT,
			Version: v.Version,
			Key:     k,
			Value:   *v.Value,
		}); err != nil {
			log.Warn("Connection interrupted.", zap.Error(err))
			break
		}
		if version < v.Version {
			version = v.Version
		}
	}
	for len(s.EntryCh) > 0 {
		ent := <-s.EntryCh
		if err := client.Send(ent); err != nil {
			log.Warn("Connection interrupted.", zap.Error(err))
			break
		}
		version = ent.Version
	}
	repl, err := client.CloseAndRecv()
	if err != nil {
		return err
	}
	if repl.Version == version {
		return nil
	} else {
		return errors.New("version does not match")
	}
}

func (s *SyncRoutine) Sync() {
	log := common.Log()
	// start gRPC bi-directional stream
	stream, err := s.conn.Sync(context.Background())
	if err != nil {
		log.Error("Failed to open gRPC stream.", zap.Error(err))
		return
	}
	log.Sugar().Infof("Backup routine for %s started", s.name)
	stop := make(chan struct{})
	// goroutine to receive reply
	go func() {
	recvLoop:
		for {
			select {
			case <-stop:
				break recvLoop
			default:
				reply, err := stream.Recv()
				if err != nil {
					log.Error("Failed to retrieve reply.", zap.Error(err))
					break recvLoop
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
backupRoutine:
	for {
		select {
		case ent := <-s.EntryCh:
			if err := stream.Send(ent); err != nil {
				log.Error("Failed to send backup entry.", zap.Error(err))
				break backupRoutine
			}
		case <-s.StopCh:
			close(stop)
			break backupRoutine
		}
	}
}
