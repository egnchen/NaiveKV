package worker

import (
	"context"
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
	Condition sync.Cond
	StopCh    chan struct{}
}

func (s *SyncRoutine) Loop() {
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
