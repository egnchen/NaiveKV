package worker

import (
	"fmt"
	"github.com/eyeKill/KV/common"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"path"
	"sort"
	"strconv"
	"strings"
)

func (s *WorkerServer) registerBackup() error {
	// backup do not have to ensure that path exists
	// just register itself will do
	node := common.NewWorkerNode(s.Hostname, s.Port, s.Id)
	nodePath := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)), common.ZK_BACKUP_WORKER_NAME)
	if _, err := common.ZkCreate(s.conn, nodePath, node, true, true); err != nil {
		return err
	}
	return nil
}

// Watch current worker node. Fire up backup election if primary worker is down.
func (s *WorkerServer) backupWatch(worker common.Worker) {
	log := common.SugaredLog()
	if len(worker.Primaries) == 0 {
		log.Info("Primary down detected, beginning primary election.")
		if err := s.backupElection(); err != nil {
			log.Error("Failed to perform backup election.", zap.Error(err))
		}
	}
}


func (s *WorkerServer) backupElection() error {
	log := common.SugaredLog()
	electionPath := path.Join(common.ZK_ELECTION_ROOT, strconv.Itoa(int(s.Id)))
	if err := common.EnsurePath(s.conn, electionPath); err != nil {
		log.Error("Failed to ensure path.", zap.Error(err))
		return err
	}
	// register itself and watch
	myName := fmt.Sprintf("%020d%s", s.version, s.NodeName)
	log.Infof("Name to assign: %s\n", myName)
	_, err := s.conn.Create(path.Join(electionPath, myName), []byte(s.NodeName), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Error("Failed to create node.", zap.Error(err))
		return err
	}
	// get number of backup nodes
	workerPath := path.Join(common.ZK_WORKERS_ROOT, strconv.Itoa(int(s.Id)))
	workers, _, err := s.conn.Children(workerPath)
	if err != nil {
		return err
	}
	backups := common.FilterString(workers, func(i int) bool { return strings.Contains(workers[i], common.ZK_BACKUP_WORKER_NAME) })
	// watch the znode
	log.Infof("Should have %d backups.", len(backups))
	for {
		children, _, eventChan, err := s.conn.ChildrenW(electionPath)
		if err != nil {
			log.Error("Failed to watch election path", zap.Error(err))
			return nil
		}
		if len(children) == len(backups) {
			sort.Strings(children)
			if children[0] == myName {
				// i am the lucky guy
				log.Infof("Seems like I am the lucky guy, upgrading...")
				if err := s.transformTo(MODE_PRIMARY); err != nil {
					log.Error("Transformation failed.", zap.Error(err))
					return err
				}
				log.Infof("Transformation successful.")
			} else {
				log.Infof("Not so lucky this time...")
			}
			break
		}
		_ = <-eventChan
	}
	return nil
}

// do nothing
func (s *WorkerServer) backupWatchMigration(stopChan chan struct{}) {
	<-stopChan
	return
}