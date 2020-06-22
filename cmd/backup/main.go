// WorkerNode for the distributed KV store
// WorkerNode is data node. It stores the actual KV hashmap and responses to
// clients' requests
package main

import (
	"flag"
	"github.com/eyeKill/KV/common"
	"github.com/eyeKill/KV/worker"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	filePath  = flag.String("path", ".", "Path for persistent log and slot file.")
	zkServers = strings.Fields(*flag.String("zk-servers", "localhost:2181",
		"Zookeeper server cluster, separated by space"))
	workerId = flag.Int("id", 0, "The worker id to backup.")
)

var (
	log  *zap.Logger
	conn *zk.Conn
)

// handle ctrl-c gracefully
func setupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Info("Ctrl-C captured.")
		if conn != nil {
			log.Info("Closing zookeeper connection...")
			conn.Close()
		}
		os.Exit(1)
	}()
}

func main() {
	setupCloseHandler()
	log = common.Log()
	flag.Parse()

	if *workerId == 0 {
		log.Panic("Please specify a valid worker id.")
	}

	// connect to zookeeper & register itself
	conn, err := common.ConnectToZk(zkServers)
	if err != nil {
		log.Panic("Failed to connect too zookeeper.", zap.Error(err))
	}
	defer conn.Close()
	log.Info("Connected to zookeeper.", zap.String("server", conn.Server()))

	// initialize backup server
	backupServer, err := worker.NewBackupWorker(*filePath, common.WorkerId(*workerId))
	if err != nil {
		log.Panic("Failed to initialize backupServer object.", zap.Error(err))
	}

	if err := backupServer.Connect(conn); err != nil {
		log.Panic("Failed to get primary node metadata from zookeeper.", zap.Error(err))
	}

	backupServer.DoBackup()
}
