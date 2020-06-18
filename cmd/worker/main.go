// Worker for the distributed KV store
// Worker is data node. It stores the actual KV hashmap and responses to
// clients' requests
package main

import (
	"flag"
	"fmt"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/eyeKill/KV/worker"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	hostname  = flag.String("hostname", "localhost", "The server's hostname")
	port      = flag.Int("port", 7900, "The server port")
	filePath  = flag.String("path", ".", "Path for persistent log and slot file.")
	zkServers = strings.Fields(*flag.String("zk-servers", "localhost:2181",
		"Zookeeper server cluster, separated by space"))
)

var (
	server *grpc.Server
	log    *zap.Logger
	conn   *zk.Conn
)

// handle ctrl-c gracefully
func setupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Info("Ctrl-C captured.")
		if server != nil {
			log.Info("Gracefully stopping gRPC server...")
			server.GracefulStop()
		}
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

	// connect to zookeeper & register itself
	conn, err := common.ConnectToZk(zkServers)
	if err != nil {
		log.Panic("Failed to connect too zookeeper.", zap.Error(err))
	}
	defer conn.Close()
	log.Info("Connected to zookeeper.", zap.String("server", conn.Server()))

	// initialize worker server
	worker, err := worker.NewWorkerServer(*hostname, uint16(*port), *filePath)
	if err != nil {
		log.Panic("Failed to initialize worker object.", zap.Error(err))
	}

	if err := worker.RegisterToZk(conn); err != nil {
		log.Panic("Failed to register to zookeeper.", zap.Error(err))
	}

	// open tcp socket
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Panic("failed to listen to port.", zap.Int("port", *port), zap.Error(err))
	}
	// create, register & start gRPC server
	server := common.NewGrpcServer()
	pb.RegisterKVWorkerServer(server, worker)
	pb.RegisterKVWorkerInternalServer(server, worker)
	defer server.GracefulStop()
	if err := server.Serve(listener); err != nil {
		log.Error("gRPC server raised error.", zap.Error(err))
	}
}
