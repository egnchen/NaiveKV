// MasterServer for the distributed KV store
// MasterServer is mainly responsible for metadata. It tells the client which data node
// it should ask for the given request.

package main

import (
	"flag"
	"fmt"
	"github.com/eyeKill/KV/common"
	"github.com/eyeKill/KV/master"
	pb "github.com/eyeKill/KV/proto"
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
	port      = flag.Int("port", 7899, "The server port")
	zkServers = strings.Fields(*flag.String("zk-servers", "localhost:2181",
		"Zookeeper server cluster, separated by space"))
)

var (
	conn     *zk.Conn
	server   *grpc.Server
	stopChan = make(chan struct{})
	log      *zap.Logger
)

// handle ctrl-c gracefully
func setupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		log.Info("Ctrl-C captured.")
		log.Info("Sending stop to watch loop...")
		stopChan <- struct{}{}
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
		log.Panic("Failed to connect to zookeeper.", zap.Error(err))
	}
	defer conn.Close()
	log.Info("Connected to zookeeper.", zap.String("server", conn.Server()))

	m := master.NewMasterServer(*hostname, uint16(*port))
	// register master to zookeeper & start watching
	if err := m.RegisterToZk(conn); err != nil {
		log.Panic("Failed to register to zookeeper.", zap.Error(err))
	}
	log.Info("Registration complete.")
	go m.Watch(stopChan)

	// open tcp socket
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Panic("failed to listen to port.", zap.Int("port", *port), zap.Error(err))
	}
	// create, register & start gRPC server
	server := common.NewGrpcServer()
	pb.RegisterKVMasterServer(server, &m)
	defer server.GracefulStop()
	if err := server.Serve(listener); err != nil {
		log.Error("gRPC server raised error.", zap.Error(err))
	}
}
