// Worker for the distributed KV store
// Worker is data node. It stores the actual KV hashmap and responses to
// clients' requests
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/eyeKill/KV/worker"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"
)

var (
	hostname  = flag.String("hostname", "localhost", "The server's hostname")
	port      = flag.Int("port", 7900, "The server port")
	filePath  = flag.String("path", "data/", "Path for persistent log and slot file.")
	zkServers = strings.Fields(*flag.String("zk-servers", "localhost:2181",
		"Zookeeper server cluster, separated by space"))
	zkNodeRoot = "/kv/nodes"
	zkNodeName = "worker"
)

var (
	conn   *zk.Conn
	kv     *worker.KVStore
	server *grpc.Server
	log    *zap.Logger
	id     common.WorkerId
)

type WorkerServer struct {
	pb.UnimplementedKVWorkerServer
}

type WorkerInternalServer struct {
	pb.UnimplementedKVWorkerInternalServer
}

func (s *WorkerServer) Put(_ context.Context, pair *pb.KVPair) (*pb.PutResponse, error) {
	kv.Put(pair.Key, pair.Value)
	return &pb.PutResponse{Status: pb.Status_OK}, nil
}

func (s *WorkerServer) Get(_ context.Context, key *pb.Key) (*pb.GetResponse, error) {
	value, ok := kv.Get(key.Key)
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

func (s *WorkerServer) Delete(_ context.Context, key *pb.Key) (*pb.DeleteResponse, error) {
	ok := kv.Delete(key.Key)
	if ok {
		return &pb.DeleteResponse{Status: pb.Status_OK}, nil
	} else {
		return &pb.DeleteResponse{Status: pb.Status_ENOENT}, nil
	}
}

func (s *WorkerInternalServer) Checkpoint(_ context.Context, _ *empty.Empty) (*pb.FlushResponse, error) {
	if err := kv.Checkpoint(); err != nil {
		log.Error("KV flush failed.", zap.Error(err))
		return &pb.FlushResponse{Status: pb.Status_EFAILED}, nil
	}
	return &pb.FlushResponse{Status: pb.Status_OK}, nil
}

func registerToZk(conn *zk.Conn) error {
	// Worker don't have to ensure that path exists.
	nodePath := zkNodeRoot + "/" + zkNodeName
	exists, _, err := conn.Exists(zkNodeRoot)
	if err != nil {
		log.Panic("Failed to check whether root node exists.", zap.Error(err))
	} else if !exists {
		log.Panic("Root node does not exist.", zap.Error(err))
	}

	// get worker id & configuration
	b, err := ioutil.ReadFile(path.Join(*filePath, "config.json"))
	if err != nil && os.IsExist(err) {
		log.Panic("Failed to read config file.", zap.Error(err))
	} else if os.IsNotExist(err) {
		// get new worker id
		distributedInteger := common.DistributedAtomicInteger{Conn: conn, Path: common.ZK_WORKER_ID}
		v, err := distributedInteger.Inc()
		if err != nil {
			log.Panic("Failed to increase DAI.", zap.Error(err))
		}
		id = common.WorkerId(v)
	} else {
		var config worker.WorkerConfig
		if err := json.Unmarshal(b, &config); err != nil {
			log.Panic("Invalid config file. Ignoring.", zap.Error(err))
		}
		id = config.Id
	}
	log.Info("Retrieved/generated configuration.", zap.Uint16("id", uint16(id)))
	data := common.GetNewWorkerNode(*hostname, uint16(*port), id, 10)
	b, err = json.Marshal(data)
	if err != nil {
		log.Panic("Failed to marshall into json object.", zap.Error(err))
	}
	name, err := conn.CreateProtectedEphemeralSequential(nodePath, b, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Panic("Failed to register itself to zookeeper.", zap.Error(err))
	}
	log.Info("Registration complete.", zap.String("name", name))
	// persist config file
	config := worker.WorkerConfig{
		Id:      id,
		Version: 0,
	}
	b, err = json.Marshal(config)
	if err != nil {
		log.Panic("Failed to unmarshall config file.", zap.Error(err))
	}
	if err := ioutil.WriteFile(path.Join(*filePath, "config.json"), b, 0644); err != nil {
		log.Panic("Failed to write config file.", zap.Error(err))
	}
	return nil
}

func getGrpcServer() *grpc.Server {
	if server == nil {
		var opts []grpc.ServerOption
		server = grpc.NewServer(opts...)
	}
	return server
}

func runGrpcServer(server *grpc.Server) error {
	address := fmt.Sprintf("localhost:%d", *port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	log.Info("Starting gRPC server...", zap.Int("port", *port))
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Info("Error from gRPC server.", zap.Error(err))
		}
	}()
	return nil
}

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
	//if len(*hostname) == 0 {
	//	n, err := os.Hostname()
	//	if err != nil {
	//		log.Fatalf("Cannot get default hostname. Try to specify it in command line.")
	//	}
	//	hostname = &n
	//}
	// by default we bind to an arbitrary port
	// this behavior could be changed under environment like docker

	// initialize kv store
	kvStore, err := worker.NewKVStore(*filePath)
	if err != nil {
		log.Panic("Failed to create KVStore object.", zap.Error(err))
	}
	kv = kvStore

	// connect to zookeeper & register itself
	c, err := common.ConnectToZk(zkServers)
	if err != nil {
		log.Panic("Failed to connect too zookeeper cluster.", zap.Error(err))
	}
	log.Info("Connected to zookeeper cluster.", zap.String("server", c.Server()))
	conn = c // transfer to global scope

	defer conn.Close()
	if err := registerToZk(conn); err != nil {
		log.Panic("Failed to register to zookeeper cluster.", zap.Error(err))
	}

	// setup gRPC server & run it
	s := getGrpcServer()
	pb.RegisterKVWorkerServer(s, &WorkerServer{})
	pb.RegisterKVWorkerInternalServer(s, &WorkerInternalServer{})
	if err := runGrpcServer(s); err != nil {
		log.Panic("Failed to run gRPC server.", zap.Error(err))
	}

	// May you rest in a deep and restless slumber
	for {
		time.Sleep(10 * time.Second)
	}
}
