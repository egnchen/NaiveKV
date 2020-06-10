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
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc"
	"log"
	"net"
	"strings"
	"sync"
)

var (
	hostname   = flag.String("hostname", "localhost", "The server's hostname")
	port       = flag.Int("port", 7900, "The server port")
	zk_servers = strings.Fields(*flag.String("zk-servers", "localhost:2181",
		"Zookeeper server cluster, separated by space"))
	zk_node_root = "/kv/nodes"
	zk_node_name = "worker"
)

var (
	conn   *zk.Conn
	data   map[string]string = make(map[string]string)
	rwlock sync.RWMutex
)

type WorkerServer struct {
	pb.UnimplementedKVWorkerServer
}

var _worker *WorkerServer

func getWorkerServer() *WorkerServer {
	if _worker == nil {
		_worker = &WorkerServer{
			UnimplementedKVWorkerServer: pb.UnimplementedKVWorkerServer{},
		}
	}
	return _worker
}

func (s *WorkerServer) Put(_ context.Context, pair *pb.KVPair) (*pb.PutResponse, error) {
	rwlock.Lock()
	data[pair.Key] = pair.Value
	rwlock.Unlock()
	return &pb.PutResponse{Status: pb.Status_OK}, nil
}

func (s *WorkerServer) Get(_ context.Context, key *pb.Key) (*pb.GetResponse, error) {
	rwlock.RLock()
	value, ok := data[key.Key]
	rwlock.RUnlock()
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
	rwlock.Lock()
	_, ok := data[key.Key]
	if ok {
		delete(data, key.Key)
	}
	rwlock.Unlock()
	if ok {
		return &pb.DeleteResponse{Status: pb.Status_OK}, nil
	} else {
		return &pb.DeleteResponse{Status: pb.Status_ENOENT}, nil
	}
}

func registerToZk(conn *zk.Conn) error {
	// don't have to ensure that the path exist here
	// since we're merely a worker
	nodePath := zk_node_root + "/" + zk_node_name
	exists, _, err := conn.Exists(zk_node_root)
	if err != nil {
		log.Fatalln("Worker: Failed to check whether root node exists.")
	} else if !exists {
		log.Fatalln("Worker: Root node doesn't exist yet.")
	}
	data := common.GetWorkerNodeData(*hostname, *port)
	b, err := json.Marshal(data)
	if err != nil {
		log.Fatalln("Worker: Failed to marshall into json object.")
	}
	if _, err = conn.CreateProtectedEphemeralSequential(nodePath, b, zk.WorldACL(zk.PermAll)); err != nil {
		log.Fatalln("Worker: Failed to register itself to zookeeper.")
	}
	log.Println("Worker: Registration complete")
	return nil
}

func runGrpcServer() (*grpc.Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("Worker: Failed to listen to port %d.\n", *port)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterKVWorkerServer(grpcServer, getWorkerServer())

	log.Println("Worker: Starting gRPC server @ ...")
	grpcServer.Serve(listener)
	return grpcServer, nil
}

func main() {
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

	// connect to zookeeper & register itself
	c, err := common.ConnectToZk(zk_servers)
	if err != nil {
		log.Fatalf("Failed to connect too zookeeper cluster: %v\n", err)
	}
	conn = c
	defer conn.Close()
	if err := registerToZk(conn); err != nil {
		log.Fatalf("Failed to register to zookeeper cluster: %v\n", err)
	}

	// run gRPC server
	if _, err = runGrpcServer(); err != nil {
		log.Fatalf("Failed to run gRPC server: %v\n", err)
	}
}
