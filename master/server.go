// Master for the distributed KV store
// Master is mainly responsible for metadata. It tells the client which data node
// it should ask for the given request.

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
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	hostname   = flag.String("hostname", "localhost", "The server's hostname")
	port       = flag.Int("port", 7899, "The server port")
	zk_servers = strings.Fields(*flag.String("zk-servers", "localhost:2181",
		"Zookeeper server cluster, separated by space"))
	zk_node_root = "/kv/nodes"
	zk_node_name = "master"
)

var (
	masters  map[string]common.Node
	workers  map[string]common.Node
	rwlock   sync.RWMutex
	conn     *zk.Conn
	server   *grpc.Server
	stopChan chan struct{}
)

type MasterServer struct {
	pb.UnimplementedKVMasterServer
}

var _server *MasterServer

func getMasterServer() *MasterServer {
	if _server == nil {
		_server = &MasterServer{
			UnimplementedKVMasterServer: pb.UnimplementedKVMasterServer{},
		}
	}
	return _server
}

// Assume that we only have one node
// Return that node
func (s *MasterServer) GetWorker(ctx context.Context, key *pb.Key) (*pb.GetWorkerResponse, error) {
	rwlock.RLock()
	defer rwlock.RUnlock()
	if len(workers) == 0 {
		return &pb.GetWorkerResponse{
			Status: pb.Status_ENOSERVER,
		}, nil
	} else {
		var ret common.Node
		for _, v := range workers {
			ret = v
			break
		}
		log.Printf("Responding %s:%d\n", ret.Hostname, ret.Port)
		return &pb.GetWorkerResponse{
			Status: pb.Status_OK,
			Worker: &pb.Worker{
				Hostname: ret.Hostname,
				Port:     int32(ret.Port),
			},
		}, nil
	}
}

func registerToZk(conn *zk.Conn) error {
	// ensure root path exist first
	if err := common.EnsurePath(conn, "/kv"); err != nil {
		return err
	}
	if err := common.EnsurePath(conn, "/kv/nodes"); err != nil {
		return err
	}
	nodePath := zk_node_root + "/" + zk_node_name
	data := common.GetMasterNodeData(*hostname, *port)
	b, err := json.Marshal(data)
	if err != nil {
		log.Fatalln("Master: Failed to marshall into json object.")
	}
	if _, err = conn.CreateProtectedEphemeralSequential(nodePath, b, zk.WorldACL(zk.PermAll)); err != nil {
		log.Fatalln("Master: Failed to register itself to zookeeper.")
	}
	log.Println("Master: Registration complete")
	return nil
}

// Keep watching the given path
// until a signal is sent from the stopChan channel.
func watchLoop(path string, stopChan <-chan struct{}) {
	log.Println("Master: Starting watch loop.")
	for {
		children, _, eventChan, err := conn.ChildrenW(path)
		if err != nil {
			log.Printf("Master: Error occured while listening to %s: %v\n", path, err)
		}
		// update local metadata cache
		rwlock.Lock()
		masters = make(map[string]common.Node)
		workers = make(map[string]common.Node)
		for _, chName := range children {
			var data common.Node
			chPath := path + "/" + chName
			b, _, err := conn.Get(chPath)
			if err != nil {
				log.Printf("Master: Failed to retrieve data for %s", chPath)
				continue
			}
			if err := json.Unmarshal(b, &data); err != nil {
				log.Printf("Master: Data for %s is invalid: %s", chPath, b)
				continue
			}
			if data.Type == "master" {
				masters[chPath] = data
			} else if data.Type == "worker" {
				workers[chPath] = data
			} else {
				log.Printf("Master: Node type is invalid: %s.\n", data.Type)
			}
		}
		rwlock.Unlock()
		select {
		case event := <-eventChan:
			{
				log.Printf("Received event: %v\n", event)
			}
		case <-stopChan:
			{
				log.Println("Watch loop exiting...")
				break
			}
		}
	}
}

func runGrpcServer() (*grpc.Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("Master: failed to listen to port %d.\n", *port)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterKVMasterServer(grpcServer, getMasterServer())
	log.Println("Master: Starting gRPC server...")
	go grpcServer.Serve(listener)
	return grpcServer, nil
}

// handle ctrl-c gracefully
func setupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Ctrl-C captured.")
		log.Println("Sending stop to watch loop...")
		stopChan <- struct{}{}
		log.Println("Stop sent")
		if server != nil {
			log.Println("Gracefully stopping gRPC server...")
			server.GracefulStop()
		}
		if conn != nil {
			log.Println("Closing zookeeper connection...")
			conn.Close()
		}
	}()
}

func main() {
	// parse flag
	flag.Parse()
	//if len(*hostname) == 0 {
	//	n, err := os.Hostname()
	//	if err != nil {
	//		log.Fatalf("Cannot get default hostname. Try to specify it in command line.")
	//	}
	//	hostname = &n
	//}

	// set up graceful handler for ctrl-c
	setupCloseHandler()

	// connect to zookeeper & register itself
	c, err := common.ConnectToZk(zk_servers)
	if err != nil {
		log.Fatalf("Failed to connect to zookeeper cluster: %v\n", err)
	}
	// transfer it to the global variable
	conn = c
	defer func() {
		log.Println("Closing connection with zookeeper...")
		conn.Close()
	}()
	// register itself to zookeeper
	if err := registerToZk(conn); err != nil {
		log.Fatalf("Failed to register to zookeeper cluster: %v", err)
	}
	// start watching
	go watchLoop(zk_node_root, stopChan)

	// run gRPC server
	server, err = runGrpcServer()
	if err != nil {
		log.Fatalf("Failed to run gRPC server: %v", err)
	}
	defer func() {
		log.Println("Gracefully exiting gRPC server...")
		server.GracefulStop()
	}()

	// May you rest in a deep and restless slumber
	for {
		time.Sleep(10 * time.Second)
	}
}
