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
	"strings"
	"sync"
	"time"
)


var (
	hostname	= flag.String("hostname", "", "The server's hostname")
	port		= flag.Int("port", 7978, "The server port")
	zk_servers	= strings.Fields(*flag.String("zk-servers", "localhost:2181",
		"Zookeeper server cluster, separated by space"))
	zk_node_root= "/kv/nodes"
	zk_node_name= "master"
)

var _server *MasterServer
func getMasterServer() *MasterServer {
	if _server == nil {
		_server = &MasterServer{
			UnimplementedKVMasterServer: pb.UnimplementedKVMasterServer{},
			masters:                     make(map[string]common.Node),
			workers:                     make(map[string]common.Node),
			mu:                          sync.RWMutex{},
			conn:                        nil,
		}
	}
	return _server
}

type MasterServer struct {
	pb.UnimplementedKVMasterServer
	masters map[string]common.Node
	workers map[string]common.Node
	mu sync.RWMutex
	conn *zk.Conn
}

// Assume that we only have one node
// Return that node
func (s *MasterServer) GetWorker(ctx context.Context, key *pb.Key) (*pb.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.workers) == 0 {
		var node = pb.Worker{
			Hostname: "localhost",
			Port: 1234,
		}
		return &node, nil
	} else {
		var ret common.Node
		for _, v := range s.workers {
			ret = v
			break
		}
		var node = pb.Worker{
			Hostname: ret.Hostname,
			Port: int32(ret.Port),
		}
		return &node, nil
	}
}

func connectToZk() (*zk.Conn, error) {
	log.Println("Connecting to zookeeper cluster...")
	conn, _, err := zk.Connect(zk_servers, time.Second * 5)
	return conn, err
}

func ensurePath(conn *zk.Conn, path string) error {
	exists, _, err := conn.Exists(path)
	if err != nil {
		return err
	}
	if !exists {
		_, err = conn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

func registerToZk(conn *zk.Conn) error {
	// ensure root path exist first
	if err := ensurePath(conn, "/kv"); err != nil {
		return err
	}
	if err := ensurePath(conn, "/kv/nodes"); err != nil {
		return err
	}
	nodePath := zk_node_root + "/" + zk_node_name
	data := common.GetMasterNodeData(*hostname, *port)
	b, err := json.Marshal(data)
	if err != nil {
		log.Fatalln("Master: Failed to marshall into json object.");
	}
	if _, err = conn.CreateProtectedEphemeralSequential(nodePath, b, zk.WorldACL(zk.PermAll));
		err != nil {
		log.Fatalln("Master: Failed to register itself to zookeeper.")
	}
	log.Println("Master: Registration complete")
	return nil
}

func startWatch(path string, watcher func(ch <-chan zk.Event)) error {
	conn := getMasterServer().conn
	children, stat, childCh, err := conn.ChildrenW(path)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Printf("Master: Watching path %s\n", path)
	for idx, ch := range children {
		log.Printf("chilren: %d, %s\n", idx, ch)
	}
	log.Printf("watch children state:\n%s\n", common.ZkStateString(stat))
	go watcher(childCh)
	return nil

}

func watchRoot(ch <-chan zk.Event) {
	server := getMasterServer()
	for {
		event := <-ch
		log.Println("path:", event.Path)
		log.Println("type:", event.Type.String())
		log.Println("state:", event.State.String())
		if event.Type == zk.EventNodeCreated || event.Type == zk.EventNodeDataChanged {
			if event.Type == zk.EventNodeCreated {
				log.Printf("A node has been created: %s\n", event.Path)
			} else if event.Type == zk.EventNodeDataChanged {
				log.Printf("A node has been changed: %s\n", event.Path)
			}
			ret, _, err := server.conn.Get(event.Path)
			if err != nil {
				log.Printf("Error: Failed to retrieve node %s.\n",event.Path)
			} else {
				var data common.Node
				if err := json.Unmarshal(ret, &data); err != nil {
					log.Printf("Error: Invalid content: %s\n", ret)
				} else {
					server.mu.Lock()
					if data.Type == "master" {
						server.masters[event.Path] = data
					} else {
						server.workers[event.Path] = data
					}
					server.mu.Unlock()
				}
			}
		} else if event.Type == zk.EventNodeDeleted {
			log.Printf("A node has been deleted: %s\n", event.Path)
			server.mu.Lock()
			if _, ok := server.masters[event.Path]; ok {
				delete(server.masters, event.Path)
			} else if _, ok := server.workers[event.Path]; ok {
				delete(server.workers, event.Path)
			} else {
				fmt.Printf("Error: Cannot find the node(%s) to delete in local metadata.\n", event.Path);
			}
			server.mu.Unlock()
		}
		server.mu.Unlock()
	}
}

func startGrpcServer() (*grpc.Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("Master: failed to listen to port %d.\n", *port)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterKVMasterServer(grpcServer, getMasterServer())
	go grpcServer.Serve(listener)
	log.Println("Master: gRPC server successfully started.")
	return grpcServer, nil
}

func main() {
	// parse flag
	flag.Parse()
	if len(*hostname) == 0 {
		n, err := os.Hostname()
		if err != nil {
			log.Fatalf("Cannot get default hostname. Try to specify it in command line.")
		}
		hostname = &n
	}

	// connect to zookeeper & register itself
	conn, err := connectToZk()
	if err != nil {
		log.Fatalf("Failed to connect to zookeeper cluster: %v", err)
	}
	defer conn.Close()
	if err := registerToZk(conn); err != nil {
		log.Fatalf("Failed to register to zookeeper cluster: %v", err)
	}

	// save the connection to master server struct
	getMasterServer().conn = conn

	// start gRPC server
	if _, err := startGrpcServer(); err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}

	// start watching
	if err := startWatch(zk_node_root, watchRoot); err != nil {
		log.Fatalf("Failed to watch path %s\n", zk_node_root);
	}

}