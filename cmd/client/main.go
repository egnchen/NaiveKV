// Client for the distributed KV store
// This is mainly a wrapper of gRPC.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/eyeKill/KV/common"
	pb "github.com/eyeKill/KV/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"strings"
	"sync"
	"time"
)

const HELP_STRING = `Welcome to NaiveKV.
Usages:
* put <key> <value>
* get <key>
* delete <key>
* exit
* quit
`

// configurations
var (
	serverAddr = flag.String("addr", "localhost:7899", "Address of the master")
)

var (
	log *zap.Logger
	sLock sync.RWMutex
	slots common.HashSlotRing
	slotVersion uint32
)

// collection of grpc clients
// note that these are interfaces, so no pointers
var (
	masterClient          pb.KVMasterClient
	workerClients         = make(map[common.WorkerId]pb.KVWorkerClient)
	//workerInternalClients = make(map[string]pb.KVWorkerInternalClient)
)

func getWorkerClient(key string) (pb.KVWorkerClient, error) {
	// look it up in local slot cache
	sLock.RLock()
	id := slots.GetWorkerIdByKey(key)
	sLock.RUnlock()
	ret, ok := workerClients[id]
	if ok {
		return ret, nil
	}
	// get worker information from remote otherwise
	resp, err := masterClient.GetWorkerById(context.Background(), &pb.WorkerId{Id: uint32(id)})
	if err != nil {
		return nil, err
	}
	if resp.Status != pb.Status_OK {
		return nil, errors.New(fmt.Sprintf("Failed to get worker, remote responded %s", pb.Status_name[int32(resp.Status)]))
	}
	connString := fmt.Sprintf("%s:%d", resp.Worker.Hostname, resp.Worker.Port)
	conn, err := common.ConnectGrpc(connString)
	if err != nil {
		log.Error("Failed to connect to worker.", zap.Error(err))
		return nil, err
	}
	log.Info("Connected.", zap.Any("conn", conn))
	ret = pb.NewKVWorkerClient(conn)
	workerClients[id] = ret
	return ret, nil
}

func UpdateNewestSlots() {
	log.Info("Updating newest slot file...")
	resp, err := masterClient.GetSlots(context.Background(), &empty.Empty{})
	if err != nil {
		log.Error("Failed to get latest slots.", zap.Error(err))
	}
	sLock.Lock()
	defer sLock.Unlock()
	slotVersion = resp.Version
	slots = make(common.HashSlotRing, len(resp.SlotTable))
	for i, v := range resp.SlotTable {
		slots[i] = common.WorkerId(v.Id)
	}
}

func doPut(key string, value string) error {
	workerClient, err := getWorkerClient(key)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sLock.RLock()
	pair := pb.KVPair{
		Key:   key,
		Value: value,
		SlotVersion: slotVersion,
	}
	sLock.RUnlock()
	resp, err := workerClient.Put(ctx, &pair)
	if err != nil {
		if HandleError(err, key) {
			return doPut(key, value)
		} else {
			return err
		}
	}
	if resp.Status == pb.Status_EINVVERSION {
		// have got to update slot table
		UpdateNewestSlots()
		return doPut(key, value)	// a little dangerous
	} else if resp.Status == pb.Status_EINVSERVER {
		id := slots.GetWorkerIdByKey(key)
		delete(workerClients, id)
		return doPut(key, value)
	} else if resp.Status != pb.Status_OK {
		return errors.New(fmt.Sprintf(
			"RPC returned %s.", pb.Status_name[int32(resp.Status)]))
	} else {
		return nil
	}
}

func doGet(key string) (string, error) {
	workerClient, err := getWorkerClient(key)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sLock.RLock()
	k := pb.Key{
		Key:   key,
		SlotVersion: slotVersion,
	}
	sLock.RUnlock()
	resp, err := workerClient.Get(ctx, &k)
	//fmt.Printf("%+v | %+v\n", resp, err)
	if err != nil {
		if HandleError(err, key) {
			return doGet(key)
		} else {
			return "", err
		}
	}
	if resp.Status == pb.Status_EINVVERSION {
		// have got to update slot table
		UpdateNewestSlots()
		return doGet(key)
	} else if resp.Status == pb.Status_EINVSERVER {
		id := slots.GetWorkerIdByKey(key)
		delete(workerClients, id)
		return doGet(key)
	} else if resp.Status != pb.Status_OK {
		return "", errors.New(fmt.Sprintf(
			"RPC returned %s.", pb.Status_name[int32(resp.Status)]))
	} else {
		return resp.Value, nil
	}
}

func doDelete(key string) error {
	workerClient, err := getWorkerClient(key)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sLock.RLock()
	k := pb.Key{
		Key:   key,
		SlotVersion: slotVersion,
	}
	sLock.RUnlock()
	resp, err := workerClient.Delete(ctx, &k)
	if err != nil {
		if HandleError(err, key) {
			return doDelete(key)
		} else {
			return err
		}
	}
	if resp.Status == pb.Status_EINVVERSION {
		// have got to update slot table
		UpdateNewestSlots()
		return doDelete(key)	// a little dangerous
	} else if resp.Status == pb.Status_EINVSERVER {
		id := slots.GetWorkerIdByKey(key)
		delete(workerClients, id)
		return doDelete(key)
	} else if resp.Status != pb.Status_OK {
		return errors.New(fmt.Sprintf(
			"RPC returned %s.", pb.Status_name[int32(resp.Status)]))
	} else {
		return nil
	}
}

func HandleError(err error, key string) bool {
	id := slots.GetWorkerIdByKey(key)
	log.Info("Handling error...", zap.Error(err))
	code := status.Code(err)
	if code == codes.Unavailable {
		// remove this connection
		delete(workerClients, id)
		return true
	} else {
		return false
	}
}

// main function is a REPL loop
func main() {
	// get logger
	log = common.Log()

	flag.Parse()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())

	log.Info("Dialing...", zap.String("server", *serverAddr))
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Panic("Failed to dial.",
			zap.String("server", *serverAddr), zap.Error(err))
	} else {
		log.Info("Connected.", zap.String("server", *serverAddr))
	}
	masterClient = pb.NewKVMasterClient(conn)

	// get first version of slots
	UpdateNewestSlots()

	// REPL
	// bufio.Scanner split tokens by '\n' by default
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print(">>> ")
	for scanner.Scan() {
		input := scanner.Text()
		fields := strings.Fields(input)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "put":
			if len(fields) != 3 {
				fmt.Println("Usage: put <key> <value>")
				break
			}
			if err := doPut(fields[1], fields[2]); err != nil {
				fmt.Printf("Put %s failed: %v", fields[1], err)
			} else {
				fmt.Println("OK")
			}
		case "get":
			if len(fields) != 2 {
				fmt.Println("Usage: get <key>")
				break
			}
			value, err := doGet(fields[1])
			if err != nil {
				fmt.Printf("Get <%s> failed: %v\n", fields[1], err)
			} else {
				fmt.Printf("%s -> %s\n", fields[1], value)
			}
		case "delete":
			if len(fields) != 2 {
				fmt.Println("Usage: delete <key>")
				break
			}
			if err := doDelete(fields[1]); err != nil {
				fmt.Printf("Delete <%s> failed: %v\n", fields[1], err)
			} else {
				fmt.Println("OK")
			}
		case "help":
			fmt.Print(HELP_STRING)
		case "exit", "quit":
			fmt.Println("Goodbye")
			return
		default:
			fmt.Printf("Illegal op \"%s\"\n", fields[0])
			fmt.Print(HELP_STRING)
		}
		fmt.Print(">>> ")
	}

}
