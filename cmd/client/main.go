// Client for the distributed KV store
// This is mainly a wrapper of gRPC.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	pb "github.com/eyeKill/KV/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"os"
	"strconv"
	"strings"
	"time"
)

const HELP_STRING = `Usage:
put <key> <value>
get <key>
delete <key>
exit
quit
`

var (
	serverAddr = flag.String("addr", "localhost:7899", "Address of the master")
)

var (
	log *zap.Logger
)

// collection of grpc clients
// note that these are interfaces, so no pointers
var (
	masterClient  pb.KVMasterClient
	workerClients map[string]pb.KVWorkerClient = make(map[string]pb.KVWorkerClient)
)

func getWorkerClient(key string) (pb.KVWorkerClient, error) {
	if masterClient == nil {
		return nil, errors.New("master client not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	k := pb.Key{Key: key}
	resp, err := masterClient.GetWorker(ctx, &k)
	if err != nil {
		log.Error("Failed to get worker node.",
			zap.String("key", key), zap.Error(err))
		return nil, err
	}
	if resp.Status != pb.Status_OK {
		log.Error("RPC failed.",
			zap.String("status", pb.Status_name[int32(resp.Status)]), zap.Error(err))
		return nil, err
	}
	// get client from client pool,
	// create one if not found.
	connString := resp.Worker.Hostname + ":" + strconv.Itoa(int(resp.Worker.Port))
	ret, ok := workerClients[connString]
	if !ok {
		// connect
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithInsecure())
		opts = append(opts, grpc.WithBlock())

		log.Info("Dialing...", zap.String("server", connString))
		conn, err := grpc.Dial(connString, opts...)
		if err != nil {
			log.Error("Failed to dail.",
				zap.String("server", connString), zap.Error(err))
			return nil, err
		}
		log.Info("Connected.", zap.Any("conn", conn))
		ret = pb.NewKVWorkerClient(conn)
		workerClients[connString] = ret
	}
	return ret, nil
}

func doPut(key string, value string) error {
	workerClient, err := getWorkerClient(key)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pair := pb.KVPair{
		Key:   key,
		Value: value,
	}
	resp, err := workerClient.Put(ctx, &pair)
	if err != nil {
		return err
	}
	if resp.Status != pb.Status_OK {
		return errors.New(pb.Status_name[int32(resp.Status)])
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
	k := pb.Key{Key: key}
	resp, err := workerClient.Get(ctx, &k)
	if err != nil {
		return "", err
	}
	if resp.Status != pb.Status_OK {
		return "", errors.New(pb.Status_name[int32(resp.Status)])
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
	k := pb.Key{Key: key}
	resp, err := workerClient.Delete(ctx, &k)
	if err != nil {
		return err
	}
	if resp.Status != pb.Status_OK {
		return errors.New(pb.Status_name[int32(resp.Status)])
	} else {
		return nil
	}
}

// main function is a REPL loop
func main() {
	// get logger
	l, err := zap.NewDevelopment()
	if err != nil {
		fmt.Println("Failed to get logger.")
		return
	}
	log = l

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
	defer conn.Close()
	masterClient = pb.NewKVMasterClient(conn)

	// REPL
	// bufio.Scanner split tokens by '\n' by default
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(">>> ")
		scanner.Scan()
		input := scanner.Text()
		fields := strings.Fields(input)
		if len(fields) == 0 {
			break
		}
		switch fields[0] {
		case "put":
			{
				if len(fields) != 3 {
					fmt.Println("Usage: put <key> <value>")
					continue
				}
				if err := doPut(fields[1], fields[2]); err != nil {
					fmt.Printf("Put %s failed: %v", fields[1], err)
				} else {
					fmt.Println("OK")
				}
			}
		case "get":
			{
				if len(fields) != 2 {
					fmt.Println("Usage: get <key>")
					continue
				}
				value, err := doGet(fields[1])
				if err != nil {
					fmt.Printf("Get <%s> failed: %v\n", fields[1], err)
				} else {
					fmt.Printf("%s -> %s\n", fields[1], value)
				}
			}
		case "delete":
			{
				if len(fields) != 2 {
					fmt.Println("Usage: delete <key>")
					continue
				}
				if err := doDelete(fields[1]); err != nil {
					fmt.Printf("Delete <%s> failed: %v\n", fields[1], err)
				} else {
					fmt.Println("OK")
				}
			}
		case "help":
			{
				fmt.Print(HELP_STRING)
			}
		case "exit", "quit":
			{
				fmt.Println("Goodbye")
				return
			}
		default:
			{
				fmt.Printf("Illegal op \"%s\"\n", fields[0])
				fmt.Print(HELP_STRING)
			}
		}
	}

}
