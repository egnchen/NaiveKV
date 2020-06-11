# KV

This is a distributed key-value service implemented in Go.

## Getting started

### Compile protobuf

This project uses gRPC. To compile the protobuf into Go stubs & declarations:

```bash
cd proto
protoc *.proto --go_out=plugins=grpc:.
```

### Start processes

This project uses zookeeper to maintain metadata, so start zookeeper first:

```bash
docker run --name kv-zookeeper --restart always -d eyek/kv-zookeeper:1.0
```

To start master server:

```bash
go run master/server.go
```

To start worker server:

```bash
go run worker/server.go
```

To start a client, which is a REPL:

```bash
go run client/client.go
```

To start a zookeeper CLI for debug:

```bash
docker run -it --rm --link kv-zookeeper:zookeeper eyek/kv-zookeeper:1.0 zkCli.sh -server zookeeper
```