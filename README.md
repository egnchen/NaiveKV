# KV

This is a distributed key-value service implemented in Go.

## Getting started

### Compile protobuf

This project uses gRPC. To compile the protobuf definitions, you'll need `protoc` and Go plugin for it.
Check out https://grpc.io/docs/languages/go/quickstart/ for detail.

You can compile project's protobuf with:

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
go run cmd/master/main.go
```

To start worker server:

```bash
go run cmd/worker/main.go
```

To start a client, which is a REPL:

```bash
go run cmd/client/main.go
```

To start a zookeeper CLI for debug:

```bash
docker run -it --rm --link kv-zookeeper:zookeeper eyek/kv-zookeeper:1.0 zkCli.sh -server zookeeper
```