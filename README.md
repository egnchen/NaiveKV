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
make zookeeper
```

To start master server:

```bash
make master
```

To start primary worker server with id `x`:

```bash
make primary id=x
```

To start a backup server with id `x`:

```bash
make backup id=x
```

To start a client, which is a REPL:

```bash
make client
```

To start a zookeeper CLI for debug:

```bash
make zk-cli
```