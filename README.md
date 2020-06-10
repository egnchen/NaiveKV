# KV

This is a distributed key-value service implemented in Go.

## Getting started

### Compile protobuf

```bash
protoc proto/master.proto --go_out=plugins=grpc:.
```