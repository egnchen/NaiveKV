# KV

This is a distributed key-value service implemented in Go.

## Getting started

### Compile protobuf

```bash
cd proto
protoc *.proto --go_out=plugins=grpc:.
```