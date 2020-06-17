package common

import (
	"google.golang.org/grpc"
)

func NewGrpcServer() *grpc.Server {
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	return grpcServer
}
