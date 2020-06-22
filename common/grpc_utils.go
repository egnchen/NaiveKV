package common

import (
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func NewGrpcServer() *grpc.Server {
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	return grpcServer
}

// Connect to a gRPC client.
// `connString` is the connection string with format `hostname:port`.
func ConnectGrpc(connString string) (*grpc.ClientConn, error) {
	log := Log()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())

	log.Info("Dialing...", zap.String("server", connString))
	conn, err := grpc.Dial(connString, opts...)
	if err != nil {
		log.Warn("Failed to dail.",
			zap.String("server", connString), zap.Error(err))
		return nil, err
	}
	return conn, nil
}
