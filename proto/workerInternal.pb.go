// gRPC service for KV data worker internal logic
// These interfaces should not be exposed to the client
// @author Eugene Chen cyj205@sjtu.edu.cn

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.23.0
// 	protoc        v3.6.1
// source: workerInternal.proto

package proto

import (
	context "context"
	proto "github.com/golang/protobuf/proto"
	empty "github.com/golang/protobuf/ptypes/empty"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type MigrationResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Status Status `protobuf:"varint,1,opt,name=status,proto3,enum=kv.proto.Status" json:"status,omitempty"`
}

func (x *MigrationResponse) Reset() {
	*x = MigrationResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_workerInternal_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MigrationResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MigrationResponse) ProtoMessage() {}

func (x *MigrationResponse) ProtoReflect() protoreflect.Message {
	mi := &file_workerInternal_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MigrationResponse.ProtoReflect.Descriptor instead.
func (*MigrationResponse) Descriptor() ([]byte, []int) {
	return file_workerInternal_proto_rawDescGZIP(), []int{0}
}

func (x *MigrationResponse) GetStatus() Status {
	if x != nil {
		return x.Status
	}
	return Status_OK
}

type FlushResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Status Status `protobuf:"varint,1,opt,name=status,proto3,enum=kv.proto.Status" json:"status,omitempty"`
}

func (x *FlushResponse) Reset() {
	*x = FlushResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_workerInternal_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FlushResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FlushResponse) ProtoMessage() {}

func (x *FlushResponse) ProtoReflect() protoreflect.Message {
	mi := &file_workerInternal_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FlushResponse.ProtoReflect.Descriptor instead.
func (*FlushResponse) Descriptor() ([]byte, []int) {
	return file_workerInternal_proto_rawDescGZIP(), []int{1}
}

func (x *FlushResponse) GetStatus() Status {
	if x != nil {
		return x.Status
	}
	return Status_OK
}

var File_workerInternal_proto protoreflect.FileDescriptor

var file_workerInternal_proto_rawDesc = []byte{
	0x0a, 0x14, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08, 0x6b, 0x76, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x0c, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1b,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f,
	0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x3d, 0x0a, 0x11, 0x4d,
	0x69, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x28, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e,
	0x32, 0x10, 0x2e, 0x6b, 0x76, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x22, 0x39, 0x0a, 0x0d, 0x46, 0x6c,
	0x75, 0x73, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x28, 0x0a, 0x06, 0x73,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x10, 0x2e, 0x6b, 0x76,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x32, 0x53, 0x0a, 0x10, 0x4b, 0x56, 0x57, 0x6f, 0x72, 0x6b, 0x65,
	0x72, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x12, 0x3f, 0x0a, 0x0a, 0x63, 0x68, 0x65,
	0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a,
	0x17, 0x2e, 0x6b, 0x76, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x46, 0x6c, 0x75, 0x73, 0x68,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x09, 0x5a, 0x07, 0x2e, 0x3b,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_workerInternal_proto_rawDescOnce sync.Once
	file_workerInternal_proto_rawDescData = file_workerInternal_proto_rawDesc
)

func file_workerInternal_proto_rawDescGZIP() []byte {
	file_workerInternal_proto_rawDescOnce.Do(func() {
		file_workerInternal_proto_rawDescData = protoimpl.X.CompressGZIP(file_workerInternal_proto_rawDescData)
	})
	return file_workerInternal_proto_rawDescData
}

var file_workerInternal_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_workerInternal_proto_goTypes = []interface{}{
	(*MigrationResponse)(nil), // 0: kv.proto.MigrationResponse
	(*FlushResponse)(nil),     // 1: kv.proto.FlushResponse
	(Status)(0),               // 2: kv.proto.Status
	(*empty.Empty)(nil),       // 3: google.protobuf.Empty
}
var file_workerInternal_proto_depIdxs = []int32{
	2, // 0: kv.proto.MigrationResponse.status:type_name -> kv.proto.Status
	2, // 1: kv.proto.FlushResponse.status:type_name -> kv.proto.Status
	3, // 2: kv.proto.KVWorkerInternal.checkpoint:input_type -> google.protobuf.Empty
	1, // 3: kv.proto.KVWorkerInternal.checkpoint:output_type -> kv.proto.FlushResponse
	3, // [3:4] is the sub-list for method output_type
	2, // [2:3] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_workerInternal_proto_init() }
func file_workerInternal_proto_init() {
	if File_workerInternal_proto != nil {
		return
	}
	file_common_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_workerInternal_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MigrationResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_workerInternal_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FlushResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_workerInternal_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_workerInternal_proto_goTypes,
		DependencyIndexes: file_workerInternal_proto_depIdxs,
		MessageInfos:      file_workerInternal_proto_msgTypes,
	}.Build()
	File_workerInternal_proto = out.File
	file_workerInternal_proto_rawDesc = nil
	file_workerInternal_proto_goTypes = nil
	file_workerInternal_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// KVWorkerInternalClient is the client API for KVWorkerInternal service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type KVWorkerInternalClient interface {
	Checkpoint(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*FlushResponse, error)
}

type kVWorkerInternalClient struct {
	cc grpc.ClientConnInterface
}

func NewKVWorkerInternalClient(cc grpc.ClientConnInterface) KVWorkerInternalClient {
	return &kVWorkerInternalClient{cc}
}

func (c *kVWorkerInternalClient) Checkpoint(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*FlushResponse, error) {
	out := new(FlushResponse)
	err := c.cc.Invoke(ctx, "/kv.proto.KVWorkerInternal/checkpoint", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KVWorkerInternalServer is the server API for KVWorkerInternal service.
type KVWorkerInternalServer interface {
	Checkpoint(context.Context, *empty.Empty) (*FlushResponse, error)
}

// UnimplementedKVWorkerInternalServer can be embedded to have forward compatible implementations.
type UnimplementedKVWorkerInternalServer struct {
}

func (*UnimplementedKVWorkerInternalServer) Checkpoint(context.Context, *empty.Empty) (*FlushResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Checkpoint not implemented")
}

func RegisterKVWorkerInternalServer(s *grpc.Server, srv KVWorkerInternalServer) {
	s.RegisterService(&_KVWorkerInternal_serviceDesc, srv)
}

func _KVWorkerInternal_Checkpoint_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVWorkerInternalServer).Checkpoint(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/kv.proto.KVWorkerInternal/Checkpoint",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVWorkerInternalServer).Checkpoint(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

var _KVWorkerInternal_serviceDesc = grpc.ServiceDesc{
	ServiceName: "kv.proto.KVWorkerInternal",
	HandlerType: (*KVWorkerInternalServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "checkpoint",
			Handler:    _KVWorkerInternal_Checkpoint_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "workerInternal.proto",
}
