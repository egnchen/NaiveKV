// Code generated by protoc-gen-go. DO NOT EDIT.
// source: backup.proto

package proto

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type BackupClientAuth struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *BackupClientAuth) Reset()         { *m = BackupClientAuth{} }
func (m *BackupClientAuth) String() string { return proto.CompactTextString(m) }
func (*BackupClientAuth) ProtoMessage()    {}
func (*BackupClientAuth) Descriptor() ([]byte, []int) {
	return fileDescriptor_65240d19de191688, []int{0}
}

func (m *BackupClientAuth) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BackupClientAuth.Unmarshal(m, b)
}
func (m *BackupClientAuth) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BackupClientAuth.Marshal(b, m, deterministic)
}
func (m *BackupClientAuth) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BackupClientAuth.Merge(m, src)
}
func (m *BackupClientAuth) XXX_Size() int {
	return xxx_messageInfo_BackupClientAuth.Size(m)
}
func (m *BackupClientAuth) XXX_DiscardUnknown() {
	xxx_messageInfo_BackupClientAuth.DiscardUnknown(m)
}

var xxx_messageInfo_BackupClientAuth proto.InternalMessageInfo

type BackupClientToken struct {
	Id                   int32    `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *BackupClientToken) Reset()         { *m = BackupClientToken{} }
func (m *BackupClientToken) String() string { return proto.CompactTextString(m) }
func (*BackupClientToken) ProtoMessage()    {}
func (*BackupClientToken) Descriptor() ([]byte, []int) {
	return fileDescriptor_65240d19de191688, []int{1}
}

func (m *BackupClientToken) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BackupClientToken.Unmarshal(m, b)
}
func (m *BackupClientToken) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BackupClientToken.Marshal(b, m, deterministic)
}
func (m *BackupClientToken) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BackupClientToken.Merge(m, src)
}
func (m *BackupClientToken) XXX_Size() int {
	return xxx_messageInfo_BackupClientToken.Size(m)
}
func (m *BackupClientToken) XXX_DiscardUnknown() {
	xxx_messageInfo_BackupClientToken.DiscardUnknown(m)
}

var xxx_messageInfo_BackupClientToken proto.InternalMessageInfo

func (m *BackupClientToken) GetId() int32 {
	if m != nil {
		return m.Id
	}
	return 0
}

type BackupEntry struct {
	Op                   Operation `protobuf:"varint,1,opt,name=op,proto3,enum=kv.proto.Operation" json:"op,omitempty"`
	Key                  string    `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Value                string    `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *BackupEntry) Reset()         { *m = BackupEntry{} }
func (m *BackupEntry) String() string { return proto.CompactTextString(m) }
func (*BackupEntry) ProtoMessage()    {}
func (*BackupEntry) Descriptor() ([]byte, []int) {
	return fileDescriptor_65240d19de191688, []int{2}
}

func (m *BackupEntry) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BackupEntry.Unmarshal(m, b)
}
func (m *BackupEntry) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BackupEntry.Marshal(b, m, deterministic)
}
func (m *BackupEntry) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BackupEntry.Merge(m, src)
}
func (m *BackupEntry) XXX_Size() int {
	return xxx_messageInfo_BackupEntry.Size(m)
}
func (m *BackupEntry) XXX_DiscardUnknown() {
	xxx_messageInfo_BackupEntry.DiscardUnknown(m)
}

var xxx_messageInfo_BackupEntry proto.InternalMessageInfo

func (m *BackupEntry) GetOp() Operation {
	if m != nil {
		return m.Op
	}
	return Operation_GET
}

func (m *BackupEntry) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *BackupEntry) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type BackupReply struct {
	Status               Status   `protobuf:"varint,1,opt,name=status,proto3,enum=kv.proto.Status" json:"status,omitempty"`
	Version              int32    `protobuf:"varint,2,opt,name=version,proto3" json:"version,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *BackupReply) Reset()         { *m = BackupReply{} }
func (m *BackupReply) String() string { return proto.CompactTextString(m) }
func (*BackupReply) ProtoMessage()    {}
func (*BackupReply) Descriptor() ([]byte, []int) {
	return fileDescriptor_65240d19de191688, []int{3}
}

func (m *BackupReply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BackupReply.Unmarshal(m, b)
}
func (m *BackupReply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BackupReply.Marshal(b, m, deterministic)
}
func (m *BackupReply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BackupReply.Merge(m, src)
}
func (m *BackupReply) XXX_Size() int {
	return xxx_messageInfo_BackupReply.Size(m)
}
func (m *BackupReply) XXX_DiscardUnknown() {
	xxx_messageInfo_BackupReply.DiscardUnknown(m)
}

var xxx_messageInfo_BackupReply proto.InternalMessageInfo

func (m *BackupReply) GetStatus() Status {
	if m != nil {
		return m.Status
	}
	return Status_OK
}

func (m *BackupReply) GetVersion() int32 {
	if m != nil {
		return m.Version
	}
	return 0
}

func init() {
	proto.RegisterType((*BackupClientAuth)(nil), "kv.proto.BackupClientAuth")
	proto.RegisterType((*BackupClientToken)(nil), "kv.proto.BackupClientToken")
	proto.RegisterType((*BackupEntry)(nil), "kv.proto.BackupEntry")
	proto.RegisterType((*BackupReply)(nil), "kv.proto.BackupReply")
}

func init() {
	proto.RegisterFile("backup.proto", fileDescriptor_65240d19de191688)
}

var fileDescriptor_65240d19de191688 = []byte{
	// 275 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x90, 0xc1, 0x4b, 0xc3, 0x30,
	0x14, 0xc6, 0x97, 0x8e, 0x76, 0xf5, 0x39, 0x46, 0x7d, 0x2a, 0x94, 0x7a, 0x19, 0xd9, 0xa5, 0xa7,
	0x22, 0xf3, 0xea, 0xc5, 0xc9, 0x4e, 0x1e, 0xc4, 0x28, 0x1e, 0x76, 0xeb, 0xb6, 0xa0, 0xa1, 0x5d,
	0x12, 0xda, 0xb4, 0xd0, 0xbf, 0xc2, 0x7f, 0x59, 0x4c, 0x5a, 0x26, 0xd3, 0x53, 0xde, 0xfb, 0xbe,
	0x8f, 0xdf, 0x0b, 0x1f, 0x4c, 0xb7, 0xf9, 0xae, 0x68, 0x74, 0xa6, 0x2b, 0x65, 0x14, 0x86, 0x45,
	0xeb, 0xa6, 0x64, 0xba, 0x53, 0x87, 0x83, 0x92, 0x6e, 0xa3, 0x08, 0xd1, 0xca, 0xe6, 0x1e, 0x4b,
	0xc1, 0xa5, 0x79, 0x68, 0xcc, 0x27, 0x5d, 0xc0, 0xc5, 0x6f, 0xed, 0x4d, 0x15, 0x5c, 0xe2, 0x0c,
	0x3c, 0xb1, 0x8f, 0xc9, 0x9c, 0xa4, 0x3e, 0xf3, 0xc4, 0x9e, 0x6e, 0xe0, 0xdc, 0x85, 0xd6, 0xd2,
	0x54, 0x1d, 0x2e, 0xc0, 0x53, 0xda, 0xda, 0xb3, 0xe5, 0x65, 0x36, 0x1c, 0xcb, 0x9e, 0x35, 0xaf,
	0x72, 0x23, 0x94, 0x64, 0x9e, 0xd2, 0x18, 0xc1, 0xb8, 0xe0, 0x5d, 0xec, 0xcd, 0x49, 0x7a, 0xc6,
	0x7e, 0x46, 0xbc, 0x02, 0xbf, 0xcd, 0xcb, 0x86, 0xc7, 0x63, 0xab, 0xb9, 0x85, 0xbe, 0x0c, 0x6c,
	0xc6, 0x75, 0xd9, 0x61, 0x0a, 0x41, 0x6d, 0x72, 0xd3, 0xd4, 0x3d, 0x3f, 0x3a, 0xf2, 0x5f, 0xad,
	0xce, 0x7a, 0x1f, 0x63, 0x98, 0xb4, 0xbc, 0xaa, 0x85, 0x92, 0xf6, 0x88, 0xcf, 0x86, 0x75, 0xf9,
	0x45, 0x20, 0x7c, 0x7a, 0x77, 0x54, 0x5c, 0x43, 0xc8, 0xf8, 0x87, 0xa8, 0x0d, 0xaf, 0x30, 0x39,
	0xc2, 0x4e, 0x8b, 0x48, 0x6e, 0xfe, 0xf7, 0x6c, 0x21, 0x74, 0x84, 0xf7, 0x10, 0xf4, 0xc0, 0xeb,
	0xd3, 0xa0, 0xfd, 0x78, 0xf2, 0x47, 0xb6, 0x5d, 0xd1, 0x51, 0x4a, 0x6e, 0xc9, 0x6a, 0xb2, 0xf1,
	0xad, 0xb5, 0x0d, 0xec, 0x73, 0xf7, 0x1d, 0x00, 0x00, 0xff, 0xff, 0xf9, 0x86, 0x1a, 0xc3, 0xb1,
	0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// KVBackupClient is the client API for KVBackup service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type KVBackupClient interface {
	Register(ctx context.Context, in *BackupClientAuth, opts ...grpc.CallOption) (*BackupClientToken, error)
	Backup(ctx context.Context, opts ...grpc.CallOption) (KVBackup_BackupClient, error)
}

type kVBackupClient struct {
	cc grpc.ClientConnInterface
}

func NewKVBackupClient(cc grpc.ClientConnInterface) KVBackupClient {
	return &kVBackupClient{cc}
}

func (c *kVBackupClient) Register(ctx context.Context, in *BackupClientAuth, opts ...grpc.CallOption) (*BackupClientToken, error) {
	out := new(BackupClientToken)
	err := c.cc.Invoke(ctx, "/kv.proto.KVBackup/Register", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVBackupClient) Backup(ctx context.Context, opts ...grpc.CallOption) (KVBackup_BackupClient, error) {
	stream, err := c.cc.NewStream(ctx, &_KVBackup_serviceDesc.Streams[0], "/kv.proto.KVBackup/Backup", opts...)
	if err != nil {
		return nil, err
	}
	x := &kVBackupBackupClient{stream}
	return x, nil
}

type KVBackup_BackupClient interface {
	Send(*BackupReply) error
	Recv() (*BackupEntry, error)
	grpc.ClientStream
}

type kVBackupBackupClient struct {
	grpc.ClientStream
}

func (x *kVBackupBackupClient) Send(m *BackupReply) error {
	return x.ClientStream.SendMsg(m)
}

func (x *kVBackupBackupClient) Recv() (*BackupEntry, error) {
	m := new(BackupEntry)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// KVBackupServer is the server API for KVBackup service.
type KVBackupServer interface {
	Register(context.Context, *BackupClientAuth) (*BackupClientToken, error)
	Backup(KVBackup_BackupServer) error
}

// UnimplementedKVBackupServer can be embedded to have forward compatible implementations.
type UnimplementedKVBackupServer struct {
}

func (*UnimplementedKVBackupServer) Register(ctx context.Context, req *BackupClientAuth) (*BackupClientToken, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Register not implemented")
}
func (*UnimplementedKVBackupServer) Backup(srv KVBackup_BackupServer) error {
	return status.Errorf(codes.Unimplemented, "method Backup not implemented")
}

func RegisterKVBackupServer(s *grpc.Server, srv KVBackupServer) {
	s.RegisterService(&_KVBackup_serviceDesc, srv)
}

func _KVBackup_Register_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BackupClientAuth)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVBackupServer).Register(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/kv.proto.KVBackup/Register",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVBackupServer).Register(ctx, req.(*BackupClientAuth))
	}
	return interceptor(ctx, in, info, handler)
}

func _KVBackup_Backup_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(KVBackupServer).Backup(&kVBackupBackupServer{stream})
}

type KVBackup_BackupServer interface {
	Send(*BackupEntry) error
	Recv() (*BackupReply, error)
	grpc.ServerStream
}

type kVBackupBackupServer struct {
	grpc.ServerStream
}

func (x *kVBackupBackupServer) Send(m *BackupEntry) error {
	return x.ServerStream.SendMsg(m)
}

func (x *kVBackupBackupServer) Recv() (*BackupReply, error) {
	m := new(BackupReply)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _KVBackup_serviceDesc = grpc.ServiceDesc{
	ServiceName: "kv.proto.KVBackup",
	HandlerType: (*KVBackupServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Register",
			Handler:    _KVBackup_Register_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Backup",
			Handler:       _KVBackup_Backup_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "backup.proto",
}
