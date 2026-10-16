package fivefamiliesv1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)

	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type LeaseGrantRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Client        uint64                 `protobuf:"varint,1,opt,name=client,proto3" json:"client,omitempty"`
	Seq           uint64                 `protobuf:"varint,2,opt,name=seq,proto3" json:"seq,omitempty"`
	TtlMs         uint64                 `protobuf:"varint,3,opt,name=ttl_ms,json=ttlMs,proto3" json:"ttl_ms,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LeaseGrantRequest) Reset() {
	*x = LeaseGrantRequest{}
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LeaseGrantRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LeaseGrantRequest) ProtoMessage() {}

func (x *LeaseGrantRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LeaseGrantRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_lease_proto_rawDescGZIP(), []int{0}
}

func (x *LeaseGrantRequest) GetClient() uint64 {
	if x != nil {
		return x.Client
	}
	return 0
}

func (x *LeaseGrantRequest) GetSeq() uint64 {
	if x != nil {
		return x.Seq
	}
	return 0
}

func (x *LeaseGrantRequest) GetTtlMs() uint64 {
	if x != nil {
		return x.TtlMs
	}
	return 0
}

type LeaseGrantResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            uint64                 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	TtlMs         uint64                 `protobuf:"varint,2,opt,name=ttl_ms,json=ttlMs,proto3" json:"ttl_ms,omitempty"`
	Rev           uint64                 `protobuf:"varint,3,opt,name=rev,proto3" json:"rev,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LeaseGrantResponse) Reset() {
	*x = LeaseGrantResponse{}
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LeaseGrantResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LeaseGrantResponse) ProtoMessage() {}

func (x *LeaseGrantResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LeaseGrantResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_lease_proto_rawDescGZIP(), []int{1}
}

func (x *LeaseGrantResponse) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *LeaseGrantResponse) GetTtlMs() uint64 {
	if x != nil {
		return x.TtlMs
	}
	return 0
}

func (x *LeaseGrantResponse) GetRev() uint64 {
	if x != nil {
		return x.Rev
	}
	return 0
}

type LeaseRevokeRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Client        uint64                 `protobuf:"varint,1,opt,name=client,proto3" json:"client,omitempty"`
	Seq           uint64                 `protobuf:"varint,2,opt,name=seq,proto3" json:"seq,omitempty"`
	Id            uint64                 `protobuf:"varint,3,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LeaseRevokeRequest) Reset() {
	*x = LeaseRevokeRequest{}
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LeaseRevokeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LeaseRevokeRequest) ProtoMessage() {}

func (x *LeaseRevokeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LeaseRevokeRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_lease_proto_rawDescGZIP(), []int{2}
}

func (x *LeaseRevokeRequest) GetClient() uint64 {
	if x != nil {
		return x.Client
	}
	return 0
}

func (x *LeaseRevokeRequest) GetSeq() uint64 {
	if x != nil {
		return x.Seq
	}
	return 0
}

func (x *LeaseRevokeRequest) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

type LeaseRevokeResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Rev           uint64                 `protobuf:"varint,1,opt,name=rev,proto3" json:"rev,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LeaseRevokeResponse) Reset() {
	*x = LeaseRevokeResponse{}
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LeaseRevokeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LeaseRevokeResponse) ProtoMessage() {}

func (x *LeaseRevokeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LeaseRevokeResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_lease_proto_rawDescGZIP(), []int{3}
}

func (x *LeaseRevokeResponse) GetRev() uint64 {
	if x != nil {
		return x.Rev
	}
	return 0
}

type LeaseKeepAliveRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            uint64                 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LeaseKeepAliveRequest) Reset() {
	*x = LeaseKeepAliveRequest{}
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LeaseKeepAliveRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LeaseKeepAliveRequest) ProtoMessage() {}

func (x *LeaseKeepAliveRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LeaseKeepAliveRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_lease_proto_rawDescGZIP(), []int{4}
}

func (x *LeaseKeepAliveRequest) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

type LeaseKeepAliveResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            uint64                 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	TtlMs         uint64                 `protobuf:"varint,2,opt,name=ttl_ms,json=ttlMs,proto3" json:"ttl_ms,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LeaseKeepAliveResponse) Reset() {
	*x = LeaseKeepAliveResponse{}
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LeaseKeepAliveResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LeaseKeepAliveResponse) ProtoMessage() {}

func (x *LeaseKeepAliveResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LeaseKeepAliveResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_lease_proto_rawDescGZIP(), []int{5}
}

func (x *LeaseKeepAliveResponse) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *LeaseKeepAliveResponse) GetTtlMs() uint64 {
	if x != nil {
		return x.TtlMs
	}
	return 0
}

type LeaseTimeToLiveRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            uint64                 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LeaseTimeToLiveRequest) Reset() {
	*x = LeaseTimeToLiveRequest{}
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LeaseTimeToLiveRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LeaseTimeToLiveRequest) ProtoMessage() {}

func (x *LeaseTimeToLiveRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LeaseTimeToLiveRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_lease_proto_rawDescGZIP(), []int{6}
}

func (x *LeaseTimeToLiveRequest) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

type LeaseTimeToLiveResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            uint64                 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	TtlMs         uint64                 `protobuf:"varint,2,opt,name=ttl_ms,json=ttlMs,proto3" json:"ttl_ms,omitempty"`
	RemainingMs   uint64                 `protobuf:"varint,3,opt,name=remaining_ms,json=remainingMs,proto3" json:"remaining_ms,omitempty"`
	Keys          uint64                 `protobuf:"varint,4,opt,name=keys,proto3" json:"keys,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LeaseTimeToLiveResponse) Reset() {
	*x = LeaseTimeToLiveResponse{}
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LeaseTimeToLiveResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LeaseTimeToLiveResponse) ProtoMessage() {}

func (x *LeaseTimeToLiveResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_lease_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LeaseTimeToLiveResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_lease_proto_rawDescGZIP(), []int{7}
}

func (x *LeaseTimeToLiveResponse) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *LeaseTimeToLiveResponse) GetTtlMs() uint64 {
	if x != nil {
		return x.TtlMs
	}
	return 0
}

func (x *LeaseTimeToLiveResponse) GetRemainingMs() uint64 {
	if x != nil {
		return x.RemainingMs
	}
	return 0
}

func (x *LeaseTimeToLiveResponse) GetKeys() uint64 {
	if x != nil {
		return x.Keys
	}
	return 0
}

var File_fivefamilies_v1_lease_proto protoreflect.FileDescriptor

const file_fivefamilies_v1_lease_proto_rawDesc = "" +
	"\n" +
	"\x1bfivefamilies/v1/lease.proto\x12\x0ffivefamilies.v1\"T\n" +
	"\x11LeaseGrantRequest\x12\x16\n" +
	"\x06client\x18\x01 \x01(\x04R\x06client\x12\x10\n" +
	"\x03seq\x18\x02 \x01(\x04R\x03seq\x12\x15\n" +
	"\x06ttl_ms\x18\x03 \x01(\x04R\x05ttlMs\"M\n" +
	"\x12LeaseGrantResponse\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x04R\x02id\x12\x15\n" +
	"\x06ttl_ms\x18\x02 \x01(\x04R\x05ttlMs\x12\x10\n" +
	"\x03rev\x18\x03 \x01(\x04R\x03rev\"N\n" +
	"\x12LeaseRevokeRequest\x12\x16\n" +
	"\x06client\x18\x01 \x01(\x04R\x06client\x12\x10\n" +
	"\x03seq\x18\x02 \x01(\x04R\x03seq\x12\x0e\n" +
	"\x02id\x18\x03 \x01(\x04R\x02id\"'\n" +
	"\x13LeaseRevokeResponse\x12\x10\n" +
	"\x03rev\x18\x01 \x01(\x04R\x03rev\"'\n" +
	"\x15LeaseKeepAliveRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x04R\x02id\"?\n" +
	"\x16LeaseKeepAliveResponse\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x04R\x02id\x12\x15\n" +
	"\x06ttl_ms\x18\x02 \x01(\x04R\x05ttlMs\"(\n" +
	"\x16LeaseTimeToLiveRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x04R\x02id\"w\n" +
	"\x17LeaseTimeToLiveResponse\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x04R\x02id\x12\x15\n" +
	"\x06ttl_ms\x18\x02 \x01(\x04R\x05ttlMs\x12!\n" +
	"\fremaining_ms\x18\x03 \x01(\x04R\vremainingMs\x12\x12\n" +
	"\x04keys\x18\x04 \x01(\x04R\x04keys2\xed\x02\n" +
	"\x05Lease\x12P\n" +
	"\x05Grant\x12\".fivefamilies.v1.LeaseGrantRequest\x1a#.fivefamilies.v1.LeaseGrantResponse\x12S\n" +
	"\x06Revoke\x12#.fivefamilies.v1.LeaseRevokeRequest\x1a$.fivefamilies.v1.LeaseRevokeResponse\x12\\\n" +
	"\tKeepAlive\x12&.fivefamilies.v1.LeaseKeepAliveRequest\x1a'.fivefamilies.v1.LeaseKeepAliveResponse\x12_\n" +
	"\n" +
	"TimeToLive\x12'.fivefamilies.v1.LeaseTimeToLiveRequest\x1a(.fivefamilies.v1.LeaseTimeToLiveResponseBHZFgithub.com/mirzanajafov/fivefamilies/api/fivefamiliesv1;fivefamiliesv1b\x06proto3"

var (
	file_fivefamilies_v1_lease_proto_rawDescOnce sync.Once
	file_fivefamilies_v1_lease_proto_rawDescData []byte
)

func file_fivefamilies_v1_lease_proto_rawDescGZIP() []byte {
	file_fivefamilies_v1_lease_proto_rawDescOnce.Do(func() {
		file_fivefamilies_v1_lease_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_lease_proto_rawDesc), len(file_fivefamilies_v1_lease_proto_rawDesc)))
	})
	return file_fivefamilies_v1_lease_proto_rawDescData
}

var file_fivefamilies_v1_lease_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_fivefamilies_v1_lease_proto_goTypes = []any{
	(*LeaseGrantRequest)(nil),
	(*LeaseGrantResponse)(nil),
	(*LeaseRevokeRequest)(nil),
	(*LeaseRevokeResponse)(nil),
	(*LeaseKeepAliveRequest)(nil),
	(*LeaseKeepAliveResponse)(nil),
	(*LeaseTimeToLiveRequest)(nil),
	(*LeaseTimeToLiveResponse)(nil),
}
var file_fivefamilies_v1_lease_proto_depIdxs = []int32{
	0,
	2,
	4,
	6,
	1,
	3,
	5,
	7,
	4,
	0,
	0,
	0,
	0,
}

func init() { file_fivefamilies_v1_lease_proto_init() }
func file_fivefamilies_v1_lease_proto_init() {
	if File_fivefamilies_v1_lease_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_lease_proto_rawDesc), len(file_fivefamilies_v1_lease_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_fivefamilies_v1_lease_proto_goTypes,
		DependencyIndexes: file_fivefamilies_v1_lease_proto_depIdxs,
		MessageInfos:      file_fivefamilies_v1_lease_proto_msgTypes,
	}.Build()
	File_fivefamilies_v1_lease_proto = out.File
	file_fivefamilies_v1_lease_proto_goTypes = nil
	file_fivefamilies_v1_lease_proto_depIdxs = nil
}
