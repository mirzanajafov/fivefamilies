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

type ReadMode int32

const (
	ReadMode_READ_MODE_INDEX ReadMode = 0
	ReadMode_READ_MODE_LEASE ReadMode = 1
	ReadMode_READ_MODE_LOG   ReadMode = 2
)

var (
	ReadMode_name = map[int32]string{
		0: "READ_MODE_INDEX",
		1: "READ_MODE_LEASE",
		2: "READ_MODE_LOG",
	}
	ReadMode_value = map[string]int32{
		"READ_MODE_INDEX": 0,
		"READ_MODE_LEASE": 1,
		"READ_MODE_LOG":   2,
	}
)

func (x ReadMode) Enum() *ReadMode {
	p := new(ReadMode)
	*p = x
	return p
}

func (x ReadMode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ReadMode) Descriptor() protoreflect.EnumDescriptor {
	return file_fivefamilies_v1_kv_proto_enumTypes[0].Descriptor()
}

func (ReadMode) Type() protoreflect.EnumType {
	return &file_fivefamilies_v1_kv_proto_enumTypes[0]
}

func (x ReadMode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (ReadMode) EnumDescriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{0}
}

type KeyValue struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Key           string                 `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value         []byte                 `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	CreateRev     uint64                 `protobuf:"varint,3,opt,name=create_rev,json=createRev,proto3" json:"create_rev,omitempty"`
	ModRev        uint64                 `protobuf:"varint,4,opt,name=mod_rev,json=modRev,proto3" json:"mod_rev,omitempty"`
	Version       uint64                 `protobuf:"varint,5,opt,name=version,proto3" json:"version,omitempty"`
	Lease         uint64                 `protobuf:"varint,6,opt,name=lease,proto3" json:"lease,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *KeyValue) Reset() {
	*x = KeyValue{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *KeyValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeyValue) ProtoMessage() {}

func (x *KeyValue) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*KeyValue) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{0}
}

func (x *KeyValue) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *KeyValue) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *KeyValue) GetCreateRev() uint64 {
	if x != nil {
		return x.CreateRev
	}
	return 0
}

func (x *KeyValue) GetModRev() uint64 {
	if x != nil {
		return x.ModRev
	}
	return 0
}

func (x *KeyValue) GetVersion() uint64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *KeyValue) GetLease() uint64 {
	if x != nil {
		return x.Lease
	}
	return 0
}

type RegisterRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RegisterRequest) Reset() {
	*x = RegisterRequest{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RegisterRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterRequest) ProtoMessage() {}

func (x *RegisterRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*RegisterRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{1}
}

type RegisterResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Client        uint64                 `protobuf:"varint,1,opt,name=client,proto3" json:"client,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RegisterResponse) Reset() {
	*x = RegisterResponse{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RegisterResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterResponse) ProtoMessage() {}

func (x *RegisterResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*RegisterResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{2}
}

func (x *RegisterResponse) GetClient() uint64 {
	if x != nil {
		return x.Client
	}
	return 0
}

type GetRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Key           string                 `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Mode          ReadMode               `protobuf:"varint,2,opt,name=mode,proto3,enum=fivefamilies.v1.ReadMode" json:"mode,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetRequest) Reset() {
	*x = GetRequest{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRequest) ProtoMessage() {}

func (x *GetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GetRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{3}
}

func (x *GetRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *GetRequest) GetMode() ReadMode {
	if x != nil {
		return x.Mode
	}
	return ReadMode_READ_MODE_INDEX
}

type GetResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Found         bool                   `protobuf:"varint,1,opt,name=found,proto3" json:"found,omitempty"`
	Kv            *KeyValue              `protobuf:"bytes,2,opt,name=kv,proto3" json:"kv,omitempty"`
	Rev           uint64                 `protobuf:"varint,3,opt,name=rev,proto3" json:"rev,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetResponse) Reset() {
	*x = GetResponse{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetResponse) ProtoMessage() {}

func (x *GetResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GetResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{4}
}

func (x *GetResponse) GetFound() bool {
	if x != nil {
		return x.Found
	}
	return false
}

func (x *GetResponse) GetKv() *KeyValue {
	if x != nil {
		return x.Kv
	}
	return nil
}

func (x *GetResponse) GetRev() uint64 {
	if x != nil {
		return x.Rev
	}
	return 0
}

type RangeRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Prefix        string                 `protobuf:"bytes,1,opt,name=prefix,proto3" json:"prefix,omitempty"`
	Limit         uint64                 `protobuf:"varint,2,opt,name=limit,proto3" json:"limit,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RangeRequest) Reset() {
	*x = RangeRequest{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RangeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RangeRequest) ProtoMessage() {}

func (x *RangeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*RangeRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{5}
}

func (x *RangeRequest) GetPrefix() string {
	if x != nil {
		return x.Prefix
	}
	return ""
}

func (x *RangeRequest) GetLimit() uint64 {
	if x != nil {
		return x.Limit
	}
	return 0
}

type RangeResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Kvs           []*KeyValue            `protobuf:"bytes,1,rep,name=kvs,proto3" json:"kvs,omitempty"`
	Rev           uint64                 `protobuf:"varint,2,opt,name=rev,proto3" json:"rev,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RangeResponse) Reset() {
	*x = RangeResponse{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RangeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RangeResponse) ProtoMessage() {}

func (x *RangeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*RangeResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{6}
}

func (x *RangeResponse) GetKvs() []*KeyValue {
	if x != nil {
		return x.Kvs
	}
	return nil
}

func (x *RangeResponse) GetRev() uint64 {
	if x != nil {
		return x.Rev
	}
	return 0
}

type PutRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Client        uint64                 `protobuf:"varint,1,opt,name=client,proto3" json:"client,omitempty"`
	Seq           uint64                 `protobuf:"varint,2,opt,name=seq,proto3" json:"seq,omitempty"`
	Key           string                 `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	Value         []byte                 `protobuf:"bytes,4,opt,name=value,proto3" json:"value,omitempty"`
	Lease         uint64                 `protobuf:"varint,5,opt,name=lease,proto3" json:"lease,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PutRequest) Reset() {
	*x = PutRequest{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PutRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutRequest) ProtoMessage() {}

func (x *PutRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*PutRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{7}
}

func (x *PutRequest) GetClient() uint64 {
	if x != nil {
		return x.Client
	}
	return 0
}

func (x *PutRequest) GetSeq() uint64 {
	if x != nil {
		return x.Seq
	}
	return 0
}

func (x *PutRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *PutRequest) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *PutRequest) GetLease() uint64 {
	if x != nil {
		return x.Lease
	}
	return 0
}

type PutResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Rev           uint64                 `protobuf:"varint,1,opt,name=rev,proto3" json:"rev,omitempty"`
	HadPrev       bool                   `protobuf:"varint,2,opt,name=had_prev,json=hadPrev,proto3" json:"had_prev,omitempty"`
	Prev          *KeyValue              `protobuf:"bytes,3,opt,name=prev,proto3" json:"prev,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PutResponse) Reset() {
	*x = PutResponse{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PutResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutResponse) ProtoMessage() {}

func (x *PutResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*PutResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{8}
}

func (x *PutResponse) GetRev() uint64 {
	if x != nil {
		return x.Rev
	}
	return 0
}

func (x *PutResponse) GetHadPrev() bool {
	if x != nil {
		return x.HadPrev
	}
	return false
}

func (x *PutResponse) GetPrev() *KeyValue {
	if x != nil {
		return x.Prev
	}
	return nil
}

type DeleteRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Client        uint64                 `protobuf:"varint,1,opt,name=client,proto3" json:"client,omitempty"`
	Seq           uint64                 `protobuf:"varint,2,opt,name=seq,proto3" json:"seq,omitempty"`
	Key           string                 `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeleteRequest) Reset() {
	*x = DeleteRequest{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteRequest) ProtoMessage() {}

func (x *DeleteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*DeleteRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{9}
}

func (x *DeleteRequest) GetClient() uint64 {
	if x != nil {
		return x.Client
	}
	return 0
}

func (x *DeleteRequest) GetSeq() uint64 {
	if x != nil {
		return x.Seq
	}
	return 0
}

func (x *DeleteRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

type DeleteResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Rev           uint64                 `protobuf:"varint,1,opt,name=rev,proto3" json:"rev,omitempty"`
	Deleted       bool                   `protobuf:"varint,2,opt,name=deleted,proto3" json:"deleted,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeleteResponse) Reset() {
	*x = DeleteResponse{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteResponse) ProtoMessage() {}

func (x *DeleteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*DeleteResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{10}
}

func (x *DeleteResponse) GetRev() uint64 {
	if x != nil {
		return x.Rev
	}
	return 0
}

func (x *DeleteResponse) GetDeleted() bool {
	if x != nil {
		return x.Deleted
	}
	return false
}

type CompareAndSwapRequest struct {
	state  protoimpl.MessageState `protogen:"open.v1"`
	Client uint64                 `protobuf:"varint,1,opt,name=client,proto3" json:"client,omitempty"`
	Seq    uint64                 `protobuf:"varint,2,opt,name=seq,proto3" json:"seq,omitempty"`
	Key    string                 `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	Value  []byte                 `protobuf:"bytes,4,opt,name=value,proto3" json:"value,omitempty"`

	Expect        isCompareAndSwapRequest_Expect `protobuf_oneof:"expect"`
	Lease         uint64                         `protobuf:"varint,8,opt,name=lease,proto3" json:"lease,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CompareAndSwapRequest) Reset() {
	*x = CompareAndSwapRequest{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CompareAndSwapRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CompareAndSwapRequest) ProtoMessage() {}

func (x *CompareAndSwapRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*CompareAndSwapRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{11}
}

func (x *CompareAndSwapRequest) GetClient() uint64 {
	if x != nil {
		return x.Client
	}
	return 0
}

func (x *CompareAndSwapRequest) GetSeq() uint64 {
	if x != nil {
		return x.Seq
	}
	return 0
}

func (x *CompareAndSwapRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *CompareAndSwapRequest) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *CompareAndSwapRequest) GetExpect() isCompareAndSwapRequest_Expect {
	if x != nil {
		return x.Expect
	}
	return nil
}

func (x *CompareAndSwapRequest) GetValueIs() []byte {
	if x != nil {
		if x, ok := x.Expect.(*CompareAndSwapRequest_ValueIs); ok {
			return x.ValueIs
		}
	}
	return nil
}

func (x *CompareAndSwapRequest) GetMissing() bool {
	if x != nil {
		if x, ok := x.Expect.(*CompareAndSwapRequest_Missing); ok {
			return x.Missing
		}
	}
	return false
}

func (x *CompareAndSwapRequest) GetModRev() uint64 {
	if x != nil {
		if x, ok := x.Expect.(*CompareAndSwapRequest_ModRev); ok {
			return x.ModRev
		}
	}
	return 0
}

func (x *CompareAndSwapRequest) GetLease() uint64 {
	if x != nil {
		return x.Lease
	}
	return 0
}

type isCompareAndSwapRequest_Expect interface {
	isCompareAndSwapRequest_Expect()
}

type CompareAndSwapRequest_ValueIs struct {
	ValueIs []byte `protobuf:"bytes,5,opt,name=value_is,json=valueIs,proto3,oneof"`
}

type CompareAndSwapRequest_Missing struct {
	Missing bool `protobuf:"varint,6,opt,name=missing,proto3,oneof"`
}

type CompareAndSwapRequest_ModRev struct {
	ModRev uint64 `protobuf:"varint,7,opt,name=mod_rev,json=modRev,proto3,oneof"`
}

func (*CompareAndSwapRequest_ValueIs) isCompareAndSwapRequest_Expect() {}

func (*CompareAndSwapRequest_Missing) isCompareAndSwapRequest_Expect() {}

func (*CompareAndSwapRequest_ModRev) isCompareAndSwapRequest_Expect() {}

type CompareAndSwapResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Rev           uint64                 `protobuf:"varint,1,opt,name=rev,proto3" json:"rev,omitempty"`
	Swapped       bool                   `protobuf:"varint,2,opt,name=swapped,proto3" json:"swapped,omitempty"`
	Found         bool                   `protobuf:"varint,3,opt,name=found,proto3" json:"found,omitempty"`
	Current       *KeyValue              `protobuf:"bytes,4,opt,name=current,proto3" json:"current,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CompareAndSwapResponse) Reset() {
	*x = CompareAndSwapResponse{}
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CompareAndSwapResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CompareAndSwapResponse) ProtoMessage() {}

func (x *CompareAndSwapResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_kv_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*CompareAndSwapResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_kv_proto_rawDescGZIP(), []int{12}
}

func (x *CompareAndSwapResponse) GetRev() uint64 {
	if x != nil {
		return x.Rev
	}
	return 0
}

func (x *CompareAndSwapResponse) GetSwapped() bool {
	if x != nil {
		return x.Swapped
	}
	return false
}

func (x *CompareAndSwapResponse) GetFound() bool {
	if x != nil {
		return x.Found
	}
	return false
}

func (x *CompareAndSwapResponse) GetCurrent() *KeyValue {
	if x != nil {
		return x.Current
	}
	return nil
}

var File_fivefamilies_v1_kv_proto protoreflect.FileDescriptor

const file_fivefamilies_v1_kv_proto_rawDesc = "" +
	"\n" +
	"\x18fivefamilies/v1/kv.proto\x12\x0ffivefamilies.v1\"\x9a\x01\n" +
	"\bKeyValue\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\fR\x05value\x12\x1d\n" +
	"\n" +
	"create_rev\x18\x03 \x01(\x04R\tcreateRev\x12\x17\n" +
	"\amod_rev\x18\x04 \x01(\x04R\x06modRev\x12\x18\n" +
	"\aversion\x18\x05 \x01(\x04R\aversion\x12\x14\n" +
	"\x05lease\x18\x06 \x01(\x04R\x05lease\"\x11\n" +
	"\x0fRegisterRequest\"*\n" +
	"\x10RegisterResponse\x12\x16\n" +
	"\x06client\x18\x01 \x01(\x04R\x06client\"M\n" +
	"\n" +
	"GetRequest\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12-\n" +
	"\x04mode\x18\x02 \x01(\x0e2\x19.fivefamilies.v1.ReadModeR\x04mode\"`\n" +
	"\vGetResponse\x12\x14\n" +
	"\x05found\x18\x01 \x01(\bR\x05found\x12)\n" +
	"\x02kv\x18\x02 \x01(\v2\x19.fivefamilies.v1.KeyValueR\x02kv\x12\x10\n" +
	"\x03rev\x18\x03 \x01(\x04R\x03rev\"<\n" +
	"\fRangeRequest\x12\x16\n" +
	"\x06prefix\x18\x01 \x01(\tR\x06prefix\x12\x14\n" +
	"\x05limit\x18\x02 \x01(\x04R\x05limit\"N\n" +
	"\rRangeResponse\x12+\n" +
	"\x03kvs\x18\x01 \x03(\v2\x19.fivefamilies.v1.KeyValueR\x03kvs\x12\x10\n" +
	"\x03rev\x18\x02 \x01(\x04R\x03rev\"t\n" +
	"\n" +
	"PutRequest\x12\x16\n" +
	"\x06client\x18\x01 \x01(\x04R\x06client\x12\x10\n" +
	"\x03seq\x18\x02 \x01(\x04R\x03seq\x12\x10\n" +
	"\x03key\x18\x03 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x04 \x01(\fR\x05value\x12\x14\n" +
	"\x05lease\x18\x05 \x01(\x04R\x05lease\"i\n" +
	"\vPutResponse\x12\x10\n" +
	"\x03rev\x18\x01 \x01(\x04R\x03rev\x12\x19\n" +
	"\bhad_prev\x18\x02 \x01(\bR\ahadPrev\x12-\n" +
	"\x04prev\x18\x03 \x01(\v2\x19.fivefamilies.v1.KeyValueR\x04prev\"K\n" +
	"\rDeleteRequest\x12\x16\n" +
	"\x06client\x18\x01 \x01(\x04R\x06client\x12\x10\n" +
	"\x03seq\x18\x02 \x01(\x04R\x03seq\x12\x10\n" +
	"\x03key\x18\x03 \x01(\tR\x03key\"<\n" +
	"\x0eDeleteResponse\x12\x10\n" +
	"\x03rev\x18\x01 \x01(\x04R\x03rev\x12\x18\n" +
	"\adeleted\x18\x02 \x01(\bR\adeleted\"\xdd\x01\n" +
	"\x15CompareAndSwapRequest\x12\x16\n" +
	"\x06client\x18\x01 \x01(\x04R\x06client\x12\x10\n" +
	"\x03seq\x18\x02 \x01(\x04R\x03seq\x12\x10\n" +
	"\x03key\x18\x03 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x04 \x01(\fR\x05value\x12\x1b\n" +
	"\bvalue_is\x18\x05 \x01(\fH\x00R\avalueIs\x12\x1a\n" +
	"\amissing\x18\x06 \x01(\bH\x00R\amissing\x12\x19\n" +
	"\amod_rev\x18\a \x01(\x04H\x00R\x06modRev\x12\x14\n" +
	"\x05lease\x18\b \x01(\x04R\x05leaseB\b\n" +
	"\x06expect\"\x8f\x01\n" +
	"\x16CompareAndSwapResponse\x12\x10\n" +
	"\x03rev\x18\x01 \x01(\x04R\x03rev\x12\x18\n" +
	"\aswapped\x18\x02 \x01(\bR\aswapped\x12\x14\n" +
	"\x05found\x18\x03 \x01(\bR\x05found\x123\n" +
	"\acurrent\x18\x04 \x01(\v2\x19.fivefamilies.v1.KeyValueR\acurrent*G\n" +
	"\bReadMode\x12\x13\n" +
	"\x0fREAD_MODE_INDEX\x10\x00\x12\x13\n" +
	"\x0fREAD_MODE_LEASE\x10\x01\x12\x11\n" +
	"\rREAD_MODE_LOG\x10\x022\xcf\x03\n" +
	"\x02KV\x12O\n" +
	"\bRegister\x12 .fivefamilies.v1.RegisterRequest\x1a!.fivefamilies.v1.RegisterResponse\x12@\n" +
	"\x03Get\x12\x1b.fivefamilies.v1.GetRequest\x1a\x1c.fivefamilies.v1.GetResponse\x12F\n" +
	"\x05Range\x12\x1d.fivefamilies.v1.RangeRequest\x1a\x1e.fivefamilies.v1.RangeResponse\x12@\n" +
	"\x03Put\x12\x1b.fivefamilies.v1.PutRequest\x1a\x1c.fivefamilies.v1.PutResponse\x12I\n" +
	"\x06Delete\x12\x1e.fivefamilies.v1.DeleteRequest\x1a\x1f.fivefamilies.v1.DeleteResponse\x12a\n" +
	"\x0eCompareAndSwap\x12&.fivefamilies.v1.CompareAndSwapRequest\x1a'.fivefamilies.v1.CompareAndSwapResponseBHZFgithub.com/mirzanajafov/fivefamilies/api/fivefamiliesv1;fivefamiliesv1b\x06proto3"

var (
	file_fivefamilies_v1_kv_proto_rawDescOnce sync.Once
	file_fivefamilies_v1_kv_proto_rawDescData []byte
)

func file_fivefamilies_v1_kv_proto_rawDescGZIP() []byte {
	file_fivefamilies_v1_kv_proto_rawDescOnce.Do(func() {
		file_fivefamilies_v1_kv_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_kv_proto_rawDesc), len(file_fivefamilies_v1_kv_proto_rawDesc)))
	})
	return file_fivefamilies_v1_kv_proto_rawDescData
}

var file_fivefamilies_v1_kv_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_fivefamilies_v1_kv_proto_msgTypes = make([]protoimpl.MessageInfo, 13)
var file_fivefamilies_v1_kv_proto_goTypes = []any{
	(ReadMode)(0),
	(*KeyValue)(nil),
	(*RegisterRequest)(nil),
	(*RegisterResponse)(nil),
	(*GetRequest)(nil),
	(*GetResponse)(nil),
	(*RangeRequest)(nil),
	(*RangeResponse)(nil),
	(*PutRequest)(nil),
	(*PutResponse)(nil),
	(*DeleteRequest)(nil),
	(*DeleteResponse)(nil),
	(*CompareAndSwapRequest)(nil),
	(*CompareAndSwapResponse)(nil),
}
var file_fivefamilies_v1_kv_proto_depIdxs = []int32{
	0,
	1,
	1,
	1,
	1,
	2,
	4,
	6,
	8,
	10,
	12,
	3,
	5,
	7,
	9,
	11,
	13,
	11,
	5,
	5,
	5,
	0,
}

func init() { file_fivefamilies_v1_kv_proto_init() }
func file_fivefamilies_v1_kv_proto_init() {
	if File_fivefamilies_v1_kv_proto != nil {
		return
	}
	file_fivefamilies_v1_kv_proto_msgTypes[11].OneofWrappers = []any{
		(*CompareAndSwapRequest_ValueIs)(nil),
		(*CompareAndSwapRequest_Missing)(nil),
		(*CompareAndSwapRequest_ModRev)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_kv_proto_rawDesc), len(file_fivefamilies_v1_kv_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   13,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_fivefamilies_v1_kv_proto_goTypes,
		DependencyIndexes: file_fivefamilies_v1_kv_proto_depIdxs,
		EnumInfos:         file_fivefamilies_v1_kv_proto_enumTypes,
		MessageInfos:      file_fivefamilies_v1_kv_proto_msgTypes,
	}.Build()
	File_fivefamilies_v1_kv_proto = out.File
	file_fivefamilies_v1_kv_proto_goTypes = nil
	file_fivefamilies_v1_kv_proto_depIdxs = nil
}
