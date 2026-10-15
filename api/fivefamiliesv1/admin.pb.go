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

type StatusRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StatusRequest) Reset() {
	*x = StatusRequest{}
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StatusRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StatusRequest) ProtoMessage() {}

func (x *StatusRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*StatusRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{0}
}

type StatusResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            uint64                 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	State         string                 `protobuf:"bytes,2,opt,name=state,proto3" json:"state,omitempty"`
	Term          uint64                 `protobuf:"varint,3,opt,name=term,proto3" json:"term,omitempty"`
	Leader        uint64                 `protobuf:"varint,4,opt,name=leader,proto3" json:"leader,omitempty"`
	Commit        uint64                 `protobuf:"varint,5,opt,name=commit,proto3" json:"commit,omitempty"`
	Applied       uint64                 `protobuf:"varint,6,opt,name=applied,proto3" json:"applied,omitempty"`
	FirstIndex    uint64                 `protobuf:"varint,7,opt,name=first_index,json=firstIndex,proto3" json:"first_index,omitempty"`
	LastIndex     uint64                 `protobuf:"varint,8,opt,name=last_index,json=lastIndex,proto3" json:"last_index,omitempty"`
	SnapshotIndex uint64                 `protobuf:"varint,9,opt,name=snapshot_index,json=snapshotIndex,proto3" json:"snapshot_index,omitempty"`
	StateHash     uint64                 `protobuf:"varint,10,opt,name=state_hash,json=stateHash,proto3" json:"state_hash,omitempty"`
	HashIndex     uint64                 `protobuf:"varint,11,opt,name=hash_index,json=hashIndex,proto3" json:"hash_index,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StatusResponse) Reset() {
	*x = StatusResponse{}
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StatusResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StatusResponse) ProtoMessage() {}

func (x *StatusResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*StatusResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{1}
}

func (x *StatusResponse) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *StatusResponse) GetState() string {
	if x != nil {
		return x.State
	}
	return ""
}

func (x *StatusResponse) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *StatusResponse) GetLeader() uint64 {
	if x != nil {
		return x.Leader
	}
	return 0
}

func (x *StatusResponse) GetCommit() uint64 {
	if x != nil {
		return x.Commit
	}
	return 0
}

func (x *StatusResponse) GetApplied() uint64 {
	if x != nil {
		return x.Applied
	}
	return 0
}

func (x *StatusResponse) GetFirstIndex() uint64 {
	if x != nil {
		return x.FirstIndex
	}
	return 0
}

func (x *StatusResponse) GetLastIndex() uint64 {
	if x != nil {
		return x.LastIndex
	}
	return 0
}

func (x *StatusResponse) GetSnapshotIndex() uint64 {
	if x != nil {
		return x.SnapshotIndex
	}
	return 0
}

func (x *StatusResponse) GetStateHash() uint64 {
	if x != nil {
		return x.StateHash
	}
	return 0
}

func (x *StatusResponse) GetHashIndex() uint64 {
	if x != nil {
		return x.HashIndex
	}
	return 0
}

var File_fivefamilies_v1_admin_proto protoreflect.FileDescriptor

const file_fivefamilies_v1_admin_proto_rawDesc = "" +
	"\n" +
	"\x1bfivefamilies/v1/admin.proto\x12\x0ffivefamilies.v1\"\x0f\n" +
	"\rStatusRequest\"\xb9\x02\n" +
	"\x0eStatusResponse\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x04R\x02id\x12\x14\n" +
	"\x05state\x18\x02 \x01(\tR\x05state\x12\x12\n" +
	"\x04term\x18\x03 \x01(\x04R\x04term\x12\x16\n" +
	"\x06leader\x18\x04 \x01(\x04R\x06leader\x12\x16\n" +
	"\x06commit\x18\x05 \x01(\x04R\x06commit\x12\x18\n" +
	"\aapplied\x18\x06 \x01(\x04R\aapplied\x12\x1f\n" +
	"\vfirst_index\x18\a \x01(\x04R\n" +
	"firstIndex\x12\x1d\n" +
	"\n" +
	"last_index\x18\b \x01(\x04R\tlastIndex\x12%\n" +
	"\x0esnapshot_index\x18\t \x01(\x04R\rsnapshotIndex\x12\x1d\n" +
	"\n" +
	"state_hash\x18\n" +
	" \x01(\x04R\tstateHash\x12\x1d\n" +
	"\n" +
	"hash_index\x18\v \x01(\x04R\thashIndex2R\n" +
	"\x05Admin\x12I\n" +
	"\x06Status\x12\x1e.fivefamilies.v1.StatusRequest\x1a\x1f.fivefamilies.v1.StatusResponseBHZFgithub.com/mirzanajafov/fivefamilies/api/fivefamiliesv1;fivefamiliesv1b\x06proto3"

var (
	file_fivefamilies_v1_admin_proto_rawDescOnce sync.Once
	file_fivefamilies_v1_admin_proto_rawDescData []byte
)

func file_fivefamilies_v1_admin_proto_rawDescGZIP() []byte {
	file_fivefamilies_v1_admin_proto_rawDescOnce.Do(func() {
		file_fivefamilies_v1_admin_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_admin_proto_rawDesc), len(file_fivefamilies_v1_admin_proto_rawDesc)))
	})
	return file_fivefamilies_v1_admin_proto_rawDescData
}

var file_fivefamilies_v1_admin_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_fivefamilies_v1_admin_proto_goTypes = []any{
	(*StatusRequest)(nil),
	(*StatusResponse)(nil),
}
var file_fivefamilies_v1_admin_proto_depIdxs = []int32{
	0,
	1,
	1,
	0,
	0,
	0,
	0,
}

func init() { file_fivefamilies_v1_admin_proto_init() }
func file_fivefamilies_v1_admin_proto_init() {
	if File_fivefamilies_v1_admin_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_admin_proto_rawDesc), len(file_fivefamilies_v1_admin_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_fivefamilies_v1_admin_proto_goTypes,
		DependencyIndexes: file_fivefamilies_v1_admin_proto_depIdxs,
		MessageInfos:      file_fivefamilies_v1_admin_proto_msgTypes,
	}.Build()
	File_fivefamilies_v1_admin_proto = out.File
	file_fivefamilies_v1_admin_proto_goTypes = nil
	file_fivefamilies_v1_admin_proto_depIdxs = nil
}
