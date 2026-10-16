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

type MemberChange int32

const (
	MemberChange_MEMBER_CHANGE_UNSPECIFIED MemberChange = 0
	MemberChange_MEMBER_CHANGE_ADD_LEARNER MemberChange = 1
	MemberChange_MEMBER_CHANGE_PROMOTE     MemberChange = 2
	MemberChange_MEMBER_CHANGE_REMOVE      MemberChange = 3
)

var (
	MemberChange_name = map[int32]string{
		0: "MEMBER_CHANGE_UNSPECIFIED",
		1: "MEMBER_CHANGE_ADD_LEARNER",
		2: "MEMBER_CHANGE_PROMOTE",
		3: "MEMBER_CHANGE_REMOVE",
	}
	MemberChange_value = map[string]int32{
		"MEMBER_CHANGE_UNSPECIFIED": 0,
		"MEMBER_CHANGE_ADD_LEARNER": 1,
		"MEMBER_CHANGE_PROMOTE":     2,
		"MEMBER_CHANGE_REMOVE":      3,
	}
)

func (x MemberChange) Enum() *MemberChange {
	p := new(MemberChange)
	*p = x
	return p
}

func (x MemberChange) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (MemberChange) Descriptor() protoreflect.EnumDescriptor {
	return file_fivefamilies_v1_admin_proto_enumTypes[0].Descriptor()
}

func (MemberChange) Type() protoreflect.EnumType {
	return &file_fivefamilies_v1_admin_proto_enumTypes[0]
}

func (x MemberChange) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (MemberChange) EnumDescriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{0}
}

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
	Voters        []uint64               `protobuf:"varint,12,rep,packed,name=voters,proto3" json:"voters,omitempty"`
	Learners      []uint64               `protobuf:"varint,13,rep,packed,name=learners,proto3" json:"learners,omitempty"`
	Transferee    uint64                 `protobuf:"varint,14,opt,name=transferee,proto3" json:"transferee,omitempty"`
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

func (x *StatusResponse) GetVoters() []uint64 {
	if x != nil {
		return x.Voters
	}
	return nil
}

func (x *StatusResponse) GetLearners() []uint64 {
	if x != nil {
		return x.Learners
	}
	return nil
}

func (x *StatusResponse) GetTransferee() uint64 {
	if x != nil {
		return x.Transferee
	}
	return 0
}

type ChangeMembersRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Change        MemberChange           `protobuf:"varint,1,opt,name=change,proto3,enum=fivefamilies.v1.MemberChange" json:"change,omitempty"`
	Node          uint64                 `protobuf:"varint,2,opt,name=node,proto3" json:"node,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ChangeMembersRequest) Reset() {
	*x = ChangeMembersRequest{}
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ChangeMembersRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChangeMembersRequest) ProtoMessage() {}

func (x *ChangeMembersRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ChangeMembersRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{2}
}

func (x *ChangeMembersRequest) GetChange() MemberChange {
	if x != nil {
		return x.Change
	}
	return MemberChange_MEMBER_CHANGE_UNSPECIFIED
}

func (x *ChangeMembersRequest) GetNode() uint64 {
	if x != nil {
		return x.Node
	}
	return 0
}

type ChangeMembersResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Index         uint64                 `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	Voters        []uint64               `protobuf:"varint,2,rep,packed,name=voters,proto3" json:"voters,omitempty"`
	Learners      []uint64               `protobuf:"varint,3,rep,packed,name=learners,proto3" json:"learners,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ChangeMembersResponse) Reset() {
	*x = ChangeMembersResponse{}
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ChangeMembersResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChangeMembersResponse) ProtoMessage() {}

func (x *ChangeMembersResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ChangeMembersResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{3}
}

func (x *ChangeMembersResponse) GetIndex() uint64 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *ChangeMembersResponse) GetVoters() []uint64 {
	if x != nil {
		return x.Voters
	}
	return nil
}

func (x *ChangeMembersResponse) GetLearners() []uint64 {
	if x != nil {
		return x.Learners
	}
	return nil
}

type TransferLeadershipRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	To            uint64                 `protobuf:"varint,1,opt,name=to,proto3" json:"to,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TransferLeadershipRequest) Reset() {
	*x = TransferLeadershipRequest{}
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TransferLeadershipRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TransferLeadershipRequest) ProtoMessage() {}

func (x *TransferLeadershipRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*TransferLeadershipRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{4}
}

func (x *TransferLeadershipRequest) GetTo() uint64 {
	if x != nil {
		return x.To
	}
	return 0
}

type TransferLeadershipResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Leader        uint64                 `protobuf:"varint,1,opt,name=leader,proto3" json:"leader,omitempty"`
	Term          uint64                 `protobuf:"varint,2,opt,name=term,proto3" json:"term,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TransferLeadershipResponse) Reset() {
	*x = TransferLeadershipResponse{}
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TransferLeadershipResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TransferLeadershipResponse) ProtoMessage() {}

func (x *TransferLeadershipResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*TransferLeadershipResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{5}
}

func (x *TransferLeadershipResponse) GetLeader() uint64 {
	if x != nil {
		return x.Leader
	}
	return 0
}

func (x *TransferLeadershipResponse) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

var File_fivefamilies_v1_admin_proto protoreflect.FileDescriptor

const file_fivefamilies_v1_admin_proto_rawDesc = "" +
	"\n" +
	"\x1bfivefamilies/v1/admin.proto\x12\x0ffivefamilies.v1\"\x0f\n" +
	"\rStatusRequest\"\x8d\x03\n" +
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
	"hash_index\x18\v \x01(\x04R\thashIndex\x12\x16\n" +
	"\x06voters\x18\f \x03(\x04R\x06voters\x12\x1a\n" +
	"\blearners\x18\r \x03(\x04R\blearners\x12\x1e\n" +
	"\n" +
	"transferee\x18\x0e \x01(\x04R\n" +
	"transferee\"a\n" +
	"\x14ChangeMembersRequest\x125\n" +
	"\x06change\x18\x01 \x01(\x0e2\x1d.fivefamilies.v1.MemberChangeR\x06change\x12\x12\n" +
	"\x04node\x18\x02 \x01(\x04R\x04node\"a\n" +
	"\x15ChangeMembersResponse\x12\x14\n" +
	"\x05index\x18\x01 \x01(\x04R\x05index\x12\x16\n" +
	"\x06voters\x18\x02 \x03(\x04R\x06voters\x12\x1a\n" +
	"\blearners\x18\x03 \x03(\x04R\blearners\"+\n" +
	"\x19TransferLeadershipRequest\x12\x0e\n" +
	"\x02to\x18\x01 \x01(\x04R\x02to\"H\n" +
	"\x1aTransferLeadershipResponse\x12\x16\n" +
	"\x06leader\x18\x01 \x01(\x04R\x06leader\x12\x12\n" +
	"\x04term\x18\x02 \x01(\x04R\x04term*\x81\x01\n" +
	"\fMemberChange\x12\x1d\n" +
	"\x19MEMBER_CHANGE_UNSPECIFIED\x10\x00\x12\x1d\n" +
	"\x19MEMBER_CHANGE_ADD_LEARNER\x10\x01\x12\x19\n" +
	"\x15MEMBER_CHANGE_PROMOTE\x10\x02\x12\x18\n" +
	"\x14MEMBER_CHANGE_REMOVE\x10\x032\xa1\x02\n" +
	"\x05Admin\x12I\n" +
	"\x06Status\x12\x1e.fivefamilies.v1.StatusRequest\x1a\x1f.fivefamilies.v1.StatusResponse\x12^\n" +
	"\rChangeMembers\x12%.fivefamilies.v1.ChangeMembersRequest\x1a&.fivefamilies.v1.ChangeMembersResponse\x12m\n" +
	"\x12TransferLeadership\x12*.fivefamilies.v1.TransferLeadershipRequest\x1a+.fivefamilies.v1.TransferLeadershipResponseBHZFgithub.com/mirzanajafov/fivefamilies/api/fivefamiliesv1;fivefamiliesv1b\x06proto3"

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

var file_fivefamilies_v1_admin_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_fivefamilies_v1_admin_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_fivefamilies_v1_admin_proto_goTypes = []any{
	(MemberChange)(0),
	(*StatusRequest)(nil),
	(*StatusResponse)(nil),
	(*ChangeMembersRequest)(nil),
	(*ChangeMembersResponse)(nil),
	(*TransferLeadershipRequest)(nil),
	(*TransferLeadershipResponse)(nil),
}
var file_fivefamilies_v1_admin_proto_depIdxs = []int32{
	0,
	1,
	3,
	5,
	2,
	4,
	6,
	4,
	1,
	1,
	1,
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
			NumEnums:      1,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_fivefamilies_v1_admin_proto_goTypes,
		DependencyIndexes: file_fivefamilies_v1_admin_proto_depIdxs,
		EnumInfos:         file_fivefamilies_v1_admin_proto_enumTypes,
		MessageInfos:      file_fivefamilies_v1_admin_proto_msgTypes,
	}.Build()
	File_fivefamilies_v1_admin_proto = out.File
	file_fivefamilies_v1_admin_proto_goTypes = nil
	file_fivefamilies_v1_admin_proto_depIdxs = nil
}
