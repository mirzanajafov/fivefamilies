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

type MessageType int32

const (
	MessageType_MESSAGE_TYPE_UNSPECIFIED     MessageType = 0
	MessageType_MESSAGE_TYPE_APP             MessageType = 1
	MessageType_MESSAGE_TYPE_APP_RESP        MessageType = 2
	MessageType_MESSAGE_TYPE_HEARTBEAT       MessageType = 3
	MessageType_MESSAGE_TYPE_HEARTBEAT_RESP  MessageType = 4
	MessageType_MESSAGE_TYPE_PRE_VOTE        MessageType = 5
	MessageType_MESSAGE_TYPE_PRE_VOTE_RESP   MessageType = 6
	MessageType_MESSAGE_TYPE_VOTE            MessageType = 7
	MessageType_MESSAGE_TYPE_VOTE_RESP       MessageType = 8
	MessageType_MESSAGE_TYPE_SNAP            MessageType = 10
	MessageType_MESSAGE_TYPE_READ_INDEX      MessageType = 11
	MessageType_MESSAGE_TYPE_READ_INDEX_RESP MessageType = 12
	MessageType_MESSAGE_TYPE_TIMEOUT_NOW     MessageType = 13
)

var (
	MessageType_name = map[int32]string{
		0:  "MESSAGE_TYPE_UNSPECIFIED",
		1:  "MESSAGE_TYPE_APP",
		2:  "MESSAGE_TYPE_APP_RESP",
		3:  "MESSAGE_TYPE_HEARTBEAT",
		4:  "MESSAGE_TYPE_HEARTBEAT_RESP",
		5:  "MESSAGE_TYPE_PRE_VOTE",
		6:  "MESSAGE_TYPE_PRE_VOTE_RESP",
		7:  "MESSAGE_TYPE_VOTE",
		8:  "MESSAGE_TYPE_VOTE_RESP",
		10: "MESSAGE_TYPE_SNAP",
		11: "MESSAGE_TYPE_READ_INDEX",
		12: "MESSAGE_TYPE_READ_INDEX_RESP",
		13: "MESSAGE_TYPE_TIMEOUT_NOW",
	}
	MessageType_value = map[string]int32{
		"MESSAGE_TYPE_UNSPECIFIED":     0,
		"MESSAGE_TYPE_APP":             1,
		"MESSAGE_TYPE_APP_RESP":        2,
		"MESSAGE_TYPE_HEARTBEAT":       3,
		"MESSAGE_TYPE_HEARTBEAT_RESP":  4,
		"MESSAGE_TYPE_PRE_VOTE":        5,
		"MESSAGE_TYPE_PRE_VOTE_RESP":   6,
		"MESSAGE_TYPE_VOTE":            7,
		"MESSAGE_TYPE_VOTE_RESP":       8,
		"MESSAGE_TYPE_SNAP":            10,
		"MESSAGE_TYPE_READ_INDEX":      11,
		"MESSAGE_TYPE_READ_INDEX_RESP": 12,
		"MESSAGE_TYPE_TIMEOUT_NOW":     13,
	}
)

func (x MessageType) Enum() *MessageType {
	p := new(MessageType)
	*p = x
	return p
}

func (x MessageType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (MessageType) Descriptor() protoreflect.EnumDescriptor {
	return file_fivefamilies_v1_peer_proto_enumTypes[0].Descriptor()
}

func (MessageType) Type() protoreflect.EnumType {
	return &file_fivefamilies_v1_peer_proto_enumTypes[0]
}

func (x MessageType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (MessageType) EnumDescriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{0}
}

type EntryType int32

const (
	EntryType_ENTRY_TYPE_NORMAL EntryType = 0
	EntryType_ENTRY_TYPE_CONFIG EntryType = 1
)

var (
	EntryType_name = map[int32]string{
		0: "ENTRY_TYPE_NORMAL",
		1: "ENTRY_TYPE_CONFIG",
	}
	EntryType_value = map[string]int32{
		"ENTRY_TYPE_NORMAL": 0,
		"ENTRY_TYPE_CONFIG": 1,
	}
)

func (x EntryType) Enum() *EntryType {
	p := new(EntryType)
	*p = x
	return p
}

func (x EntryType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (EntryType) Descriptor() protoreflect.EnumDescriptor {
	return file_fivefamilies_v1_peer_proto_enumTypes[1].Descriptor()
}

func (EntryType) Type() protoreflect.EnumType {
	return &file_fivefamilies_v1_peer_proto_enumTypes[1]
}

func (x EntryType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (EntryType) EnumDescriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{1}
}

type Entry struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Term          uint64                 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	Index         uint64                 `protobuf:"varint,2,opt,name=index,proto3" json:"index,omitempty"`
	Type          EntryType              `protobuf:"varint,3,opt,name=type,proto3,enum=fivefamilies.v1.EntryType" json:"type,omitempty"`
	Data          []byte                 `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Entry) Reset() {
	*x = Entry{}
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Entry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entry) ProtoMessage() {}

func (x *Entry) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*Entry) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{0}
}

func (x *Entry) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *Entry) GetIndex() uint64 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *Entry) GetType() EntryType {
	if x != nil {
		return x.Type
	}
	return EntryType_ENTRY_TYPE_NORMAL
}

func (x *Entry) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type SnapshotMeta struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Index         uint64                 `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	Term          uint64                 `protobuf:"varint,2,opt,name=term,proto3" json:"term,omitempty"`
	Voters        []uint64               `protobuf:"varint,3,rep,packed,name=voters,proto3" json:"voters,omitempty"`
	Learners      []uint64               `protobuf:"varint,4,rep,packed,name=learners,proto3" json:"learners,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SnapshotMeta) Reset() {
	*x = SnapshotMeta{}
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SnapshotMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SnapshotMeta) ProtoMessage() {}

func (x *SnapshotMeta) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*SnapshotMeta) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{1}
}

func (x *SnapshotMeta) GetIndex() uint64 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *SnapshotMeta) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *SnapshotMeta) GetVoters() []uint64 {
	if x != nil {
		return x.Voters
	}
	return nil
}

func (x *SnapshotMeta) GetLearners() []uint64 {
	if x != nil {
		return x.Learners
	}
	return nil
}

type RaftMessage struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Type          MessageType            `protobuf:"varint,1,opt,name=type,proto3,enum=fivefamilies.v1.MessageType" json:"type,omitempty"`
	From          uint64                 `protobuf:"varint,2,opt,name=from,proto3" json:"from,omitempty"`
	To            uint64                 `protobuf:"varint,3,opt,name=to,proto3" json:"to,omitempty"`
	Term          uint64                 `protobuf:"varint,4,opt,name=term,proto3" json:"term,omitempty"`
	LogTerm       uint64                 `protobuf:"varint,5,opt,name=log_term,json=logTerm,proto3" json:"log_term,omitempty"`
	Index         uint64                 `protobuf:"varint,6,opt,name=index,proto3" json:"index,omitempty"`
	Entries       []*Entry               `protobuf:"bytes,7,rep,name=entries,proto3" json:"entries,omitempty"`
	Commit        uint64                 `protobuf:"varint,8,opt,name=commit,proto3" json:"commit,omitempty"`
	Reject        bool                   `protobuf:"varint,9,opt,name=reject,proto3" json:"reject,omitempty"`
	Hint          uint64                 `protobuf:"varint,10,opt,name=hint,proto3" json:"hint,omitempty"`
	Context       uint64                 `protobuf:"varint,11,opt,name=context,proto3" json:"context,omitempty"`
	Snapshot      *SnapshotMeta          `protobuf:"bytes,12,opt,name=snapshot,proto3" json:"snapshot,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RaftMessage) Reset() {
	*x = RaftMessage{}
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RaftMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RaftMessage) ProtoMessage() {}

func (x *RaftMessage) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*RaftMessage) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{2}
}

func (x *RaftMessage) GetType() MessageType {
	if x != nil {
		return x.Type
	}
	return MessageType_MESSAGE_TYPE_UNSPECIFIED
}

func (x *RaftMessage) GetFrom() uint64 {
	if x != nil {
		return x.From
	}
	return 0
}

func (x *RaftMessage) GetTo() uint64 {
	if x != nil {
		return x.To
	}
	return 0
}

func (x *RaftMessage) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RaftMessage) GetLogTerm() uint64 {
	if x != nil {
		return x.LogTerm
	}
	return 0
}

func (x *RaftMessage) GetIndex() uint64 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *RaftMessage) GetEntries() []*Entry {
	if x != nil {
		return x.Entries
	}
	return nil
}

func (x *RaftMessage) GetCommit() uint64 {
	if x != nil {
		return x.Commit
	}
	return 0
}

func (x *RaftMessage) GetReject() bool {
	if x != nil {
		return x.Reject
	}
	return false
}

func (x *RaftMessage) GetHint() uint64 {
	if x != nil {
		return x.Hint
	}
	return 0
}

func (x *RaftMessage) GetContext() uint64 {
	if x != nil {
		return x.Context
	}
	return 0
}

func (x *RaftMessage) GetSnapshot() *SnapshotMeta {
	if x != nil {
		return x.Snapshot
	}
	return nil
}

type SendRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Messages      []*RaftMessage         `protobuf:"bytes,1,rep,name=messages,proto3" json:"messages,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SendRequest) Reset() {
	*x = SendRequest{}
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SendRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SendRequest) ProtoMessage() {}

func (x *SendRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*SendRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{3}
}

func (x *SendRequest) GetMessages() []*RaftMessage {
	if x != nil {
		return x.Messages
	}
	return nil
}

type SendResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SendResponse) Reset() {
	*x = SendResponse{}
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SendResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SendResponse) ProtoMessage() {}

func (x *SendResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*SendResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{4}
}

type InstallSnapshotRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Message       *RaftMessage           `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
	Data          []byte                 `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *InstallSnapshotRequest) Reset() {
	*x = InstallSnapshotRequest{}
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InstallSnapshotRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InstallSnapshotRequest) ProtoMessage() {}

func (x *InstallSnapshotRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*InstallSnapshotRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{5}
}

func (x *InstallSnapshotRequest) GetMessage() *RaftMessage {
	if x != nil {
		return x.Message
	}
	return nil
}

func (x *InstallSnapshotRequest) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type InstallSnapshotResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *InstallSnapshotResponse) Reset() {
	*x = InstallSnapshotResponse{}
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InstallSnapshotResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InstallSnapshotResponse) ProtoMessage() {}

func (x *InstallSnapshotResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*InstallSnapshotResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{6}
}

var File_fivefamilies_v1_peer_proto protoreflect.FileDescriptor

const file_fivefamilies_v1_peer_proto_rawDesc = "" +
	"\n" +
	"\x1afivefamilies/v1/peer.proto\x12\x0ffivefamilies.v1\"u\n" +
	"\x05Entry\x12\x12\n" +
	"\x04term\x18\x01 \x01(\x04R\x04term\x12\x14\n" +
	"\x05index\x18\x02 \x01(\x04R\x05index\x12.\n" +
	"\x04type\x18\x03 \x01(\x0e2\x1a.fivefamilies.v1.EntryTypeR\x04type\x12\x12\n" +
	"\x04data\x18\x04 \x01(\fR\x04data\"l\n" +
	"\fSnapshotMeta\x12\x14\n" +
	"\x05index\x18\x01 \x01(\x04R\x05index\x12\x12\n" +
	"\x04term\x18\x02 \x01(\x04R\x04term\x12\x16\n" +
	"\x06voters\x18\x03 \x03(\x04R\x06voters\x12\x1a\n" +
	"\blearners\x18\x04 \x03(\x04R\blearners\"\xf3\x02\n" +
	"\vRaftMessage\x120\n" +
	"\x04type\x18\x01 \x01(\x0e2\x1c.fivefamilies.v1.MessageTypeR\x04type\x12\x12\n" +
	"\x04from\x18\x02 \x01(\x04R\x04from\x12\x0e\n" +
	"\x02to\x18\x03 \x01(\x04R\x02to\x12\x12\n" +
	"\x04term\x18\x04 \x01(\x04R\x04term\x12\x19\n" +
	"\blog_term\x18\x05 \x01(\x04R\alogTerm\x12\x14\n" +
	"\x05index\x18\x06 \x01(\x04R\x05index\x120\n" +
	"\aentries\x18\a \x03(\v2\x16.fivefamilies.v1.EntryR\aentries\x12\x16\n" +
	"\x06commit\x18\b \x01(\x04R\x06commit\x12\x16\n" +
	"\x06reject\x18\t \x01(\bR\x06reject\x12\x12\n" +
	"\x04hint\x18\n" +
	" \x01(\x04R\x04hint\x12\x18\n" +
	"\acontext\x18\v \x01(\x04R\acontext\x129\n" +
	"\bsnapshot\x18\f \x01(\v2\x1d.fivefamilies.v1.SnapshotMetaR\bsnapshot\"G\n" +
	"\vSendRequest\x128\n" +
	"\bmessages\x18\x01 \x03(\v2\x1c.fivefamilies.v1.RaftMessageR\bmessages\"\x0e\n" +
	"\fSendResponse\"d\n" +
	"\x16InstallSnapshotRequest\x126\n" +
	"\amessage\x18\x01 \x01(\v2\x1c.fivefamilies.v1.RaftMessageR\amessage\x12\x12\n" +
	"\x04data\x18\x02 \x01(\fR\x04data\"\x19\n" +
	"\x17InstallSnapshotResponse*\x81\x03\n" +
	"\vMessageType\x12\x1c\n" +
	"\x18MESSAGE_TYPE_UNSPECIFIED\x10\x00\x12\x14\n" +
	"\x10MESSAGE_TYPE_APP\x10\x01\x12\x19\n" +
	"\x15MESSAGE_TYPE_APP_RESP\x10\x02\x12\x1a\n" +
	"\x16MESSAGE_TYPE_HEARTBEAT\x10\x03\x12\x1f\n" +
	"\x1bMESSAGE_TYPE_HEARTBEAT_RESP\x10\x04\x12\x19\n" +
	"\x15MESSAGE_TYPE_PRE_VOTE\x10\x05\x12\x1e\n" +
	"\x1aMESSAGE_TYPE_PRE_VOTE_RESP\x10\x06\x12\x15\n" +
	"\x11MESSAGE_TYPE_VOTE\x10\a\x12\x1a\n" +
	"\x16MESSAGE_TYPE_VOTE_RESP\x10\b\x12\x15\n" +
	"\x11MESSAGE_TYPE_SNAP\x10\n" +
	"\x12\x1b\n" +
	"\x17MESSAGE_TYPE_READ_INDEX\x10\v\x12 \n" +
	"\x1cMESSAGE_TYPE_READ_INDEX_RESP\x10\f\x12\x1c\n" +
	"\x18MESSAGE_TYPE_TIMEOUT_NOW\x10\r\"\x04\b\t\x10\t*9\n" +
	"\tEntryType\x12\x15\n" +
	"\x11ENTRY_TYPE_NORMAL\x10\x00\x12\x15\n" +
	"\x11ENTRY_TYPE_CONFIG\x10\x012\xb5\x01\n" +
	"\x04Peer\x12E\n" +
	"\x04Send\x12\x1c.fivefamilies.v1.SendRequest\x1a\x1d.fivefamilies.v1.SendResponse(\x01\x12f\n" +
	"\x0fInstallSnapshot\x12'.fivefamilies.v1.InstallSnapshotRequest\x1a(.fivefamilies.v1.InstallSnapshotResponse(\x01BHZFgithub.com/mirzanajafov/fivefamilies/api/fivefamiliesv1;fivefamiliesv1b\x06proto3"

var (
	file_fivefamilies_v1_peer_proto_rawDescOnce sync.Once
	file_fivefamilies_v1_peer_proto_rawDescData []byte
)

func file_fivefamilies_v1_peer_proto_rawDescGZIP() []byte {
	file_fivefamilies_v1_peer_proto_rawDescOnce.Do(func() {
		file_fivefamilies_v1_peer_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_peer_proto_rawDesc), len(file_fivefamilies_v1_peer_proto_rawDesc)))
	})
	return file_fivefamilies_v1_peer_proto_rawDescData
}

var file_fivefamilies_v1_peer_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_fivefamilies_v1_peer_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_fivefamilies_v1_peer_proto_goTypes = []any{
	(MessageType)(0),
	(EntryType)(0),
	(*Entry)(nil),
	(*SnapshotMeta)(nil),
	(*RaftMessage)(nil),
	(*SendRequest)(nil),
	(*SendResponse)(nil),
	(*InstallSnapshotRequest)(nil),
	(*InstallSnapshotResponse)(nil),
}
var file_fivefamilies_v1_peer_proto_depIdxs = []int32{
	1,
	0,
	2,
	3,
	4,
	4,
	5,
	7,
	6,
	8,
	8,
	6,
	6,
	6,
	0,
}

func init() { file_fivefamilies_v1_peer_proto_init() }
func file_fivefamilies_v1_peer_proto_init() {
	if File_fivefamilies_v1_peer_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_peer_proto_rawDesc), len(file_fivefamilies_v1_peer_proto_rawDesc)),
			NumEnums:      2,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_fivefamilies_v1_peer_proto_goTypes,
		DependencyIndexes: file_fivefamilies_v1_peer_proto_depIdxs,
		EnumInfos:         file_fivefamilies_v1_peer_proto_enumTypes,
		MessageInfos:      file_fivefamilies_v1_peer_proto_msgTypes,
	}.Build()
	File_fivefamilies_v1_peer_proto = out.File
	file_fivefamilies_v1_peer_proto_goTypes = nil
	file_fivefamilies_v1_peer_proto_depIdxs = nil
}
