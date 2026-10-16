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

type Entry struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Term          uint64                 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	Index         uint64                 `protobuf:"varint,2,opt,name=index,proto3" json:"index,omitempty"`
	Type          uint32                 `protobuf:"varint,3,opt,name=type,proto3" json:"type,omitempty"`
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

func (x *Entry) GetType() uint32 {
	if x != nil {
		return x.Type
	}
	return 0
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
	Type          uint32                 `protobuf:"varint,1,opt,name=type,proto3" json:"type,omitempty"`
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

func (x *RaftMessage) GetType() uint32 {
	if x != nil {
		return x.Type
	}
	return 0
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

type Batch struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Messages      []*RaftMessage         `protobuf:"bytes,1,rep,name=messages,proto3" json:"messages,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Batch) Reset() {
	*x = Batch{}
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Batch) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Batch) ProtoMessage() {}

func (x *Batch) ProtoReflect() protoreflect.Message {
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

func (*Batch) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{3}
}

func (x *Batch) GetMessages() []*RaftMessage {
	if x != nil {
		return x.Messages
	}
	return nil
}

type SnapshotChunk struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Message       *RaftMessage           `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
	Data          []byte                 `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SnapshotChunk) Reset() {
	*x = SnapshotChunk{}
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SnapshotChunk) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SnapshotChunk) ProtoMessage() {}

func (x *SnapshotChunk) ProtoReflect() protoreflect.Message {
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

func (*SnapshotChunk) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{4}
}

func (x *SnapshotChunk) GetMessage() *RaftMessage {
	if x != nil {
		return x.Message
	}
	return nil
}

func (x *SnapshotChunk) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type Ack struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Ack) Reset() {
	*x = Ack{}
	mi := &file_fivefamilies_v1_peer_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Ack) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Ack) ProtoMessage() {}

func (x *Ack) ProtoReflect() protoreflect.Message {
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

func (*Ack) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_peer_proto_rawDescGZIP(), []int{5}
}

var File_fivefamilies_v1_peer_proto protoreflect.FileDescriptor

const file_fivefamilies_v1_peer_proto_rawDesc = "" +
	"\n" +
	"\x1afivefamilies/v1/peer.proto\x12\x0ffivefamilies.v1\"Y\n" +
	"\x05Entry\x12\x12\n" +
	"\x04term\x18\x01 \x01(\x04R\x04term\x12\x14\n" +
	"\x05index\x18\x02 \x01(\x04R\x05index\x12\x12\n" +
	"\x04type\x18\x03 \x01(\rR\x04type\x12\x12\n" +
	"\x04data\x18\x04 \x01(\fR\x04data\"l\n" +
	"\fSnapshotMeta\x12\x14\n" +
	"\x05index\x18\x01 \x01(\x04R\x05index\x12\x12\n" +
	"\x04term\x18\x02 \x01(\x04R\x04term\x12\x16\n" +
	"\x06voters\x18\x03 \x03(\x04R\x06voters\x12\x1a\n" +
	"\blearners\x18\x04 \x03(\x04R\blearners\"\xd5\x02\n" +
	"\vRaftMessage\x12\x12\n" +
	"\x04type\x18\x01 \x01(\rR\x04type\x12\x12\n" +
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
	"\bsnapshot\x18\f \x01(\v2\x1d.fivefamilies.v1.SnapshotMetaR\bsnapshot\"A\n" +
	"\x05Batch\x128\n" +
	"\bmessages\x18\x01 \x03(\v2\x1c.fivefamilies.v1.RaftMessageR\bmessages\"[\n" +
	"\rSnapshotChunk\x126\n" +
	"\amessage\x18\x01 \x01(\v2\x1c.fivefamilies.v1.RaftMessageR\amessage\x12\x12\n" +
	"\x04data\x18\x02 \x01(\fR\x04data\"\x05\n" +
	"\x03Ack2\x89\x01\n" +
	"\x04Peer\x126\n" +
	"\x04Send\x12\x16.fivefamilies.v1.Batch\x1a\x14.fivefamilies.v1.Ack(\x01\x12I\n" +
	"\x0fInstallSnapshot\x12\x1e.fivefamilies.v1.SnapshotChunk\x1a\x14.fivefamilies.v1.Ack(\x01BHZFgithub.com/mirzanajafov/fivefamilies/api/fivefamiliesv1;fivefamiliesv1b\x06proto3"

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

var file_fivefamilies_v1_peer_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_fivefamilies_v1_peer_proto_goTypes = []any{
	(*Entry)(nil),
	(*SnapshotMeta)(nil),
	(*RaftMessage)(nil),
	(*Batch)(nil),
	(*SnapshotChunk)(nil),
	(*Ack)(nil),
}
var file_fivefamilies_v1_peer_proto_depIdxs = []int32{
	0,
	1,
	2,
	2,
	3,
	4,
	5,
	5,
	6,
	4,
	4,
	4,
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
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_fivefamilies_v1_peer_proto_goTypes,
		DependencyIndexes: file_fivefamilies_v1_peer_proto_depIdxs,
		MessageInfos:      file_fivefamilies_v1_peer_proto_msgTypes,
	}.Build()
	File_fivefamilies_v1_peer_proto = out.File
	file_fivefamilies_v1_peer_proto_goTypes = nil
	file_fivefamilies_v1_peer_proto_depIdxs = nil
}
