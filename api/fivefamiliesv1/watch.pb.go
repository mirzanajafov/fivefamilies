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

type EventType int32

const (
	EventType_EVENT_TYPE_UNSPECIFIED EventType = 0
	EventType_EVENT_TYPE_PUT         EventType = 1
	EventType_EVENT_TYPE_DELETE      EventType = 2
)

var (
	EventType_name = map[int32]string{
		0: "EVENT_TYPE_UNSPECIFIED",
		1: "EVENT_TYPE_PUT",
		2: "EVENT_TYPE_DELETE",
	}
	EventType_value = map[string]int32{
		"EVENT_TYPE_UNSPECIFIED": 0,
		"EVENT_TYPE_PUT":         1,
		"EVENT_TYPE_DELETE":      2,
	}
)

func (x EventType) Enum() *EventType {
	p := new(EventType)
	*p = x
	return p
}

func (x EventType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (EventType) Descriptor() protoreflect.EnumDescriptor {
	return file_fivefamilies_v1_watch_proto_enumTypes[0].Descriptor()
}

func (EventType) Type() protoreflect.EnumType {
	return &file_fivefamilies_v1_watch_proto_enumTypes[0]
}

func (x EventType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (EventType) EnumDescriptor() ([]byte, []int) {
	return file_fivefamilies_v1_watch_proto_rawDescGZIP(), []int{0}
}

type WatchRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Key           string                 `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Prefix        bool                   `protobuf:"varint,2,opt,name=prefix,proto3" json:"prefix,omitempty"`
	StartRev      uint64                 `protobuf:"varint,3,opt,name=start_rev,json=startRev,proto3" json:"start_rev,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *WatchRequest) Reset() {
	*x = WatchRequest{}
	mi := &file_fivefamilies_v1_watch_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WatchRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WatchRequest) ProtoMessage() {}

func (x *WatchRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_watch_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*WatchRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_watch_proto_rawDescGZIP(), []int{0}
}

func (x *WatchRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *WatchRequest) GetPrefix() bool {
	if x != nil {
		return x.Prefix
	}
	return false
}

func (x *WatchRequest) GetStartRev() uint64 {
	if x != nil {
		return x.StartRev
	}
	return 0
}

type WatchEvent struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Type          EventType              `protobuf:"varint,1,opt,name=type,proto3,enum=fivefamilies.v1.EventType" json:"type,omitempty"`
	Kv            *KeyValue              `protobuf:"bytes,2,opt,name=kv,proto3" json:"kv,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *WatchEvent) Reset() {
	*x = WatchEvent{}
	mi := &file_fivefamilies_v1_watch_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WatchEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WatchEvent) ProtoMessage() {}

func (x *WatchEvent) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_watch_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*WatchEvent) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_watch_proto_rawDescGZIP(), []int{1}
}

func (x *WatchEvent) GetType() EventType {
	if x != nil {
		return x.Type
	}
	return EventType_EVENT_TYPE_UNSPECIFIED
}

func (x *WatchEvent) GetKv() *KeyValue {
	if x != nil {
		return x.Kv
	}
	return nil
}

type WatchResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Events        []*WatchEvent          `protobuf:"bytes,1,rep,name=events,proto3" json:"events,omitempty"`
	Rev           uint64                 `protobuf:"varint,2,opt,name=rev,proto3" json:"rev,omitempty"`
	Progress      bool                   `protobuf:"varint,3,opt,name=progress,proto3" json:"progress,omitempty"`
	CompactRev    uint64                 `protobuf:"varint,4,opt,name=compact_rev,json=compactRev,proto3" json:"compact_rev,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *WatchResponse) Reset() {
	*x = WatchResponse{}
	mi := &file_fivefamilies_v1_watch_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WatchResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WatchResponse) ProtoMessage() {}

func (x *WatchResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_watch_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*WatchResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_watch_proto_rawDescGZIP(), []int{2}
}

func (x *WatchResponse) GetEvents() []*WatchEvent {
	if x != nil {
		return x.Events
	}
	return nil
}

func (x *WatchResponse) GetRev() uint64 {
	if x != nil {
		return x.Rev
	}
	return 0
}

func (x *WatchResponse) GetProgress() bool {
	if x != nil {
		return x.Progress
	}
	return false
}

func (x *WatchResponse) GetCompactRev() uint64 {
	if x != nil {
		return x.CompactRev
	}
	return 0
}

var File_fivefamilies_v1_watch_proto protoreflect.FileDescriptor

const file_fivefamilies_v1_watch_proto_rawDesc = "" +
	"\n" +
	"\x1bfivefamilies/v1/watch.proto\x12\x0ffivefamilies.v1\x1a\x18fivefamilies/v1/kv.proto\"U\n" +
	"\fWatchRequest\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x16\n" +
	"\x06prefix\x18\x02 \x01(\bR\x06prefix\x12\x1b\n" +
	"\tstart_rev\x18\x03 \x01(\x04R\bstartRev\"g\n" +
	"\n" +
	"WatchEvent\x12.\n" +
	"\x04type\x18\x01 \x01(\x0e2\x1a.fivefamilies.v1.EventTypeR\x04type\x12)\n" +
	"\x02kv\x18\x02 \x01(\v2\x19.fivefamilies.v1.KeyValueR\x02kv\"\x93\x01\n" +
	"\rWatchResponse\x123\n" +
	"\x06events\x18\x01 \x03(\v2\x1b.fivefamilies.v1.WatchEventR\x06events\x12\x10\n" +
	"\x03rev\x18\x02 \x01(\x04R\x03rev\x12\x1a\n" +
	"\bprogress\x18\x03 \x01(\bR\bprogress\x12\x1f\n" +
	"\vcompact_rev\x18\x04 \x01(\x04R\n" +
	"compactRev*R\n" +
	"\tEventType\x12\x1a\n" +
	"\x16EVENT_TYPE_UNSPECIFIED\x10\x00\x12\x12\n" +
	"\x0eEVENT_TYPE_PUT\x10\x01\x12\x15\n" +
	"\x11EVENT_TYPE_DELETE\x10\x022Q\n" +
	"\x05Watch\x12H\n" +
	"\x05Watch\x12\x1d.fivefamilies.v1.WatchRequest\x1a\x1e.fivefamilies.v1.WatchResponse0\x01BHZFgithub.com/mirzanajafov/fivefamilies/api/fivefamiliesv1;fivefamiliesv1b\x06proto3"

var (
	file_fivefamilies_v1_watch_proto_rawDescOnce sync.Once
	file_fivefamilies_v1_watch_proto_rawDescData []byte
)

func file_fivefamilies_v1_watch_proto_rawDescGZIP() []byte {
	file_fivefamilies_v1_watch_proto_rawDescOnce.Do(func() {
		file_fivefamilies_v1_watch_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_watch_proto_rawDesc), len(file_fivefamilies_v1_watch_proto_rawDesc)))
	})
	return file_fivefamilies_v1_watch_proto_rawDescData
}

var file_fivefamilies_v1_watch_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_fivefamilies_v1_watch_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_fivefamilies_v1_watch_proto_goTypes = []any{
	(EventType)(0),
	(*WatchRequest)(nil),
	(*WatchEvent)(nil),
	(*WatchResponse)(nil),
	(*KeyValue)(nil),
}
var file_fivefamilies_v1_watch_proto_depIdxs = []int32{
	0,
	4,
	2,
	1,
	3,
	4,
	3,
	3,
	3,
	0,
}

func init() { file_fivefamilies_v1_watch_proto_init() }
func file_fivefamilies_v1_watch_proto_init() {
	if File_fivefamilies_v1_watch_proto != nil {
		return
	}
	file_fivefamilies_v1_kv_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_watch_proto_rawDesc), len(file_fivefamilies_v1_watch_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_fivefamilies_v1_watch_proto_goTypes,
		DependencyIndexes: file_fivefamilies_v1_watch_proto_depIdxs,
		EnumInfos:         file_fivefamilies_v1_watch_proto_enumTypes,
		MessageInfos:      file_fivefamilies_v1_watch_proto_msgTypes,
	}.Build()
	File_fivefamilies_v1_watch_proto = out.File
	file_fivefamilies_v1_watch_proto_goTypes = nil
	file_fivefamilies_v1_watch_proto_depIdxs = nil
}
