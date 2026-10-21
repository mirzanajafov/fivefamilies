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

type Role int32

const (
	Role_ROLE_UNSPECIFIED   Role = 0
	Role_ROLE_FOLLOWER      Role = 1
	Role_ROLE_PRE_CANDIDATE Role = 2
	Role_ROLE_CANDIDATE     Role = 3
	Role_ROLE_LEADER        Role = 4
)

var (
	Role_name = map[int32]string{
		0: "ROLE_UNSPECIFIED",
		1: "ROLE_FOLLOWER",
		2: "ROLE_PRE_CANDIDATE",
		3: "ROLE_CANDIDATE",
		4: "ROLE_LEADER",
	}
	Role_value = map[string]int32{
		"ROLE_UNSPECIFIED":   0,
		"ROLE_FOLLOWER":      1,
		"ROLE_PRE_CANDIDATE": 2,
		"ROLE_CANDIDATE":     3,
		"ROLE_LEADER":        4,
	}
)

func (x Role) Enum() *Role {
	p := new(Role)
	*p = x
	return p
}

func (x Role) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Role) Descriptor() protoreflect.EnumDescriptor {
	return file_fivefamilies_v1_admin_proto_enumTypes[0].Descriptor()
}

func (Role) Type() protoreflect.EnumType {
	return &file_fivefamilies_v1_admin_proto_enumTypes[0]
}

func (x Role) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (Role) EnumDescriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{0}
}

type ChaosAction int32

const (
	ChaosAction_CHAOS_ACTION_UNSPECIFIED ChaosAction = 0
	ChaosAction_CHAOS_ACTION_STALL_DISK  ChaosAction = 1
	ChaosAction_CHAOS_ACTION_PAUSE       ChaosAction = 2
	ChaosAction_CHAOS_ACTION_ISOLATE     ChaosAction = 3
	ChaosAction_CHAOS_ACTION_HEAL        ChaosAction = 4
)

var (
	ChaosAction_name = map[int32]string{
		0: "CHAOS_ACTION_UNSPECIFIED",
		1: "CHAOS_ACTION_STALL_DISK",
		2: "CHAOS_ACTION_PAUSE",
		3: "CHAOS_ACTION_ISOLATE",
		4: "CHAOS_ACTION_HEAL",
	}
	ChaosAction_value = map[string]int32{
		"CHAOS_ACTION_UNSPECIFIED": 0,
		"CHAOS_ACTION_STALL_DISK":  1,
		"CHAOS_ACTION_PAUSE":       2,
		"CHAOS_ACTION_ISOLATE":     3,
		"CHAOS_ACTION_HEAL":        4,
	}
)

func (x ChaosAction) Enum() *ChaosAction {
	p := new(ChaosAction)
	*p = x
	return p
}

func (x ChaosAction) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ChaosAction) Descriptor() protoreflect.EnumDescriptor {
	return file_fivefamilies_v1_admin_proto_enumTypes[1].Descriptor()
}

func (ChaosAction) Type() protoreflect.EnumType {
	return &file_fivefamilies_v1_admin_proto_enumTypes[1]
}

func (x ChaosAction) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (ChaosAction) EnumDescriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{1}
}

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
	return file_fivefamilies_v1_admin_proto_enumTypes[2].Descriptor()
}

func (MemberChange) Type() protoreflect.EnumType {
	return &file_fivefamilies_v1_admin_proto_enumTypes[2]
}

func (x MemberChange) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (MemberChange) EnumDescriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{2}
}

type StatusRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Hash          bool                   `protobuf:"varint,1,opt,name=hash,proto3" json:"hash,omitempty"`
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

func (x *StatusRequest) GetHash() bool {
	if x != nil {
		return x.Hash
	}
	return false
}

type StatusResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	Id    uint64                 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`

	State          string   `protobuf:"bytes,2,opt,name=state,proto3" json:"state,omitempty"`
	Term           uint64   `protobuf:"varint,3,opt,name=term,proto3" json:"term,omitempty"`
	Leader         uint64   `protobuf:"varint,4,opt,name=leader,proto3" json:"leader,omitempty"`
	Commit         uint64   `protobuf:"varint,5,opt,name=commit,proto3" json:"commit,omitempty"`
	Applied        uint64   `protobuf:"varint,6,opt,name=applied,proto3" json:"applied,omitempty"`
	FirstIndex     uint64   `protobuf:"varint,7,opt,name=first_index,json=firstIndex,proto3" json:"first_index,omitempty"`
	LastIndex      uint64   `protobuf:"varint,8,opt,name=last_index,json=lastIndex,proto3" json:"last_index,omitempty"`
	SnapshotIndex  uint64   `protobuf:"varint,9,opt,name=snapshot_index,json=snapshotIndex,proto3" json:"snapshot_index,omitempty"`
	StateHash      uint64   `protobuf:"varint,10,opt,name=state_hash,json=stateHash,proto3" json:"state_hash,omitempty"`
	HashIndex      uint64   `protobuf:"varint,11,opt,name=hash_index,json=hashIndex,proto3" json:"hash_index,omitempty"`
	Voters         []uint64 `protobuf:"varint,12,rep,packed,name=voters,proto3" json:"voters,omitempty"`
	Learners       []uint64 `protobuf:"varint,13,rep,packed,name=learners,proto3" json:"learners,omitempty"`
	Transferee     uint64   `protobuf:"varint,14,opt,name=transferee,proto3" json:"transferee,omitempty"`
	Fsyncs         uint64   `protobuf:"varint,15,opt,name=fsyncs,proto3" json:"fsyncs,omitempty"`
	WalBatches     uint64   `protobuf:"varint,16,opt,name=wal_batches,json=walBatches,proto3" json:"wal_batches,omitempty"`
	EntriesWritten uint64   `protobuf:"varint,17,opt,name=entries_written,json=entriesWritten,proto3" json:"entries_written,omitempty"`
	MsgsSent       uint64   `protobuf:"varint,18,opt,name=msgs_sent,json=msgsSent,proto3" json:"msgs_sent,omitempty"`
	MsgsReceived   uint64   `protobuf:"varint,19,opt,name=msgs_received,json=msgsReceived,proto3" json:"msgs_received,omitempty"`
	Reads          uint64   `protobuf:"varint,20,opt,name=reads,proto3" json:"reads,omitempty"`
	Writes         uint64   `protobuf:"varint,21,opt,name=writes,proto3" json:"writes,omitempty"`
	Elections      uint64   `protobuf:"varint,22,opt,name=elections,proto3" json:"elections,omitempty"`
	Role           Role     `protobuf:"varint,23,opt,name=role,proto3,enum=fivefamilies.v1.Role" json:"role,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
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

func (x *StatusResponse) GetFsyncs() uint64 {
	if x != nil {
		return x.Fsyncs
	}
	return 0
}

func (x *StatusResponse) GetWalBatches() uint64 {
	if x != nil {
		return x.WalBatches
	}
	return 0
}

func (x *StatusResponse) GetEntriesWritten() uint64 {
	if x != nil {
		return x.EntriesWritten
	}
	return 0
}

func (x *StatusResponse) GetMsgsSent() uint64 {
	if x != nil {
		return x.MsgsSent
	}
	return 0
}

func (x *StatusResponse) GetMsgsReceived() uint64 {
	if x != nil {
		return x.MsgsReceived
	}
	return 0
}

func (x *StatusResponse) GetReads() uint64 {
	if x != nil {
		return x.Reads
	}
	return 0
}

func (x *StatusResponse) GetWrites() uint64 {
	if x != nil {
		return x.Writes
	}
	return 0
}

func (x *StatusResponse) GetElections() uint64 {
	if x != nil {
		return x.Elections
	}
	return 0
}

func (x *StatusResponse) GetRole() Role {
	if x != nil {
		return x.Role
	}
	return Role_ROLE_UNSPECIFIED
}

type ChaosRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	LegacyAction  string      `protobuf:"bytes,1,opt,name=legacy_action,json=legacyAction,proto3" json:"legacy_action,omitempty"`
	DurationMs    uint64      `protobuf:"varint,2,opt,name=duration_ms,json=durationMs,proto3" json:"duration_ms,omitempty"`
	Action        ChaosAction `protobuf:"varint,3,opt,name=action,proto3,enum=fivefamilies.v1.ChaosAction" json:"action,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ChaosRequest) Reset() {
	*x = ChaosRequest{}
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ChaosRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChaosRequest) ProtoMessage() {}

func (x *ChaosRequest) ProtoReflect() protoreflect.Message {
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

func (*ChaosRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{2}
}

func (x *ChaosRequest) GetLegacyAction() string {
	if x != nil {
		return x.LegacyAction
	}
	return ""
}

func (x *ChaosRequest) GetDurationMs() uint64 {
	if x != nil {
		return x.DurationMs
	}
	return 0
}

func (x *ChaosRequest) GetAction() ChaosAction {
	if x != nil {
		return x.Action
	}
	return ChaosAction_CHAOS_ACTION_UNSPECIFIED
}

type ChaosResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ChaosResponse) Reset() {
	*x = ChaosResponse{}
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ChaosResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChaosResponse) ProtoMessage() {}

func (x *ChaosResponse) ProtoReflect() protoreflect.Message {
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

func (*ChaosResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{3}
}

type BackupRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *BackupRequest) Reset() {
	*x = BackupRequest{}
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BackupRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BackupRequest) ProtoMessage() {}

func (x *BackupRequest) ProtoReflect() protoreflect.Message {
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

func (*BackupRequest) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{4}
}

type BackupResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Rev           uint64                 `protobuf:"varint,1,opt,name=rev,proto3" json:"rev,omitempty"`
	Hash          uint64                 `protobuf:"varint,2,opt,name=hash,proto3" json:"hash,omitempty"`
	Size          uint64                 `protobuf:"varint,3,opt,name=size,proto3" json:"size,omitempty"`
	Data          []byte                 `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *BackupResponse) Reset() {
	*x = BackupResponse{}
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BackupResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BackupResponse) ProtoMessage() {}

func (x *BackupResponse) ProtoReflect() protoreflect.Message {
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

func (*BackupResponse) Descriptor() ([]byte, []int) {
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{5}
}

func (x *BackupResponse) GetRev() uint64 {
	if x != nil {
		return x.Rev
	}
	return 0
}

func (x *BackupResponse) GetHash() uint64 {
	if x != nil {
		return x.Hash
	}
	return 0
}

func (x *BackupResponse) GetSize() uint64 {
	if x != nil {
		return x.Size
	}
	return 0
}

func (x *BackupResponse) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
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
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ChangeMembersRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChangeMembersRequest) ProtoMessage() {}

func (x *ChangeMembersRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[6]
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
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{6}
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
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ChangeMembersResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChangeMembersResponse) ProtoMessage() {}

func (x *ChangeMembersResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[7]
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
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{7}
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
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TransferLeadershipRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TransferLeadershipRequest) ProtoMessage() {}

func (x *TransferLeadershipRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[8]
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
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{8}
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
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TransferLeadershipResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TransferLeadershipResponse) ProtoMessage() {}

func (x *TransferLeadershipResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fivefamilies_v1_admin_proto_msgTypes[9]
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
	return file_fivefamilies_v1_admin_proto_rawDescGZIP(), []int{9}
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
	"\x1bfivefamilies/v1/admin.proto\x12\x0ffivefamilies.v1\"#\n" +
	"\rStatusRequest\x12\x12\n" +
	"\x04hash\x18\x01 \x01(\bR\x04hash\"\xac\x05\n" +
	"\x0eStatusResponse\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x04R\x02id\x12\x18\n" +
	"\x05state\x18\x02 \x01(\tB\x02\x18\x01R\x05state\x12\x12\n" +
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
	"transferee\x12\x16\n" +
	"\x06fsyncs\x18\x0f \x01(\x04R\x06fsyncs\x12\x1f\n" +
	"\vwal_batches\x18\x10 \x01(\x04R\n" +
	"walBatches\x12'\n" +
	"\x0fentries_written\x18\x11 \x01(\x04R\x0eentriesWritten\x12\x1b\n" +
	"\tmsgs_sent\x18\x12 \x01(\x04R\bmsgsSent\x12#\n" +
	"\rmsgs_received\x18\x13 \x01(\x04R\fmsgsReceived\x12\x14\n" +
	"\x05reads\x18\x14 \x01(\x04R\x05reads\x12\x16\n" +
	"\x06writes\x18\x15 \x01(\x04R\x06writes\x12\x1c\n" +
	"\telections\x18\x16 \x01(\x04R\telections\x12)\n" +
	"\x04role\x18\x17 \x01(\x0e2\x15.fivefamilies.v1.RoleR\x04role\"\x8e\x01\n" +
	"\fChaosRequest\x12'\n" +
	"\rlegacy_action\x18\x01 \x01(\tB\x02\x18\x01R\flegacyAction\x12\x1f\n" +
	"\vduration_ms\x18\x02 \x01(\x04R\n" +
	"durationMs\x124\n" +
	"\x06action\x18\x03 \x01(\x0e2\x1c.fivefamilies.v1.ChaosActionR\x06action\"\x0f\n" +
	"\rChaosResponse\"\x0f\n" +
	"\rBackupRequest\"^\n" +
	"\x0eBackupResponse\x12\x10\n" +
	"\x03rev\x18\x01 \x01(\x04R\x03rev\x12\x12\n" +
	"\x04hash\x18\x02 \x01(\x04R\x04hash\x12\x12\n" +
	"\x04size\x18\x03 \x01(\x04R\x04size\x12\x12\n" +
	"\x04data\x18\x04 \x01(\fR\x04data\"a\n" +
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
	"\x04term\x18\x02 \x01(\x04R\x04term*l\n" +
	"\x04Role\x12\x14\n" +
	"\x10ROLE_UNSPECIFIED\x10\x00\x12\x11\n" +
	"\rROLE_FOLLOWER\x10\x01\x12\x16\n" +
	"\x12ROLE_PRE_CANDIDATE\x10\x02\x12\x12\n" +
	"\x0eROLE_CANDIDATE\x10\x03\x12\x0f\n" +
	"\vROLE_LEADER\x10\x04*\x91\x01\n" +
	"\vChaosAction\x12\x1c\n" +
	"\x18CHAOS_ACTION_UNSPECIFIED\x10\x00\x12\x1b\n" +
	"\x17CHAOS_ACTION_STALL_DISK\x10\x01\x12\x16\n" +
	"\x12CHAOS_ACTION_PAUSE\x10\x02\x12\x18\n" +
	"\x14CHAOS_ACTION_ISOLATE\x10\x03\x12\x15\n" +
	"\x11CHAOS_ACTION_HEAL\x10\x04*\x81\x01\n" +
	"\fMemberChange\x12\x1d\n" +
	"\x19MEMBER_CHANGE_UNSPECIFIED\x10\x00\x12\x1d\n" +
	"\x19MEMBER_CHANGE_ADD_LEARNER\x10\x01\x12\x19\n" +
	"\x15MEMBER_CHANGE_PROMOTE\x10\x02\x12\x18\n" +
	"\x14MEMBER_CHANGE_REMOVE\x10\x032\xb6\x03\n" +
	"\x05Admin\x12I\n" +
	"\x06Status\x12\x1e.fivefamilies.v1.StatusRequest\x1a\x1f.fivefamilies.v1.StatusResponse\x12^\n" +
	"\rChangeMembers\x12%.fivefamilies.v1.ChangeMembersRequest\x1a&.fivefamilies.v1.ChangeMembersResponse\x12m\n" +
	"\x12TransferLeadership\x12*.fivefamilies.v1.TransferLeadershipRequest\x1a+.fivefamilies.v1.TransferLeadershipResponse\x12F\n" +
	"\x05Chaos\x12\x1d.fivefamilies.v1.ChaosRequest\x1a\x1e.fivefamilies.v1.ChaosResponse\x12K\n" +
	"\x06Backup\x12\x1e.fivefamilies.v1.BackupRequest\x1a\x1f.fivefamilies.v1.BackupResponse0\x01BHZFgithub.com/mirzanajafov/fivefamilies/api/fivefamiliesv1;fivefamiliesv1b\x06proto3"

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

var file_fivefamilies_v1_admin_proto_enumTypes = make([]protoimpl.EnumInfo, 3)
var file_fivefamilies_v1_admin_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
var file_fivefamilies_v1_admin_proto_goTypes = []any{
	(Role)(0),
	(ChaosAction)(0),
	(MemberChange)(0),
	(*StatusRequest)(nil),
	(*StatusResponse)(nil),
	(*ChaosRequest)(nil),
	(*ChaosResponse)(nil),
	(*BackupRequest)(nil),
	(*BackupResponse)(nil),
	(*ChangeMembersRequest)(nil),
	(*ChangeMembersResponse)(nil),
	(*TransferLeadershipRequest)(nil),
	(*TransferLeadershipResponse)(nil),
}
var file_fivefamilies_v1_admin_proto_depIdxs = []int32{
	0,
	1,
	2,
	3,
	9,
	11,
	5,
	7,
	4,
	10,
	12,
	6,
	8,
	8,
	3,
	3,
	3,
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
			NumEnums:      3,
			NumMessages:   10,
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
