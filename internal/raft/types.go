package raft

import "fmt"

type NodeID uint64

const None NodeID = 0

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryConfig
)

func (t EntryType) Valid() bool {
	return t == EntryNormal || t == EntryConfig
}

type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
	Conf  ConfState
}

type HardState struct {
	Term   uint64
	Vote   NodeID
	Commit uint64
}

func (h HardState) IsEmpty() bool {
	return h == HardState{}
}

type MessageType uint8

const (
	MsgApp MessageType = iota + 1
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgPreVote
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgStorageAck
	MsgSnap
	MsgReadIndex
	MsgReadIndexResp
	MsgTimeoutNow
)

var messageTypeNames = [...]string{
	MsgApp:           "App",
	MsgAppResp:       "AppResp",
	MsgHeartbeat:     "Heartbeat",
	MsgHeartbeatResp: "HeartbeatResp",
	MsgPreVote:       "PreVote",
	MsgPreVoteResp:   "PreVoteResp",
	MsgVote:          "Vote",
	MsgVoteResp:      "VoteResp",
	MsgStorageAck:    "StorageAck",
	MsgSnap:          "Snap",
	MsgReadIndex:     "ReadIndex",
	MsgReadIndexResp: "ReadIndexResp",
	MsgTimeoutNow:    "TimeoutNow",
}

func (t MessageType) known() bool {
	return int(t) < len(messageTypeNames) && messageTypeNames[t] != ""
}

func (t MessageType) String() string {
	if t.known() {
		return messageTypeNames[t]
	}
	return fmt.Sprintf("MessageType(%d)", uint8(t))
}

func (t MessageType) Remote() bool {
	return t.known() && t != MsgStorageAck
}

type Message struct {
	Type     MessageType
	From     NodeID
	To       NodeID
	Term     uint64
	LogTerm  uint64
	Index    uint64
	Entries  []Entry
	Commit   uint64
	Reject   bool
	Hint     uint64
	Context  uint64
	Snapshot *Snapshot
}

func (m Message) String() string {
	s := fmt.Sprintf("%s %d->%d t%d idx%d lt%d c%d", m.Type, m.From, m.To, m.Term, m.Index, m.LogTerm, m.Commit)
	if len(m.Entries) > 0 {
		s += fmt.Sprintf(" ents[%d..%d]", m.Entries[0].Index, m.Entries[len(m.Entries)-1].Index)
	}
	if m.Reject {
		s += fmt.Sprintf(" reject hint%d", m.Hint)
	}
	if m.Snapshot != nil {
		s += fmt.Sprintf(" snap[%d t%d %dB]", m.Snapshot.Index, m.Snapshot.Term, len(m.Snapshot.Data))
	}
	return s
}

type StateType uint8

const (
	StateFollower StateType = iota
	StatePreCandidate
	StateCandidate
	StateLeader
)

func (s StateType) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StatePreCandidate:
		return "precandidate"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	}
	return fmt.Sprintf("StateType(%d)", uint8(s))
}

type Ready struct {
	HardState    HardState
	Snapshot     *Snapshot
	Entries      []Entry
	MustSync     bool
	Messages     []Message
	AfterPersist []Message
	Committed    []Entry
	ReadStates   []ReadState
}

func (rd Ready) NeedsWrite() bool {
	return !rd.HardState.IsEmpty() || len(rd.Entries) > 0 || rd.Snapshot != nil
}
