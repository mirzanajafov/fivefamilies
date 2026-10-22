package loadgen

type SampleKind string

const (
	SampleWrite       SampleKind = ""
	SampleUnreachable SampleKind = "unreachable"
	SampleReachable   SampleKind = "reachable"
	SampleLeader      SampleKind = "leader"
)

type FailoverSample struct {
	Kind      SampleKind `json:"kind,omitempty"`
	Leader    int        `json:"leader,omitempty"`
	Term      uint64     `json:"term,omitempty"`
	Scheduled int64      `json:"scheduled_ns"`
	Done      int64      `json:"done_ns"`
	Node      int        `json:"node"`
	Ok        bool       `json:"ok"`
	Err       string     `json:"error,omitempty"`
}
