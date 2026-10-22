package loadgen

type FailoverSample struct {
	Kind      string `json:"kind,omitempty"`
	Leader    int    `json:"leader,omitempty"`
	Term      uint64 `json:"term,omitempty"`
	Scheduled int64  `json:"scheduled_ns"`
	Done      int64  `json:"done_ns"`
	Node      int    `json:"node"`
	Ok        bool   `json:"ok"`
	Err       string `json:"error,omitempty"`
}
