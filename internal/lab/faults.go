package lab

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mirzanajafov/fivefamilies/internal/rig"
)

type FaultKind string

const (
	FaultKill       FaultKind = "kill"
	FaultKillLeader FaultKind = "kill-leader"
	FaultPause      FaultKind = "pause"
	FaultIsolate    FaultKind = "isolate"
	FaultSplit      FaultKind = "split"
	FaultBridge     FaultKind = "bridge"
	FaultOneWay     FaultKind = "one-way"
	FaultNetem      FaultKind = "netem"
	FaultMember     FaultKind = "member"
	FaultTransfer   FaultKind = "transfer"
)

var AllFaults = []FaultKind{FaultKill, FaultKillLeader, FaultPause, FaultIsolate, FaultSplit, FaultBridge, FaultOneWay, FaultNetem, FaultMember, FaultTransfer}

func ParseFaults(s string) ([]FaultKind, error) {
	switch strings.TrimSpace(s) {
	case "all":
		return AllFaults, nil
	case "none":
		return nil, nil
	}
	var kinds []FaultKind
	for name := range strings.SplitSeq(s, ",") {
		k := FaultKind(strings.TrimSpace(name))
		if !slices.Contains(AllFaults, k) {
			return nil, fmt.Errorf("lab: unknown fault %q", k)
		}
		kinds = append(kinds, k)
	}
	return kinds, nil
}

type FaultRecord struct {
	Kind     FaultKind     `json:"kind"`
	Detail   string        `json:"detail"`
	Start    time.Duration `json:"start_ns"`
	End      time.Duration `json:"end_ns"`
	Err      string        `json:"error,omitempty"`
	leader   int
	duration time.Duration
}

type nemesis struct {
	d      *Docker
	adm    *rig.Admin
	rng    *rand.Rand
	kinds  []FaultKind
	mean   time.Duration
	leader func() int
	start  time.Time
	log    []FaultRecord
}

func (n *nemesis) since() time.Duration {
	return time.Since(n.start)
}

func (n *nemesis) between(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(n.rng.Int64N(int64(hi-lo)))
}

func (n *nemesis) run(ctx context.Context) {
	for {
		gap := time.Duration(-float64(n.mean) * math.Log(1-n.rng.Float64()))
		if !rig.Wait(ctx, gap) {
			return
		}
		kind := n.kinds[n.rng.IntN(len(n.kinds))]
		rec := n.inject(ctx, kind)
		if rec.Err == "" {
			rig.Wait(ctx, rec.duration)
		}
		if err := n.clear(context.Background(), &rec); err != nil && rec.Err == "" {
			rec.Err = err.Error()
		}
		rec.End = n.since()
		n.log = append(n.log, rec)
	}
}

func (n *nemesis) nodes() []int {
	out := make([]int, n.d.Nodes)
	for i := range out {
		out[i] = i + 1
	}
	n.rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func (n *nemesis) inject(ctx context.Context, kind FaultKind) FaultRecord {
	rec := FaultRecord{Kind: kind, Start: n.since()}
	order := n.nodes()
	var err error
	switch kind {
	case FaultKill, FaultKillLeader:
		target := order[0]
		if kind == FaultKillLeader {
			if l := n.leader(); l > 0 {
				target = l
			}
		}
		rec.leader, rec.duration = target, n.between(500*time.Millisecond, 5*time.Second)
		rec.Detail = fmt.Sprintf("SIGKILL node %d", target)
		err = n.d.Kill(ctx, target)
	case FaultPause:
		rec.leader, rec.duration = order[0], n.between(200*time.Millisecond, 4*time.Second)
		rec.Detail = fmt.Sprintf("pause node %d", order[0])
		err = n.d.Pause(ctx, order[0])
	case FaultIsolate:
		rec.duration = n.between(time.Second, 6*time.Second)
		rec.Detail = fmt.Sprintf("isolate node %d", order[0])
		err = n.d.Block(ctx, order[:1], order[1:])
	case FaultSplit:
		rec.duration = n.between(time.Second, 6*time.Second)
		k := 1 + n.rng.IntN((n.d.Nodes-1)/2)
		a, b := slices.Clone(order[:k]), slices.Clone(order[k:])
		rec.Detail = fmt.Sprintf("split %v | %v", a, b)
		err = n.d.Block(ctx, a, b)
	case FaultBridge:
		rec.duration = n.between(time.Second, 6*time.Second)
		rest := order[1:]
		k := 1 + n.rng.IntN(len(rest)-1)
		a, b := slices.Clone(rest[:k]), slices.Clone(rest[k:])
		rec.Detail = fmt.Sprintf("bridge %d between %v and %v", order[0], a, b)
		err = n.d.Block(ctx, a, b)
	case FaultOneWay:
		rec.duration = n.between(time.Second, 6*time.Second)
		rec.Detail = fmt.Sprintf("one-way %d -> %d", order[0], order[1])
		err = n.d.BlockOneWay(ctx, order[0], order[1])
	case FaultNetem:
		rec.duration = n.between(time.Second, 6*time.Second)
		spec := fmt.Sprintf("delay %dms %dms loss %d%%", 5+n.rng.IntN(60), 1+n.rng.IntN(20), n.rng.IntN(10))
		rec.Detail = fmt.Sprintf("netem %s on node %d", spec, order[0])
		err = n.d.Netem(ctx, order[0], spec)
	case FaultMember, FaultTransfer:
		l := n.leader()
		if l == 0 {
			err = fmt.Errorf("no leader to ask")
			break
		}
		target := order[0]
		if target == l {
			target = order[1]
		}
		rec.leader = target
		if kind == FaultTransfer {
			rec.duration = n.between(500*time.Millisecond, 2*time.Second)
			rec.Detail = fmt.Sprintf("hand leadership from node %d to %d", l, target)
			var lead int
			switch lead, err = transfer(ctx, n.adm, l, target); {
			case err == nil:
				rec.Detail += fmt.Sprintf(" (node %d leads after it)", lead)
			case status.Code(err) == codes.FailedPrecondition:
				rec.Detail += fmt.Sprintf(" (%s)", status.Convert(err).Message())
				err = nil
			}
			break
		}
		rec.duration = n.between(time.Second, 6*time.Second)
		rec.Detail = fmt.Sprintf("remove node %d, then add it back as a learner and promote it", target)
		err = removeVoter(ctx, n.adm, target)
	}
	if err != nil {
		rec.Err = err.Error()
	}
	return rec
}

func (n *nemesis) clear(ctx context.Context, rec *FaultRecord) error {
	switch rec.Kind {
	case FaultKill, FaultKillLeader:
		return n.d.Start(ctx, rec.leader)
	case FaultPause:
		return n.d.Unpause(ctx, rec.leader)
	case FaultMember:
		if rec.leader == 0 {
			return nil
		}
		return restoreVoter(ctx, n.adm, rec.leader)
	case FaultTransfer:
		return nil
	}
	return n.d.Heal(ctx)
}
