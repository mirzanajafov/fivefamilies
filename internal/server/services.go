package server

import (
	"context"
	"errors"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

const (
	HeaderLeader     = "ff-leader"
	HeaderLeaderAddr = "ff-leader-addr"
)

type kvService struct {
	pb.UnimplementedKVServer
	s *Server
}

func (k *kvService) fail(ctx context.Context, err error) error {
	return fail(ctx, k.s, err)
}

func fail(ctx context.Context, s *Server, err error) error {
	var nl *node.NotLeaderError
	switch {
	case errors.As(err, &nl):
		if nl.Leader != raft.None {
			grpc.SetHeader(ctx, metadata.Pairs(HeaderLeader, strconv.FormatUint(uint64(nl.Leader), 10), HeaderLeaderAddr, s.cfg.Nodes[nl.Leader]))
		}
		return status.Error(codes.Unavailable, "not the leader")
	case errors.Is(err, node.ErrLost), errors.Is(err, node.ErrNotReady), errors.Is(err, raft.ErrNotLeader),
		errors.Is(err, raft.ErrTransferring), errors.Is(err, raft.ErrConfNotReady):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, raft.ErrConfPending), errors.Is(err, raft.ErrLearnerBehind), errors.Is(err, ErrTransferAborted):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, raft.ErrBadConfChange):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Internal, err.Error())
}

func outcome(res kv.Result) error {
	switch res.Code {
	case kv.OK:
		return nil
	case kv.ErrSessionExpired:
		return status.Error(codes.FailedPrecondition, "session expired")
	case kv.ErrStaleSequence:
		return status.Error(codes.FailedPrecondition, "stale sequence number")
	case kv.ErrLeaseNotFound:
		return status.Error(codes.NotFound, "lease not found")
	case kv.ErrTooManyLeases:
		return status.Error(codes.ResourceExhausted, "too many leases")
	}
	return status.Error(codes.InvalidArgument, res.Code.String())
}

func (k *kvService) submit(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	if cmd.Op != kv.OpGet && cmd.Op != kv.OpRange {
		k.s.counts.writes.Add(1)
	}
	res, err := k.s.Submit(ctx, cmd)
	if err != nil {
		return res, k.fail(ctx, err)
	}
	return res, outcome(res)
}

func toKV(kvp kv.KeyValue) *pb.KeyValue {
	return &pb.KeyValue{Key: kvp.Key, Value: kvp.Value, CreateRev: kvp.CreateRev, ModRev: kvp.ModRev, Version: kvp.Version, Lease: kvp.Lease}
}

func (k *kvService) Register(ctx context.Context, _ *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	res, err := k.submit(ctx, kv.Command{Op: kv.OpRegister})
	if err != nil {
		return nil, err
	}
	return &pb.RegisterResponse{Client: res.Client}, nil
}

func (k *kvService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	k.s.counts.reads.Add(1)
	var res kv.Result
	var err error
	if req.GetMode() == pb.ReadMode_READ_MODE_LOG {
		res, err = k.submit(ctx, kv.Command{Op: kv.OpGet, Key: req.GetKey()})
	} else {
		res, err = k.s.Read(ctx, req.GetKey(), req.GetMode() == pb.ReadMode_READ_MODE_LEASE)
		if err != nil {
			err = k.fail(ctx, err)
		}
	}
	if err != nil {
		return nil, err
	}
	out := &pb.GetResponse{Found: res.Found, Rev: res.Rev}
	if res.Found {
		out.Kv = toKV(res.KV)
	}
	return out, nil
}

func (k *kvService) Range(ctx context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	res, err := k.submit(ctx, kv.Command{Op: kv.OpRange, Key: req.GetPrefix(), Limit: req.GetLimit()})
	if err != nil {
		return nil, err
	}
	out := &pb.RangeResponse{Rev: res.Rev}
	for _, kvp := range res.Range {
		out.Kvs = append(out.Kvs, toKV(kvp))
	}
	return out, nil
}

func (k *kvService) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	res, err := k.submit(ctx, kv.Command{Op: kv.OpPut, Client: req.GetClient(), Seq: req.GetSeq(), Key: req.GetKey(), Value: req.GetValue(), Lease: req.GetLease()})
	if err != nil {
		return nil, err
	}
	out := &pb.PutResponse{Rev: res.Rev, HadPrev: res.Found}
	if res.Found {
		out.Prev = toKV(res.KV)
	}
	return out, nil
}

func (k *kvService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	res, err := k.submit(ctx, kv.Command{Op: kv.OpDelete, Client: req.GetClient(), Seq: req.GetSeq(), Key: req.GetKey()})
	if err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{Rev: res.Rev, Deleted: res.Ok}, nil
}

func (k *kvService) CompareAndSwap(ctx context.Context, req *pb.CompareAndSwapRequest) (*pb.CompareAndSwapResponse, error) {
	cmd := kv.Command{Op: kv.OpCAS, Client: req.GetClient(), Seq: req.GetSeq(), Key: req.GetKey(), Value: req.GetValue(), Lease: req.GetLease()}
	switch e := req.GetExpect().(type) {
	case *pb.CompareAndSwapRequest_ValueIs:
		cmd.Expect = e.ValueIs
	case *pb.CompareAndSwapRequest_Missing:
		cmd.ExpectMissing = true
	case *pb.CompareAndSwapRequest_ModRev:
		cmd.Op, cmd.ExpectRev = kv.OpCASRev, e.ModRev
	default:
		return nil, status.Error(codes.InvalidArgument, "compare-and-swap needs an expectation")
	}
	res, err := k.submit(ctx, cmd)
	if err != nil {
		return nil, err
	}
	out := &pb.CompareAndSwapResponse{Rev: res.Rev, Swapped: res.Ok, Found: res.Found}
	if res.Found {
		out.Current = toKV(res.KV)
	}
	return out, nil
}

type adminService struct {
	pb.UnimplementedAdminServer
	s *Server
}

func (a *adminService) Status(ctx context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	st := *a.s.Status()
	hash, rev, err := a.s.StateHash(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}
	st.StateHash, st.HashIndex = hash, rev
	return &st, nil
}

func (a *adminService) ChangeMembers(ctx context.Context, req *pb.ChangeMembersRequest) (*pb.ChangeMembersResponse, error) {
	typ, ok := map[pb.MemberChange]raft.ConfChangeType{
		pb.MemberChange_MEMBER_CHANGE_ADD_LEARNER: raft.ConfAddLearner,
		pb.MemberChange_MEMBER_CHANGE_PROMOTE:     raft.ConfPromote,
		pb.MemberChange_MEMBER_CHANGE_REMOVE:      raft.ConfRemove,
	}[req.GetChange()]
	id := raft.NodeID(req.GetNode())
	switch {
	case !ok:
		return nil, status.Error(codes.InvalidArgument, "unknown membership change")
	case a.s.cfg.Nodes[id] == "":
		return nil, status.Errorf(codes.InvalidArgument, "node %d has no address in this node's directory", id)
	}
	index, conf, err := a.s.ChangeMembers(ctx, raft.ConfChange{Type: typ, Node: id})
	if err != nil {
		return nil, fail(ctx, a.s, err)
	}
	return &pb.ChangeMembersResponse{Index: index, Voters: ids64(conf.Voters), Learners: ids64(conf.Learners)}, nil
}

func (a *adminService) Backup(_ *pb.BackupRequest, stream pb.Admin_BackupServer) error {
	ctx := stream.Context()
	f, err := a.s.Backup(ctx.Done())
	if err != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	const chunk = 1 << 20
	for off := 0; off == 0 || off < len(f.Data); off += chunk {
		msg := &pb.BackupChunk{Data: f.Data[off:min(off+chunk, len(f.Data))]}
		if off == 0 {
			msg.Rev, msg.Hash, msg.Size = f.Rev, f.Hash, uint64(len(f.Data))
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	return nil
}

const maxChaos = time.Minute

func (a *adminService) Chaos(ctx context.Context, req *pb.ChaosRequest) (*pb.ChaosResponse, error) {
	if !a.s.cfg.Chaos {
		return nil, status.Error(codes.PermissionDenied, "chaos is off on this node")
	}
	if req.GetAction() == "heal" {
		a.s.Filter.Clear()
		a.s.stall.Store(0)
		return &pb.ChaosResponse{}, nil
	}
	d := time.Duration(req.GetDurationMs()) * time.Millisecond
	if d <= 0 || d > maxChaos {
		return nil, status.Errorf(codes.InvalidArgument, "chaos lasts between 1 ms and %v", maxChaos)
	}
	switch req.GetAction() {
	case "stall-disk":
		a.s.stall.Store(time.Now().Add(d).UnixNano())
	case "pause":
		select {
		case a.s.calls <- func() { time.Sleep(d) }:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	case "isolate":
		a.s.isolate(d)
	case "crash":
		go a.s.cfg.Crash()
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown chaos action %q", req.GetAction())
	}
	return &pb.ChaosResponse{}, nil
}

func (a *adminService) TransferLeadership(ctx context.Context, req *pb.TransferLeadershipRequest) (*pb.TransferLeadershipResponse, error) {
	if err := a.s.TransferLeadership(ctx, raft.NodeID(req.GetTo())); err != nil {
		return nil, fail(ctx, a.s, err)
	}
	st := a.s.Status()
	return &pb.TransferLeadershipResponse{Leader: st.GetLeader(), Term: st.GetTerm()}, nil
}

type leaseService struct {
	pb.UnimplementedLeaseServer
	s *Server
}

const maxLeaseTTL = 24 * time.Hour

func (l *leaseService) Grant(ctx context.Context, req *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	ttl := time.Duration(req.GetTtlMs()) * time.Millisecond
	if ttl <= 0 || ttl > maxLeaseTTL {
		return nil, status.Errorf(codes.InvalidArgument, "a lease needs a TTL between 1 ms and %v", maxLeaseTTL)
	}
	ttl = max(ttl, l.s.MinLeaseTTL())
	res, err := l.s.Submit(ctx, kv.Command{Op: kv.OpLeaseGrant, Client: req.GetClient(), Seq: req.GetSeq(), TTL: uint64(ttl / time.Millisecond)})
	if err != nil {
		return nil, fail(ctx, l.s, err)
	}
	if err := outcome(res); err != nil {
		return nil, err
	}
	return &pb.LeaseGrantResponse{Id: res.Lease, TtlMs: res.TTL, Rev: res.Rev}, nil
}

func (l *leaseService) Revoke(ctx context.Context, req *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	res, err := l.s.Submit(ctx, kv.Command{Op: kv.OpLeaseRevoke, Client: req.GetClient(), Seq: req.GetSeq(), Lease: req.GetId()})
	if err != nil {
		return nil, fail(ctx, l.s, err)
	}
	if err := outcome(res); err != nil {
		return nil, err
	}
	return &pb.LeaseRevokeResponse{Rev: res.Rev}, nil
}

func (l *leaseService) KeepAlive(ctx context.Context, req *pb.LeaseKeepAliveRequest) (*pb.LeaseKeepAliveResponse, error) {
	res, err := l.s.KeepAlive(ctx, req.GetId())
	if err != nil {
		return nil, fail(ctx, l.s, err)
	}
	if err := outcome(res); err != nil {
		return nil, err
	}
	return &pb.LeaseKeepAliveResponse{Id: res.Lease, TtlMs: res.TTL}, nil
}

func (l *leaseService) TimeToLive(ctx context.Context, req *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	remaining, info, err := l.s.TimeToLive(ctx, req.GetId())
	if errors.Is(err, errLeaseNotFound) {
		return nil, status.Error(codes.NotFound, "lease not found")
	}
	if err != nil {
		return nil, fail(ctx, l.s, err)
	}
	return &pb.LeaseTimeToLiveResponse{Id: info.ID, TtlMs: info.TTL, RemainingMs: uint64(remaining / time.Millisecond), Keys: uint64(info.Keys)}, nil
}
