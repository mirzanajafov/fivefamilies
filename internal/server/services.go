package server

import (
	"context"
	"errors"
	"strconv"

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
	var nl *node.NotLeaderError
	switch {
	case errors.As(err, &nl):
		if nl.Leader != raft.None {
			grpc.SetHeader(ctx, metadata.Pairs(HeaderLeader, strconv.FormatUint(uint64(nl.Leader), 10), HeaderLeaderAddr, k.s.cfg.Nodes[nl.Leader]))
		}
		return status.Error(codes.Unavailable, "not the leader")
	case errors.Is(err, node.ErrLost), errors.Is(err, node.ErrNotReady), errors.Is(err, raft.ErrNotLeader):
		return status.Error(codes.Unavailable, err.Error())
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
	}
	return status.Error(codes.InvalidArgument, res.Code.String())
}

func (k *kvService) submit(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	res, err := k.s.Submit(ctx, cmd)
	if err != nil {
		return res, k.fail(ctx, err)
	}
	return res, outcome(res)
}

func toKV(kvp kv.KeyValue) *pb.KeyValue {
	return &pb.KeyValue{Key: kvp.Key, Value: kvp.Value, CreateRev: kvp.CreateRev, ModRev: kvp.ModRev, Version: kvp.Version}
}

func (k *kvService) Register(ctx context.Context, _ *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	res, err := k.submit(ctx, kv.Command{Op: kv.OpRegister})
	if err != nil {
		return nil, err
	}
	return &pb.RegisterResponse{Client: res.Client}, nil
}

func (k *kvService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
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
	res, err := k.submit(ctx, kv.Command{Op: kv.OpPut, Client: req.GetClient(), Seq: req.GetSeq(), Key: req.GetKey(), Value: req.GetValue()})
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
	cmd := kv.Command{Op: kv.OpCAS, Client: req.GetClient(), Seq: req.GetSeq(), Key: req.GetKey(), Value: req.GetValue()}
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
