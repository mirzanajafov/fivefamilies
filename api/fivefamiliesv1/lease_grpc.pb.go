package fivefamiliesv1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const _ = grpc.SupportPackageIsVersion9

const (
	Lease_Grant_FullMethodName      = "/fivefamilies.v1.Lease/Grant"
	Lease_Revoke_FullMethodName     = "/fivefamilies.v1.Lease/Revoke"
	Lease_KeepAlive_FullMethodName  = "/fivefamilies.v1.Lease/KeepAlive"
	Lease_TimeToLive_FullMethodName = "/fivefamilies.v1.Lease/TimeToLive"
)

type LeaseClient interface {
	Grant(ctx context.Context, in *LeaseGrantRequest, opts ...grpc.CallOption) (*LeaseGrantResponse, error)
	Revoke(ctx context.Context, in *LeaseRevokeRequest, opts ...grpc.CallOption) (*LeaseRevokeResponse, error)
	KeepAlive(ctx context.Context, in *LeaseKeepAliveRequest, opts ...grpc.CallOption) (*LeaseKeepAliveResponse, error)
	TimeToLive(ctx context.Context, in *LeaseTimeToLiveRequest, opts ...grpc.CallOption) (*LeaseTimeToLiveResponse, error)
}

type leaseClient struct {
	cc grpc.ClientConnInterface
}

func NewLeaseClient(cc grpc.ClientConnInterface) LeaseClient {
	return &leaseClient{cc}
}

func (c *leaseClient) Grant(ctx context.Context, in *LeaseGrantRequest, opts ...grpc.CallOption) (*LeaseGrantResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LeaseGrantResponse)
	err := c.cc.Invoke(ctx, Lease_Grant_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *leaseClient) Revoke(ctx context.Context, in *LeaseRevokeRequest, opts ...grpc.CallOption) (*LeaseRevokeResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LeaseRevokeResponse)
	err := c.cc.Invoke(ctx, Lease_Revoke_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *leaseClient) KeepAlive(ctx context.Context, in *LeaseKeepAliveRequest, opts ...grpc.CallOption) (*LeaseKeepAliveResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LeaseKeepAliveResponse)
	err := c.cc.Invoke(ctx, Lease_KeepAlive_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *leaseClient) TimeToLive(ctx context.Context, in *LeaseTimeToLiveRequest, opts ...grpc.CallOption) (*LeaseTimeToLiveResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LeaseTimeToLiveResponse)
	err := c.cc.Invoke(ctx, Lease_TimeToLive_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type LeaseServer interface {
	Grant(context.Context, *LeaseGrantRequest) (*LeaseGrantResponse, error)
	Revoke(context.Context, *LeaseRevokeRequest) (*LeaseRevokeResponse, error)
	KeepAlive(context.Context, *LeaseKeepAliveRequest) (*LeaseKeepAliveResponse, error)
	TimeToLive(context.Context, *LeaseTimeToLiveRequest) (*LeaseTimeToLiveResponse, error)
	mustEmbedUnimplementedLeaseServer()
}

type UnimplementedLeaseServer struct{}

func (UnimplementedLeaseServer) Grant(context.Context, *LeaseGrantRequest) (*LeaseGrantResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Grant not implemented")
}
func (UnimplementedLeaseServer) Revoke(context.Context, *LeaseRevokeRequest) (*LeaseRevokeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Revoke not implemented")
}
func (UnimplementedLeaseServer) KeepAlive(context.Context, *LeaseKeepAliveRequest) (*LeaseKeepAliveResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method KeepAlive not implemented")
}
func (UnimplementedLeaseServer) TimeToLive(context.Context, *LeaseTimeToLiveRequest) (*LeaseTimeToLiveResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method TimeToLive not implemented")
}
func (UnimplementedLeaseServer) mustEmbedUnimplementedLeaseServer() {}
func (UnimplementedLeaseServer) testEmbeddedByValue()               {}

type UnsafeLeaseServer interface {
	mustEmbedUnimplementedLeaseServer()
}

func RegisterLeaseServer(s grpc.ServiceRegistrar, srv LeaseServer) {

	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&Lease_ServiceDesc, srv)
}

func _Lease_Grant_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaseGrantRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LeaseServer).Grant(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Lease_Grant_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LeaseServer).Grant(ctx, req.(*LeaseGrantRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lease_Revoke_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaseRevokeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LeaseServer).Revoke(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Lease_Revoke_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LeaseServer).Revoke(ctx, req.(*LeaseRevokeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lease_KeepAlive_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaseKeepAliveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LeaseServer).KeepAlive(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Lease_KeepAlive_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LeaseServer).KeepAlive(ctx, req.(*LeaseKeepAliveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lease_TimeToLive_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaseTimeToLiveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LeaseServer).TimeToLive(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Lease_TimeToLive_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LeaseServer).TimeToLive(ctx, req.(*LeaseTimeToLiveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var Lease_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "fivefamilies.v1.Lease",
	HandlerType: (*LeaseServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Grant",
			Handler:    _Lease_Grant_Handler,
		},
		{
			MethodName: "Revoke",
			Handler:    _Lease_Revoke_Handler,
		},
		{
			MethodName: "KeepAlive",
			Handler:    _Lease_KeepAlive_Handler,
		},
		{
			MethodName: "TimeToLive",
			Handler:    _Lease_TimeToLive_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "fivefamilies/v1/lease.proto",
}
