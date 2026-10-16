package fivefamiliesv1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const _ = grpc.SupportPackageIsVersion9

const (
	Admin_Status_FullMethodName             = "/fivefamilies.v1.Admin/Status"
	Admin_ChangeMembers_FullMethodName      = "/fivefamilies.v1.Admin/ChangeMembers"
	Admin_TransferLeadership_FullMethodName = "/fivefamilies.v1.Admin/TransferLeadership"
)

type AdminClient interface {
	Status(ctx context.Context, in *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error)
	ChangeMembers(ctx context.Context, in *ChangeMembersRequest, opts ...grpc.CallOption) (*ChangeMembersResponse, error)
	TransferLeadership(ctx context.Context, in *TransferLeadershipRequest, opts ...grpc.CallOption) (*TransferLeadershipResponse, error)
}

type adminClient struct {
	cc grpc.ClientConnInterface
}

func NewAdminClient(cc grpc.ClientConnInterface) AdminClient {
	return &adminClient{cc}
}

func (c *adminClient) Status(ctx context.Context, in *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(StatusResponse)
	err := c.cc.Invoke(ctx, Admin_Status_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminClient) ChangeMembers(ctx context.Context, in *ChangeMembersRequest, opts ...grpc.CallOption) (*ChangeMembersResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ChangeMembersResponse)
	err := c.cc.Invoke(ctx, Admin_ChangeMembers_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminClient) TransferLeadership(ctx context.Context, in *TransferLeadershipRequest, opts ...grpc.CallOption) (*TransferLeadershipResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(TransferLeadershipResponse)
	err := c.cc.Invoke(ctx, Admin_TransferLeadership_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type AdminServer interface {
	Status(context.Context, *StatusRequest) (*StatusResponse, error)
	ChangeMembers(context.Context, *ChangeMembersRequest) (*ChangeMembersResponse, error)
	TransferLeadership(context.Context, *TransferLeadershipRequest) (*TransferLeadershipResponse, error)
	mustEmbedUnimplementedAdminServer()
}

type UnimplementedAdminServer struct{}

func (UnimplementedAdminServer) Status(context.Context, *StatusRequest) (*StatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Status not implemented")
}
func (UnimplementedAdminServer) ChangeMembers(context.Context, *ChangeMembersRequest) (*ChangeMembersResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ChangeMembers not implemented")
}
func (UnimplementedAdminServer) TransferLeadership(context.Context, *TransferLeadershipRequest) (*TransferLeadershipResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method TransferLeadership not implemented")
}
func (UnimplementedAdminServer) mustEmbedUnimplementedAdminServer() {}
func (UnimplementedAdminServer) testEmbeddedByValue()               {}

type UnsafeAdminServer interface {
	mustEmbedUnimplementedAdminServer()
}

func RegisterAdminServer(s grpc.ServiceRegistrar, srv AdminServer) {

	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&Admin_ServiceDesc, srv)
}

func _Admin_Status_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServer).Status(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Admin_Status_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServer).Status(ctx, req.(*StatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Admin_ChangeMembers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ChangeMembersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServer).ChangeMembers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Admin_ChangeMembers_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServer).ChangeMembers(ctx, req.(*ChangeMembersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Admin_TransferLeadership_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TransferLeadershipRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServer).TransferLeadership(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Admin_TransferLeadership_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServer).TransferLeadership(ctx, req.(*TransferLeadershipRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var Admin_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "fivefamilies.v1.Admin",
	HandlerType: (*AdminServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Status",
			Handler:    _Admin_Status_Handler,
		},
		{
			MethodName: "ChangeMembers",
			Handler:    _Admin_ChangeMembers_Handler,
		},
		{
			MethodName: "TransferLeadership",
			Handler:    _Admin_TransferLeadership_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "fivefamilies/v1/admin.proto",
}
