package fivefamiliesv1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const _ = grpc.SupportPackageIsVersion9

const (
	KV_OpenSession_FullMethodName    = "/fivefamilies.v1.KV/OpenSession"
	KV_Register_FullMethodName       = "/fivefamilies.v1.KV/Register"
	KV_Get_FullMethodName            = "/fivefamilies.v1.KV/Get"
	KV_Range_FullMethodName          = "/fivefamilies.v1.KV/Range"
	KV_Put_FullMethodName            = "/fivefamilies.v1.KV/Put"
	KV_Delete_FullMethodName         = "/fivefamilies.v1.KV/Delete"
	KV_CompareAndSwap_FullMethodName = "/fivefamilies.v1.KV/CompareAndSwap"
)

type KVClient interface {
	OpenSession(ctx context.Context, in *OpenSessionRequest, opts ...grpc.CallOption) (*OpenSessionResponse, error)

	Register(ctx context.Context, in *RegisterRequest, opts ...grpc.CallOption) (*RegisterResponse, error)
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Range(ctx context.Context, in *RangeRequest, opts ...grpc.CallOption) (*RangeResponse, error)
	Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error)
	Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
	CompareAndSwap(ctx context.Context, in *CompareAndSwapRequest, opts ...grpc.CallOption) (*CompareAndSwapResponse, error)
}

type kVClient struct {
	cc grpc.ClientConnInterface
}

func NewKVClient(cc grpc.ClientConnInterface) KVClient {
	return &kVClient{cc}
}

func (c *kVClient) OpenSession(ctx context.Context, in *OpenSessionRequest, opts ...grpc.CallOption) (*OpenSessionResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(OpenSessionResponse)
	err := c.cc.Invoke(ctx, KV_OpenSession_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) Register(ctx context.Context, in *RegisterRequest, opts ...grpc.CallOption) (*RegisterResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(RegisterResponse)
	err := c.cc.Invoke(ctx, KV_Register_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, KV_Get_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) Range(ctx context.Context, in *RangeRequest, opts ...grpc.CallOption) (*RangeResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(RangeResponse)
	err := c.cc.Invoke(ctx, KV_Range_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, KV_Put_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DeleteResponse)
	err := c.cc.Invoke(ctx, KV_Delete_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) CompareAndSwap(ctx context.Context, in *CompareAndSwapRequest, opts ...grpc.CallOption) (*CompareAndSwapResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CompareAndSwapResponse)
	err := c.cc.Invoke(ctx, KV_CompareAndSwap_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type KVServer interface {
	OpenSession(context.Context, *OpenSessionRequest) (*OpenSessionResponse, error)

	Register(context.Context, *RegisterRequest) (*RegisterResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Range(context.Context, *RangeRequest) (*RangeResponse, error)
	Put(context.Context, *PutRequest) (*PutResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	CompareAndSwap(context.Context, *CompareAndSwapRequest) (*CompareAndSwapResponse, error)
	mustEmbedUnimplementedKVServer()
}

type UnimplementedKVServer struct{}

func (UnimplementedKVServer) OpenSession(context.Context, *OpenSessionRequest) (*OpenSessionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method OpenSession not implemented")
}
func (UnimplementedKVServer) Register(context.Context, *RegisterRequest) (*RegisterResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Register not implemented")
}
func (UnimplementedKVServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedKVServer) Range(context.Context, *RangeRequest) (*RangeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Range not implemented")
}
func (UnimplementedKVServer) Put(context.Context, *PutRequest) (*PutResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Put not implemented")
}
func (UnimplementedKVServer) Delete(context.Context, *DeleteRequest) (*DeleteResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Delete not implemented")
}
func (UnimplementedKVServer) CompareAndSwap(context.Context, *CompareAndSwapRequest) (*CompareAndSwapResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method CompareAndSwap not implemented")
}
func (UnimplementedKVServer) mustEmbedUnimplementedKVServer() {}
func (UnimplementedKVServer) testEmbeddedByValue()            {}

type UnsafeKVServer interface {
	mustEmbedUnimplementedKVServer()
}

func RegisterKVServer(s grpc.ServiceRegistrar, srv KVServer) {

	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&KV_ServiceDesc, srv)
}

func _KV_OpenSession_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OpenSessionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).OpenSession(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KV_OpenSession_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).OpenSession(ctx, req.(*OpenSessionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Register_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Register(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KV_Register_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Register(ctx, req.(*RegisterRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KV_Get_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Range_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RangeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Range(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KV_Range_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Range(ctx, req.(*RangeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KV_Put_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Put(ctx, req.(*PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KV_Delete_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Delete(ctx, req.(*DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_CompareAndSwap_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CompareAndSwapRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).CompareAndSwap(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KV_CompareAndSwap_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).CompareAndSwap(ctx, req.(*CompareAndSwapRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var KV_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "fivefamilies.v1.KV",
	HandlerType: (*KVServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "OpenSession",
			Handler:    _KV_OpenSession_Handler,
		},
		{
			MethodName: "Register",
			Handler:    _KV_Register_Handler,
		},
		{
			MethodName: "Get",
			Handler:    _KV_Get_Handler,
		},
		{
			MethodName: "Range",
			Handler:    _KV_Range_Handler,
		},
		{
			MethodName: "Put",
			Handler:    _KV_Put_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _KV_Delete_Handler,
		},
		{
			MethodName: "CompareAndSwap",
			Handler:    _KV_CompareAndSwap_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "fivefamilies/v1/kv.proto",
}
