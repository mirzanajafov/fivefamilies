package fivefamiliesv1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const _ = grpc.SupportPackageIsVersion9

const (
	Watch_Watch_FullMethodName = "/fivefamilies.v1.Watch/Watch"
)

type WatchClient interface {
	Watch(ctx context.Context, in *WatchRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[WatchResponse], error)
}

type watchClient struct {
	cc grpc.ClientConnInterface
}

func NewWatchClient(cc grpc.ClientConnInterface) WatchClient {
	return &watchClient{cc}
}

func (c *watchClient) Watch(ctx context.Context, in *WatchRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[WatchResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &Watch_ServiceDesc.Streams[0], Watch_Watch_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[WatchRequest, WatchResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Watch_WatchClient = grpc.ServerStreamingClient[WatchResponse]

type WatchServer interface {
	Watch(*WatchRequest, grpc.ServerStreamingServer[WatchResponse]) error
	mustEmbedUnimplementedWatchServer()
}

type UnimplementedWatchServer struct{}

func (UnimplementedWatchServer) Watch(*WatchRequest, grpc.ServerStreamingServer[WatchResponse]) error {
	return status.Error(codes.Unimplemented, "method Watch not implemented")
}
func (UnimplementedWatchServer) mustEmbedUnimplementedWatchServer() {}
func (UnimplementedWatchServer) testEmbeddedByValue()               {}

type UnsafeWatchServer interface {
	mustEmbedUnimplementedWatchServer()
}

func RegisterWatchServer(s grpc.ServiceRegistrar, srv WatchServer) {

	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&Watch_ServiceDesc, srv)
}

func _Watch_Watch_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(WatchRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(WatchServer).Watch(m, &grpc.GenericServerStream[WatchRequest, WatchResponse]{ServerStream: stream})
}

type Watch_WatchServer = grpc.ServerStreamingServer[WatchResponse]

var Watch_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "fivefamilies.v1.Watch",
	HandlerType: (*WatchServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Watch",
			Handler:       _Watch_Watch_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "fivefamilies/v1/watch.proto",
}
