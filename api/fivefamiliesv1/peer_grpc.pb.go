package fivefamiliesv1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const _ = grpc.SupportPackageIsVersion9

const (
	Peer_Send_FullMethodName            = "/fivefamilies.v1.Peer/Send"
	Peer_InstallSnapshot_FullMethodName = "/fivefamilies.v1.Peer/InstallSnapshot"
)

type PeerClient interface {
	Send(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[SendRequest, SendResponse], error)
	InstallSnapshot(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[InstallSnapshotRequest, InstallSnapshotResponse], error)
}

type peerClient struct {
	cc grpc.ClientConnInterface
}

func NewPeerClient(cc grpc.ClientConnInterface) PeerClient {
	return &peerClient{cc}
}

func (c *peerClient) Send(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[SendRequest, SendResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &Peer_ServiceDesc.Streams[0], Peer_Send_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[SendRequest, SendResponse]{ClientStream: stream}
	return x, nil
}

type Peer_SendClient = grpc.ClientStreamingClient[SendRequest, SendResponse]

func (c *peerClient) InstallSnapshot(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[InstallSnapshotRequest, InstallSnapshotResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &Peer_ServiceDesc.Streams[1], Peer_InstallSnapshot_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[InstallSnapshotRequest, InstallSnapshotResponse]{ClientStream: stream}
	return x, nil
}

type Peer_InstallSnapshotClient = grpc.ClientStreamingClient[InstallSnapshotRequest, InstallSnapshotResponse]

type PeerServer interface {
	Send(grpc.ClientStreamingServer[SendRequest, SendResponse]) error
	InstallSnapshot(grpc.ClientStreamingServer[InstallSnapshotRequest, InstallSnapshotResponse]) error
	mustEmbedUnimplementedPeerServer()
}

type UnimplementedPeerServer struct{}

func (UnimplementedPeerServer) Send(grpc.ClientStreamingServer[SendRequest, SendResponse]) error {
	return status.Error(codes.Unimplemented, "method Send not implemented")
}
func (UnimplementedPeerServer) InstallSnapshot(grpc.ClientStreamingServer[InstallSnapshotRequest, InstallSnapshotResponse]) error {
	return status.Error(codes.Unimplemented, "method InstallSnapshot not implemented")
}
func (UnimplementedPeerServer) mustEmbedUnimplementedPeerServer() {}
func (UnimplementedPeerServer) testEmbeddedByValue()              {}

type UnsafePeerServer interface {
	mustEmbedUnimplementedPeerServer()
}

func RegisterPeerServer(s grpc.ServiceRegistrar, srv PeerServer) {

	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&Peer_ServiceDesc, srv)
}

func _Peer_Send_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(PeerServer).Send(&grpc.GenericServerStream[SendRequest, SendResponse]{ServerStream: stream})
}

type Peer_SendServer = grpc.ClientStreamingServer[SendRequest, SendResponse]

func _Peer_InstallSnapshot_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(PeerServer).InstallSnapshot(&grpc.GenericServerStream[InstallSnapshotRequest, InstallSnapshotResponse]{ServerStream: stream})
}

type Peer_InstallSnapshotServer = grpc.ClientStreamingServer[InstallSnapshotRequest, InstallSnapshotResponse]

var Peer_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "fivefamilies.v1.Peer",
	HandlerType: (*PeerServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Send",
			Handler:       _Peer_Send_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "InstallSnapshot",
			Handler:       _Peer_InstallSnapshot_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "fivefamilies/v1/peer.proto",
}
