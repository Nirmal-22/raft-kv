package node

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/hashicorp/raft"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Nirmal-22/raft-kv/internal/pb"
)

func (n *Node) startGRPC() error {
	ln, err := net.Listen("tcp", n.cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc %s: %w", n.cfg.GRPCAddr, err)
	}
	n.grpcAddr = ln.Addr().String()
	n.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(n.dropWhenIsolated),
		grpc.StatsHandler(otelgrpc.NewServerHandler()), // a no-op unless tracing is switched on
	)
	pb.RegisterClusterServer(n.grpcServer, &clusterServer{n: n})
	go func() {
		if err := n.grpcServer.Serve(ln); err != nil {
			n.log.Debug("grpc server stopped", "error", err)
		}
	}()
	return nil
}

func (n *Node) dropWhenIsolated(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if n.faults.Isolated() {
		return nil, status.Error(codes.Unavailable, errIsolated.Error())
	}
	return handler(ctx, req)
}

// client returns a cached gRPC connection to another node.
func (n *Node) client(addr string) (pb.ClusterClient, error) {
	if n.faults.Isolated() {
		return nil, errIsolated
	}
	n.connMu.Lock()
	defer n.connMu.Unlock()
	conn, ok := n.conns[addr]
	if !ok {
		var err error
		// Plain TCP between nodes. A real deployment would use mutual TLS here.
		conn, err = grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		)
		if err != nil {
			return nil, err
		}
		n.conns[addr] = conn
	}
	return pb.NewClusterClient(conn), nil
}

// leaderClient finds the leader's gRPC address in the replicated member table.
func (n *Node) leaderClient() (pb.ClusterClient, error) {
	_, id := n.raft.LeaderWithID()
	if id == "" {
		return nil, ErrNoLeader
	}
	m, ok, err := n.store.Member(string(id))
	if err != nil {
		return nil, err
	}
	if !ok || m.GrpcAddr == "" {
		return nil, ErrNoLeader // the leader has not registered itself yet
	}
	return n.client(m.GrpcAddr)
}

// clusterServer answers requests from other nodes. Apply, Get and List run
// here or not at all: a node that is not the leader says so and the caller
// looks the leader up again. That rule is what prevents forwarding loops.
type clusterServer struct {
	pb.UnimplementedClusterServer
	n *Node
}

func (s *clusterServer) Apply(ctx context.Context, cmd *pb.Command) (*pb.ApplyResult, error) {
	leave, err := s.n.enter()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	defer leave()
	res, err := s.n.applyLocal(ctx, cmd)
	return res, toStatus(err)
}

func (s *clusterServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	leave, err := s.n.enter()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	defer leave()
	res, err := s.n.getLocal(ctx, req.Key)
	return res, toStatus(err)
}

func (s *clusterServer) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	leave, err := s.n.enter()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	defer leave()
	res, err := s.n.listLocal(ctx, req.Prefix, int(req.Limit))
	return res, toStatus(err)
}

// Join is the one call a non-leader passes on, once: a new node only knows
// the addresses it was given and cannot look the leader up yet.
func (s *clusterServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	leave, err := s.n.enter()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	defer leave()
	if s.n.raft.State() == raft.Leader {
		return &pb.JoinResponse{}, toStatus(s.n.joinLocal(ctx, req.Member))
	}
	if req.Forwarded {
		return nil, status.Error(codes.FailedPrecondition, raft.ErrNotLeader.Error())
	}
	client, err := s.n.leaderClient()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return client.Join(ctx, &pb.JoinRequest{Member: req.Member, Forwarded: true})
}

// toStatus maps errors onto gRPC codes. FAILED_PRECONDITION is reserved for
// "I am not the leader and did nothing", the only case a caller may retry.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, raft.ErrNotLeader):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		// Includes ErrLeadershipLost: the command was proposed and its fate is
		// unknown.
		return status.Error(codes.Unavailable, err.Error())
	}
}

func isNotLeader(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}
