// Package node runs one member of the cluster: a Raft instance, the store it
// replicates into, and the gRPC service nodes use to talk to each other.
package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/Nirmal-22/raft-kv/internal/metrics"
	"github.com/Nirmal-22/raft-kv/internal/pb"
	"github.com/Nirmal-22/raft-kv/internal/store"
)

// ErrNoLeader means the request could not be handed to a leader before its
// deadline. Nothing was written.
var ErrNoLeader = errors.New("no leader available")

// ErrStopped is returned by every operation once Shutdown has begun.
var ErrStopped = errors.New("node is shut down")

// Consistency selects how a read is served.
type Consistency string

const (
	// Linearizable reads go to the leader, which first confirms with a
	// majority that it still is the leader. The result reflects every write
	// that finished before the read started.
	Linearizable Consistency = "linearizable"
	// Stale reads are answered from whatever the contacted node has applied.
	// Fast, works without a leader, and may lag behind.
	Stale Consistency = "stale"
)

type Config struct {
	ID      string
	DataDir string

	// Addresses other nodes use to reach this one. They are also the listen
	// addresses, so they must be host:port pairs the peers can connect to.
	RaftAddr string
	GRPCAddr string
	HTTPAddr string

	// Bootstrap starts a brand-new single-node cluster. Use it on exactly one
	// node, once.
	Bootstrap bool
	// Join lists gRPC addresses of nodes already in the cluster.
	Join []string

	// Raft timing. Zero keeps the library default (about 1s to notice a dead
	// leader). The tests shorten these.
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration

	// SnapshotThreshold is how many log entries accumulate before the log is
	// compacted into a snapshot.
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration
	TrailingLogs      uint64

	// ApplyTimeout bounds how long the leader waits to queue a command.
	ApplyTimeout time.Duration
	// TickInterval is how often the leader checks for keys due to expire.
	TickInterval time.Duration

	LogLevel  string
	LogOutput io.Writer
}

func (c *Config) defaults() {
	if c.ApplyTimeout == 0 {
		c.ApplyTimeout = 5 * time.Second
	}
	if c.TickInterval == 0 {
		c.TickInterval = time.Second
	}
	if c.LogLevel == "" {
		c.LogLevel = "warn"
	}
	if c.LogOutput == nil {
		c.LogOutput = os.Stderr
	}
}

type Node struct {
	cfg     Config
	log     hclog.Logger
	Metrics *metrics.Metrics

	store    *store.Store
	raft     *raft.Raft
	trans    *raft.NetworkTransport
	logStore *raftboltdb.BoltStore
	faults   *faultNet

	grpcServer *grpc.Server
	raftAddr   string
	grpcAddr   string
	needsJoin  bool

	connMu sync.Mutex
	conns  map[string]*grpc.ClientConn

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// life guards the store against use after Close. Every entry point holds
	// the read side for as long as it runs; Shutdown takes the write side
	// before closing anything.
	life    sync.RWMutex
	stopped bool
}

// enter marks the start of an operation. The caller must call the returned
// function when done. Entry points never nest, so the read lock is never
// taken twice by one goroutine.
func (n *Node) enter() (func(), error) {
	n.life.RLock()
	if n.stopped {
		n.life.RUnlock()
		return nil, ErrStopped
	}
	return n.life.RUnlock, nil
}

// New opens the node's data, starts Raft and the gRPC service, and either
// bootstraps a new cluster or starts trying to join an existing one.
func New(cfg Config) (*Node, error) {
	cfg.defaults()
	if cfg.ID == "" {
		return nil, errors.New("node id is required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	n := &Node{
		cfg:     cfg,
		Metrics: metrics.New(),
		faults:  newFaultNet(),
		conns:   make(map[string]*grpc.ClientConn),
		stop:    make(chan struct{}),
	}
	n.log = hclog.New(&hclog.LoggerOptions{
		Name:   "raftkv." + cfg.ID,
		Level:  hclog.LevelFromString(cfg.LogLevel),
		Output: cfg.LogOutput,
	})

	st, err := store.Open(filepath.Join(cfg.DataDir, "state"))
	if err != nil {
		return nil, err
	}
	n.store = st

	if err := n.startRaft(); err != nil {
		st.Close()
		return nil, err
	}
	if err := n.startGRPC(); err != nil {
		n.Shutdown()
		return nil, err
	}
	n.registerGauges()

	// Background work starts only now, when both listeners are up and the
	// node's addresses are final: the join request announces them.
	n.wg.Add(1)
	go n.leaderLoop()
	if n.needsJoin {
		n.wg.Add(1)
		go n.joinLoop()
	}
	return n, nil
}

func (n *Node) startRaft() error {
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(n.cfg.ID)
	rc.Logger = n.log.Named("raft")
	if n.cfg.HeartbeatTimeout > 0 {
		rc.HeartbeatTimeout = n.cfg.HeartbeatTimeout
	}
	if n.cfg.ElectionTimeout > 0 {
		rc.ElectionTimeout = n.cfg.ElectionTimeout
	}
	if n.cfg.LeaderLeaseTimeout > 0 {
		rc.LeaderLeaseTimeout = n.cfg.LeaderLeaseTimeout
	}
	if n.cfg.CommitTimeout > 0 {
		rc.CommitTimeout = n.cfg.CommitTimeout
	}
	if n.cfg.SnapshotThreshold > 0 {
		rc.SnapshotThreshold = n.cfg.SnapshotThreshold
	}
	if n.cfg.SnapshotInterval > 0 {
		rc.SnapshotInterval = n.cfg.SnapshotInterval
	}
	if n.cfg.TrailingLogs > 0 {
		rc.TrailingLogs = n.cfg.TrailingLogs
	}

	ln, err := net.Listen("tcp", n.cfg.RaftAddr)
	if err != nil {
		return fmt.Errorf("listen raft %s: %w", n.cfg.RaftAddr, err)
	}
	n.raftAddr = ln.Addr().String()
	n.trans = raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  &streamLayer{Listener: ln, f: n.faults},
		MaxPool: 3,
		Timeout: 10 * time.Second,
		Logger:  n.log.Named("raft-net"),
	})

	// The Raft log and vote are fsynced to BoltDB: they are the source of
	// truth. The Pebble store is rebuilt from them if it falls behind.
	n.logStore, err = raftboltdb.NewBoltStore(filepath.Join(n.cfg.DataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("open raft log: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStoreWithLogger(n.cfg.DataDir, 2, n.log.Named("snapshot"))
	if err != nil {
		return fmt.Errorf("open snapshot store: %w", err)
	}
	hasState, err := raft.HasExistingState(n.logStore, n.logStore, snaps)
	if err != nil {
		return err
	}

	n.raft, err = raft.NewRaft(rc, &fsm{store: n.store, metrics: n.Metrics}, n.logStore, n.logStore, snaps, n.trans)
	if err != nil {
		return fmt.Errorf("start raft: %w", err)
	}

	switch {
	case hasState:
		// A restart. The node already knows the cluster from its own log.
	case n.cfg.Bootstrap:
		f := n.raft.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(n.cfg.ID),
			Address:  raft.ServerAddress(n.raftAddr),
		}}})
		if err := f.Error(); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
	case len(n.cfg.Join) > 0:
		n.needsJoin = true
	}
	return nil
}

// self describes this node for the member table.
func (n *Node) self() *pb.Member {
	return &pb.Member{Id: n.cfg.ID, RaftAddr: n.raftAddr, GrpcAddr: n.grpcAddr, HttpAddr: n.cfg.HTTPAddr}
}

// joinLoop asks the listed nodes to add this one until one of them succeeds.
func (n *Node) joinLoop() {
	defer n.wg.Done()
	for {
		for _, seed := range n.cfg.Join {
			client, err := n.client(seed)
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = client.Join(ctx, &pb.JoinRequest{Member: n.self()})
				cancel()
			}
			if err == nil {
				n.log.Info("joined cluster", "via", seed)
				return
			}
			n.log.Debug("join attempt failed", "via", seed, "error", err)
		}
		select {
		case <-n.stop:
			return
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// leaderLoop does the two jobs only the leader has: keep its own entry in the
// member table current, and propose Expire commands when keys are due.
func (n *Node) leaderLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stop:
			return
		case isLeader := <-n.raft.LeaderCh():
			if !isLeader {
				continue
			}
			n.Metrics.LeaderChanges.Inc()
			n.log.Info("became leader", "term", n.raft.CurrentTerm())
		case <-ticker.C:
		}
		if n.raft.State() != raft.Leader {
			continue
		}
		leave, err := n.enter()
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ApplyTimeout)
		n.ensureSelfRegistered(ctx)
		n.expireDueKeys(ctx)
		cancel()
		leave()
	}
}

func (n *Node) ensureSelfRegistered(ctx context.Context) {
	me := n.self()
	if cur, ok, err := n.store.Member(n.cfg.ID); err == nil && ok && proto.Equal(cur, me) {
		return
	}
	cmd := &pb.Command{Op: &pb.Command_SetMember{SetMember: &pb.SetMember{Member: me}}}
	if _, err := n.applyLocal(ctx, cmd); err != nil {
		n.log.Debug("could not register self", "error", err)
	}
}

func (n *Node) expireDueKeys(ctx context.Context) {
	next, err := n.store.NextExpiry()
	now := time.Now().UnixMilli()
	if err != nil || next == 0 || next > now {
		return // nothing is due: do not fill the log with empty commands
	}
	cmd := &pb.Command{Op: &pb.Command_Expire{Expire: &pb.Expire{NowMs: now}}}
	if _, err := n.applyLocal(ctx, cmd); err != nil {
		n.log.Debug("expire failed", "error", err)
	}
}

// ------------------------------------------------------- client operations

// Apply runs a write. If this node is not the leader it forwards the command
// to the leader over gRPC, so a client may talk to any node.
func (n *Node) Apply(ctx context.Context, cmd *pb.Command) (*pb.ApplyResult, error) {
	leave, err := n.enter()
	if err != nil {
		return nil, err
	}
	defer leave()
	return route(ctx, n, "apply",
		func() (*pb.ApplyResult, error) { return n.applyLocal(ctx, cmd) },
		func(c pb.ClusterClient) (*pb.ApplyResult, error) { return c.Apply(ctx, cmd) },
	)
}

// Get reads one key at the requested consistency.
func (n *Node) Get(ctx context.Context, key string, consistency Consistency) (*pb.GetResponse, error) {
	leave, err := n.enter()
	if err != nil {
		return nil, err
	}
	defer leave()
	if consistency == Stale {
		return n.readLocal(key, Stale)
	}
	return route(ctx, n, "get",
		func() (*pb.GetResponse, error) { return n.getLocal(ctx, key) },
		func(c pb.ClusterClient) (*pb.GetResponse, error) { return c.Get(ctx, &pb.GetRequest{Key: key}) },
	)
}

// List returns the keys under a prefix at the requested consistency.
func (n *Node) List(ctx context.Context, prefix string, limit int, consistency Consistency) ([]*pb.KeyInfo, error) {
	leave, err := n.enter()
	if err != nil {
		return nil, err
	}
	defer leave()
	if consistency == Stale {
		return n.store.List(prefix, limit)
	}
	res, err := route(ctx, n, "list",
		func() (*pb.ListResponse, error) { return n.listLocal(ctx, prefix, limit) },
		func(c pb.ClusterClient) (*pb.ListResponse, error) {
			return c.List(ctx, &pb.ListRequest{Prefix: prefix, Limit: uint32(limit)})
		},
	)
	if err != nil {
		return nil, err
	}
	return res.Keys, nil
}

// Watch streams changes as this node applies them.
func (n *Node) Watch(prefix string) (<-chan store.Event, func()) {
	leave, err := n.enter()
	if err != nil {
		closed := make(chan store.Event)
		close(closed)
		return closed, func() {}
	}
	defer leave()
	n.Metrics.Watchers.Inc()
	ch, cancel := n.store.Watch(prefix)
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			n.Metrics.Watchers.Dec()
			cancel()
		})
	}
}

// route runs an operation on the leader: here if this node leads, otherwise
// over gRPC. It retries only when it is certain the operation was not started
// (this node, or the node it asked, said "I am not the leader"). Any other
// failure is returned, because the write may or may not have gone through and
// repeating it blindly could apply it twice.
func route[T any](ctx context.Context, n *Node, op string, local func() (T, error), remote func(pb.ClusterClient) (T, error)) (T, error) {
	var zero T
	for {
		if n.raft.State() == raft.Leader {
			v, err := local()
			if !errors.Is(err, raft.ErrNotLeader) {
				return v, err
			}
		} else if client, err := n.leaderClient(); err == nil {
			n.Metrics.Forwarded.WithLabelValues(op).Inc()
			v, err := remote(client)
			if !isNotLeader(err) {
				return v, err
			}
		}
		select {
		case <-ctx.Done():
			return zero, fmt.Errorf("%w: %v", ErrNoLeader, ctx.Err())
		case <-n.stop:
			return zero, ErrNoLeader
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// applyLocal proposes a command. It only succeeds on the leader, and returns
// once a majority has stored the command and this node has applied it.
func (n *Node) applyLocal(ctx context.Context, cmd *pb.Command) (*pb.ApplyResult, error) {
	if n.raft.State() != raft.Leader {
		return nil, raft.ErrNotLeader
	}
	if put := cmd.GetPut(); put != nil && put.TtlMs > 0 {
		// Stamp the expiry with the leader's clock. Copy first: the caller's
		// message must not change under it.
		cmd = proto.Clone(cmd).(*pb.Command)
		put = cmd.GetPut()
		put.ExpiresAtMs = time.Now().UnixMilli() + put.TtlMs
		put.TtlMs = 0
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	f := n.raft.Apply(data, n.cfg.ApplyTimeout)
	if err := waitFuture(ctx, f); err != nil {
		return nil, err
	}
	switch r := f.Response().(type) {
	case *pb.ApplyResult:
		return r, nil
	case error:
		return nil, r
	default:
		return nil, fmt.Errorf("unexpected apply response %T", r)
	}
}

// getLocal is a linearizable read on the leader.
//
// A leader that has been cut off does not know it yet and still holds old
// data. Barrier fixes that: it appends a marker to the log and waits for a
// majority to store it and for this node to apply everything before it. If
// this node has been replaced, the marker never commits and the read fails
// instead of returning stale data.
//
// The cost is one log entry per read. See the README for cheaper schemes.
func (n *Node) getLocal(ctx context.Context, key string) (*pb.GetResponse, error) {
	if err := n.barrier(ctx); err != nil {
		return nil, err
	}
	return n.readLocal(key, Linearizable)
}

func (n *Node) listLocal(ctx context.Context, prefix string, limit int) (*pb.ListResponse, error) {
	if err := n.barrier(ctx); err != nil {
		return nil, err
	}
	keys, err := n.store.List(prefix, limit)
	return &pb.ListResponse{Keys: keys}, err
}

func (n *Node) barrier(ctx context.Context) error {
	if n.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}
	return waitFuture(ctx, n.raft.Barrier(n.cfg.ApplyTimeout))
}

func (n *Node) readLocal(key string, c Consistency) (*pb.GetResponse, error) {
	start := time.Now()
	e, ok, err := n.store.Get(key)
	n.Metrics.ReadDuration.WithLabelValues(string(c)).Observe(time.Since(start).Seconds())
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Found: ok, Value: e.Value, Version: e.Version, ExpiresAtMs: e.ExpiresAtMs}, nil
}

// waitFuture waits for a Raft future but gives up when ctx does. Giving up
// does not cancel the command: it may still commit.
func waitFuture(ctx context.Context, f raft.Future) error {
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// --------------------------------------------------------------- membership

// joinLocal adds a node to the cluster. Leader only.
func (n *Node) joinLocal(ctx context.Context, m *pb.Member) error {
	if n.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}
	if m == nil || m.Id == "" || m.RaftAddr == "" {
		return errors.New("join: member id and raft address are required")
	}
	cf := n.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		return err
	}
	id, addr := raft.ServerID(m.Id), raft.ServerAddress(m.RaftAddr)
	present := false
	for _, srv := range cf.Configuration().Servers {
		if srv.ID != id {
			continue
		}
		if srv.Address == addr {
			present = true // the node is retrying a join that already worked
		} else if err := waitFuture(ctx, n.raft.RemoveServer(id, 0, n.cfg.ApplyTimeout)); err != nil {
			return err // same id at a new address: drop the old entry first
		}
	}
	if !present {
		if err := waitFuture(ctx, n.raft.AddVoter(id, addr, 0, n.cfg.ApplyTimeout)); err != nil {
			return err
		}
	}
	_, err := n.applyLocal(ctx, &pb.Command{Op: &pb.Command_SetMember{SetMember: &pb.SetMember{Member: m}}})
	return err
}

// Remove takes a node out of the cluster. It must be sent to the leader.
func (n *Node) Remove(ctx context.Context, id string) error {
	leave, err := n.enter()
	if err != nil {
		return err
	}
	defer leave()
	if n.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}
	if err := waitFuture(ctx, n.raft.RemoveServer(raft.ServerID(id), 0, n.cfg.ApplyTimeout)); err != nil {
		return err
	}
	_, err = n.applyLocal(ctx, &pb.Command{Op: &pb.Command_RemoveMember{RemoveMember: &pb.RemoveMember{Id: id}}})
	return err
}

// ------------------------------------------------------------------- status

type MemberStatus struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr,omitempty"`
	HTTPAddr string `json:"http_addr,omitempty"`
	Suffrage string `json:"suffrage"`
	Leader   bool   `json:"leader"`
}

type Status struct {
	NodeID       string         `json:"node_id"`
	State        string         `json:"state"`
	LeaderID     string         `json:"leader_id"`
	LeaderHTTP   string         `json:"leader_http,omitempty"`
	Term         uint64         `json:"term"`
	LastIndex    uint64         `json:"last_log_index"`
	CommitIndex  uint64         `json:"commit_index"`
	AppliedIndex uint64         `json:"applied_index"`
	Isolated     bool           `json:"isolated,omitempty"`
	Members      []MemberStatus `json:"members"`
}

// Status reports what this node currently believes about the cluster.
func (n *Node) Status() Status {
	leave, err := n.enter()
	if err != nil {
		return Status{NodeID: n.cfg.ID, State: "Shutdown"}
	}
	defer leave()
	_, leaderID := n.raft.LeaderWithID()
	st := Status{
		NodeID:       n.cfg.ID,
		State:        n.raft.State().String(),
		LeaderID:     string(leaderID),
		Term:         n.raft.CurrentTerm(),
		LastIndex:    n.raft.LastIndex(),
		CommitIndex:  n.raft.CommitIndex(),
		AppliedIndex: n.raft.AppliedIndex(),
		Isolated:     n.faults.Isolated(),
	}
	known := map[string]*pb.Member{}
	if members, err := n.store.Members(); err == nil {
		for _, m := range members {
			known[m.Id] = m
		}
	}
	if m, ok := known[string(leaderID)]; ok {
		st.LeaderHTTP = m.HttpAddr
	}
	if cf := n.raft.GetConfiguration(); cf.Error() == nil {
		for _, srv := range cf.Configuration().Servers {
			ms := MemberStatus{
				ID:       string(srv.ID),
				RaftAddr: string(srv.Address),
				Suffrage: srv.Suffrage.String(),
				Leader:   srv.ID == leaderID,
			}
			if m, ok := known[ms.ID]; ok {
				ms.GRPCAddr, ms.HTTPAddr = m.GrpcAddr, m.HttpAddr
			}
			st.Members = append(st.Members, ms)
		}
	}
	return st
}

func (n *Node) ID() string       { return n.cfg.ID }
func (n *Node) IsLeader() bool   { return n.raft.State() == raft.Leader }
func (n *Node) GRPCAddr() string { return n.grpcAddr }
func (n *Node) RaftAddr() string { return n.raftAddr }

// SetIsolated cuts this node off from its peers, or reconnects it. Clients can
// still reach it, which is exactly the dangerous case: a leader that has been
// partitioned away but does not know it.
func (n *Node) SetIsolated(v bool) { n.faults.SetIsolated(v) }

func (n *Node) registerGauges() {
	m := n.Metrics
	m.Gauge("raftkv_raft_is_leader", "1 if this node is the leader.", func() float64 {
		if n.raft.State() == raft.Leader {
			return 1
		}
		return 0
	})
	m.Gauge("raftkv_raft_term", "Current Raft term.", func() float64 { return float64(n.raft.CurrentTerm()) })
	m.Gauge("raftkv_raft_last_log_index", "Index of the last log entry on this node.", func() float64 { return float64(n.raft.LastIndex()) })
	m.Gauge("raftkv_raft_commit_index", "Highest log index known to be stored on a majority.", func() float64 { return float64(n.raft.CommitIndex()) })
	m.Gauge("raftkv_raft_applied_index", "Highest log index applied to the local store.", func() float64 { return float64(n.raft.AppliedIndex()) })
	m.Gauge("raftkv_raft_peers", "Servers in the cluster configuration.", func() float64 {
		cf := n.raft.GetConfiguration()
		if cf.Error() != nil {
			return 0
		}
		return float64(len(cf.Configuration().Servers))
	})
}

// Shutdown stops the node cleanly. It is safe to call more than once.
func (n *Node) Shutdown() {
	n.stopOnce.Do(func() {
		close(n.stop)
		if n.grpcServer != nil {
			n.grpcServer.Stop()
		}
		if n.raft != nil {
			if err := n.raft.Shutdown().Error(); err != nil {
				n.log.Warn("raft shutdown", "error", err)
			}
		}
		n.wg.Wait()
		// Closing the outgoing connections aborts any request this node is
		// still forwarding. With Raft down as well, every in-flight operation
		// fails fast and releases the lock taken next.
		n.connMu.Lock()
		for _, c := range n.conns {
			c.Close()
		}
		n.connMu.Unlock()
		n.life.Lock()
		defer n.life.Unlock()
		n.stopped = true
		if n.trans != nil {
			n.trans.Close()
		}
		if n.logStore != nil {
			n.logStore.Close()
		}
		n.store.Close()
	})
}
