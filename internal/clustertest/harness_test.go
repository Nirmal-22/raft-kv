// Package clustertest runs real multi-node clusters inside the test process:
// real Raft over TCP on localhost, real gRPC between nodes, real Pebble and
// BoltDB files in temporary directories. Only the timeouts are shortened.
package clustertest

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nirmal-22/raft-kv/internal/node"
	"github.com/Nirmal-22/raft-kv/internal/pb"
)

type cluster struct {
	t    *testing.T
	mu   sync.RWMutex
	cfgs []node.Config
	live []*node.Node // nil while a node is stopped
}

// nextPort hands out ports below Linux's ephemeral range (32768 and up). Ports
// from that range can be taken as the source port of an outgoing connection
// between "reserve" and "bind", which made node start-up fail now and then.
// The base varies by process so two test binaries rarely overlap.
var nextPort atomic.Int32

func init() { nextPort.Store(int32(20000 + (os.Getpid()%300)*30)) }

// freeAddr returns a localhost address that is free right now.
func freeAddr(t *testing.T) string {
	t.Helper()
	for i := 0; i < 200; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", nextPort.Add(1))
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			continue
		}
		ln.Close()
		return addr
	}
	t.Fatal("no free port found")
	return ""
}

func newCluster(t *testing.T, size int, tweak func(*node.Config)) *cluster {
	t.Helper()
	c := &cluster{t: t}
	t.Cleanup(c.shutdown)
	for i := 0; i < size; i++ {
		c.add(tweak)
	}
	for i := range c.cfgs {
		c.start(i)
	}
	c.waitHealthy(size)
	return c
}

// add creates the configuration for one more node without starting it.
func (c *cluster) add(tweak func(*node.Config)) int {
	c.t.Helper()
	i := len(c.cfgs)
	logLevel := "off"
	if os.Getenv("RAFTKV_TEST_LOG") != "" {
		logLevel = os.Getenv("RAFTKV_TEST_LOG")
	}
	cfg := node.Config{
		ID:                 fmt.Sprintf("n%d", i+1),
		DataDir:            filepath.Join(c.t.TempDir(), fmt.Sprintf("n%d", i+1)),
		RaftAddr:           freeAddr(c.t),
		GRPCAddr:           freeAddr(c.t),
		HTTPAddr:           fmt.Sprintf("127.0.0.1:%d", 18000+i),
		Bootstrap:          i == 0,
		HeartbeatTimeout:   100 * time.Millisecond,
		ElectionTimeout:    100 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
		ApplyTimeout:       2 * time.Second,
		TickInterval:       30 * time.Millisecond,
		LogLevel:           logLevel,
	}
	if i > 0 {
		// Every later node knows every earlier one, so a join still works
		// when one of them is down.
		for _, other := range c.cfgs {
			cfg.Join = append(cfg.Join, other.GRPCAddr)
		}
	}
	if tweak != nil {
		tweak(&cfg)
	}
	c.mu.Lock()
	c.cfgs = append(c.cfgs, cfg)
	c.live = append(c.live, nil)
	c.mu.Unlock()
	return i
}

func (c *cluster) start(i int) *node.Node {
	c.t.Helper()
	n, err := node.New(c.cfgs[i])
	if err != nil {
		c.t.Fatalf("start node %d: %v", i+1, err)
	}
	c.mu.Lock()
	c.live[i] = n
	c.mu.Unlock()
	return n
}

// stop shuts a node down cleanly and keeps its data directory, so start(i)
// brings the same node back.
func (c *cluster) stop(i int) {
	c.mu.Lock()
	n := c.live[i]
	c.live[i] = nil
	c.mu.Unlock()
	if n != nil {
		n.Shutdown()
	}
}

func (c *cluster) shutdown() {
	for i := range c.cfgs {
		c.stop(i)
	}
}

func (c *cluster) node(i int) *node.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.live[i]
}

func (c *cluster) running() []*node.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []*node.Node
	for _, n := range c.live {
		if n != nil {
			out = append(out, n)
		}
	}
	return out
}

// leaderIndex returns the index of a node that currently claims leadership,
// or -1. During a partition two nodes can claim it for a moment.
func (c *cluster) leaderIndex(skip ...int) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
next:
	for i, n := range c.live {
		for _, s := range skip {
			if s == i {
				continue next
			}
		}
		if n != nil && n.IsLeader() {
			return i
		}
	}
	return -1
}

func (c *cluster) waitLeader(skip ...int) int {
	c.t.Helper()
	leader := -1
	eventually(c.t, 10*time.Second, "a leader is elected", func() bool {
		leader = c.leaderIndex(skip...)
		return leader >= 0
	})
	return leader
}

// waitHealthy waits until every running node agrees on a leader, sees the
// expected number of members and knows how to reach the leader.
func (c *cluster) waitHealthy(members int) {
	c.t.Helper()
	eventually(c.t, 15*time.Second, fmt.Sprintf("all nodes see %d members and a reachable leader", members), func() bool {
		leaders := map[string]bool{}
		for _, n := range c.running() {
			st := n.Status()
			if st.LeaderID == "" || len(st.Members) != members || st.LeaderHTTP == "" {
				return false
			}
			leaders[st.LeaderID] = true
		}
		return len(leaders) == 1
	})
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting until %s", timeout, what)
}

func ctxFor(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func putCmd(key, value string) *pb.Command {
	return &pb.Command{Op: &pb.Command_Put{Put: &pb.Put{Key: key, Value: []byte(value)}}}
}

func delCmd(key string) *pb.Command {
	return &pb.Command{Op: &pb.Command_Delete{Delete: &pb.Delete{Key: key}}}
}

func mustPut(t *testing.T, n *node.Node, key, value string) *pb.ApplyResult {
	t.Helper()
	ctx, cancel := ctxFor(5 * time.Second)
	defer cancel()
	res, err := n.Apply(ctx, putCmd(key, value))
	if err != nil {
		t.Fatalf("put %s via %s: %v", key, n.ID(), err)
	}
	return res
}

func mustGet(t *testing.T, n *node.Node, key string, c node.Consistency) *pb.GetResponse {
	t.Helper()
	ctx, cancel := ctxFor(5 * time.Second)
	defer cancel()
	res, err := n.Get(ctx, key, c)
	if err != nil {
		t.Fatalf("get %s via %s (%s): %v", key, n.ID(), c, err)
	}
	return res
}

// staleValue reads a node's own copy; "" means the key is absent.
func staleValue(n *node.Node, key string) string {
	ctx, cancel := ctxFor(time.Second)
	defer cancel()
	res, err := n.Get(ctx, key, node.Stale)
	if err != nil || !res.Found {
		return ""
	}
	return string(res.Value)
}
