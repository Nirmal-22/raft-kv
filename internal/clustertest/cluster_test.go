package clustertest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nirmal-22/raft-kv/internal/node"
	"github.com/Nirmal-22/raft-kv/internal/pb"
)

func followerOf(c *cluster, leader int) int {
	return (leader + 1) % len(c.cfgs)
}

func TestWritesReplicateAndFollowersForward(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader()
	follower := c.node(followerOf(c, leader))
	other := c.node((leader + 2) % 3)

	// A write sent to a follower is forwarded to the leader.
	res := mustPut(t, follower, "greeting", "hello")
	if res.Status != pb.Status_STATUS_OK || res.Version == 0 {
		t.Fatalf("put via follower: %v", res)
	}

	// A linearizable read through a different follower sees it at once.
	got := mustGet(t, other, "greeting", node.Linearizable)
	if !got.Found || string(got.Value) != "hello" || got.Version != res.Version {
		t.Fatalf("linearizable read via follower: %v", got)
	}

	// Every node ends up with its own copy.
	eventually(t, 5*time.Second, "all three copies hold the value", func() bool {
		for _, n := range c.running() {
			if staleValue(n, "greeting") != "hello" {
				return false
			}
		}
		return true
	})

	// Compare-and-swap is decided by the leader, wherever it was sent.
	stale := res.Version
	mustPut(t, c.node(leader), "greeting", "changed")
	ctx, cancel := ctxFor(5 * time.Second)
	defer cancel()
	cas, err := follower.Apply(ctx, &pb.Command{Op: &pb.Command_Put{Put: &pb.Put{
		Key: "greeting", Value: []byte("lost update"), IfVersion: &stale,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if cas.Status != pb.Status_STATUS_VERSION_MISMATCH {
		t.Fatalf("stale compare-and-swap was accepted: %v", cas)
	}
	if v := mustGet(t, other, "greeting", node.Linearizable); string(v.Value) != "changed" {
		t.Fatalf("value = %q, want changed", v.Value)
	}
}

func TestLeaderCrashLosesNothing(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader()

	const keys = 30
	for i := 0; i < keys; i++ {
		mustPut(t, c.node(leader), fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
	}

	c.stop(leader)
	newLeader := c.waitLeader(leader)
	if newLeader == leader {
		t.Fatal("stopped node is still reported as leader")
	}

	// Everything acknowledged before the crash is still there.
	survivor := c.node(newLeader)
	for i := 0; i < keys; i++ {
		got := mustGet(t, survivor, fmt.Sprintf("k%02d", i), node.Linearizable)
		if !got.Found || string(got.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%02d after failover: %v", i, got)
		}
	}

	// Two of three nodes are a majority, so writes continue.
	mustPut(t, survivor, "after-failover", "ok")

	// The old leader comes back with its old data directory and catches up.
	back := c.start(leader)
	eventually(t, 10*time.Second, "the restarted node catches up", func() bool {
		return staleValue(back, "after-failover") == "ok" && staleValue(back, "k00") == "v0"
	})
	c.waitHealthy(3)
}

func TestMinorityRefusesWritesAndLinearizableReads(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader()
	mustPut(t, c.node(leader), "k", "v")
	last := (leader + 2) % 3
	eventually(t, 5*time.Second, "the surviving node has the value", func() bool {
		return staleValue(c.node(last), "k") == "v"
	})

	c.stop(leader)
	c.stop(followerOf(c, leader))
	alone := c.node(last)

	ctx, cancel := ctxFor(700 * time.Millisecond)
	defer cancel()
	if _, err := alone.Apply(ctx, putCmd("k", "should not be accepted")); err == nil {
		t.Fatal("a single node out of three accepted a write")
	}
	ctx2, cancel2 := ctxFor(700 * time.Millisecond)
	defer cancel2()
	if _, err := alone.Get(ctx2, "k", node.Linearizable); err == nil {
		t.Fatal("a single node out of three served a linearizable read")
	}

	// A stale read is the escape hatch: it still answers, from local data.
	if got := staleValue(alone, "k"); got != "v" {
		t.Fatalf("stale read = %q, want v", got)
	}
}

// The classic stale-read bug: a leader is cut off, the rest elect a new leader
// and accept a write, and a client still talking to the old leader reads the
// old value. A linearizable read must fail here instead.
func TestPartitionedLeaderDoesNotServeStaleReads(t *testing.T) {
	c := newCluster(t, 3, nil)
	old := c.waitLeader()
	oldLeader := c.node(old)
	mustPut(t, oldLeader, "k", "v1")

	oldLeader.SetIsolated(true)

	// The majority side elects a new leader and moves on.
	newLeader := c.node(c.waitLeader(old))
	mustPut(t, newLeader, "k", "v2")

	// Ask the cut-off node. It may still believe it is the leader.
	for i := 0; i < 5; i++ {
		ctx, cancel := ctxFor(200 * time.Millisecond)
		res, err := oldLeader.Get(ctx, "k", node.Linearizable)
		cancel()
		if err == nil {
			t.Fatalf("isolated node answered a linearizable read with %q (found=%v)", res.Value, res.Found)
		}
	}
	ctx, cancel := ctxFor(300 * time.Millisecond)
	_, err := oldLeader.Apply(ctx, putCmd("k", "from the minority"))
	cancel()
	if err == nil {
		t.Fatal("isolated node acknowledged a write")
	}

	// Its own copy is behind, which is what a stale read is allowed to show.
	if got := staleValue(oldLeader, "k"); got != "v1" {
		t.Fatalf("stale read on isolated node = %q, want v1", got)
	}

	// Heal the partition: the old leader rejoins as a follower and catches up.
	oldLeader.SetIsolated(false)
	eventually(t, 10*time.Second, "the old leader catches up after the partition heals", func() bool {
		return staleValue(oldLeader, "k") == "v2"
	})
	c.waitHealthy(3)
	if got := mustGet(t, oldLeader, "k", node.Linearizable); string(got.Value) != "v2" {
		t.Fatalf("after healing: %q, want v2", got.Value)
	}
}

func TestRestartKeepsDataAndVersions(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader()
	n := c.node(leader)

	first := mustPut(t, n, "k", "v1")
	ctx, cancel := ctxFor(5 * time.Second)
	defer cancel()
	second, err := n.Apply(ctx, &pb.Command{Op: &pb.Command_Put{Put: &pb.Put{
		Key: "k", Value: []byte("v2"), IfVersion: &first.Version,
	}}})
	if err != nil || second.Status != pb.Status_STATUS_OK {
		t.Fatalf("compare-and-swap: %v %v", second, err)
	}

	// Restart every node. Each replays its Raft log over a store that already
	// holds the data; the replayed compare-and-swap must not run again.
	for i := range c.cfgs {
		c.stop(i)
	}
	for i := range c.cfgs {
		c.start(i)
	}
	c.waitHealthy(3)

	got := mustGet(t, c.node(c.waitLeader()), "k", node.Linearizable)
	if string(got.Value) != "v2" || got.Version != second.Version {
		t.Fatalf("after full restart: value=%q version=%d, want v2 at version %d", got.Value, got.Version, second.Version)
	}
}

func TestNewNodeCatchesUpFromSnapshot(t *testing.T) {
	small := func(cfg *node.Config) {
		cfg.SnapshotThreshold = 16
		cfg.SnapshotInterval = 50 * time.Millisecond
		cfg.TrailingLogs = 4
	}
	c := newCluster(t, 3, small)
	leader := c.node(c.waitLeader())

	const keys = 120
	for i := 0; i < keys; i++ {
		mustPut(t, leader, fmt.Sprintf("snap/%03d", i), fmt.Sprintf("v%d", i))
	}
	// Wait for every node, not only the current leader: if leadership moves
	// before the new node joins, the new leader must have compacted too, or it
	// could bring the newcomer up to date from its log alone.
	eventually(t, 10*time.Second, "every node compacts its log into a snapshot", func() bool {
		for _, cfg := range c.cfgs {
			if entries, _ := os.ReadDir(filepath.Join(cfg.DataDir, "snapshots")); len(entries) == 0 {
				return false
			}
		}
		return true
	})

	// The log no longer reaches back to the start, so a brand-new node can
	// only be brought up to date by shipping it a snapshot.
	i := c.add(small)
	fresh := c.start(i)
	eventually(t, 15*time.Second, "the new node receives all keys", func() bool {
		return staleValue(fresh, "snap/000") == "v0" && staleValue(fresh, fmt.Sprintf("snap/%03d", keys-1)) == fmt.Sprintf("v%d", keys-1)
	})
	entries, _ := os.ReadDir(filepath.Join(c.cfgs[i].DataDir, "snapshots"))
	if len(entries) == 0 {
		t.Fatal("the new node caught up without installing a snapshot; the test did not exercise what it claims")
	}
	c.waitHealthy(4)

	// Writes after the snapshot still reach it through normal replication.
	mustPut(t, leader, "snap/after", "ok")
	eventually(t, 5*time.Second, "the new node receives later writes", func() bool {
		return staleValue(fresh, "snap/after") == "ok"
	})
}

func TestTTLExpiresOnEveryNode(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader()
	follower := c.node(followerOf(c, leader))

	ctx, cancel := ctxFor(5 * time.Second)
	defer cancel()
	_, err := follower.Apply(ctx, &pb.Command{Op: &pb.Command_Put{Put: &pb.Put{
		Key: "session", Value: []byte("token"), TtlMs: 300,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, follower, "permanent", "stays")

	got := mustGet(t, follower, "session", node.Linearizable)
	if !got.Found || got.ExpiresAtMs == 0 {
		t.Fatalf("key with TTL right after the write: %v", got)
	}

	eventually(t, 5*time.Second, "the key expires on all three nodes", func() bool {
		for _, n := range c.running() {
			if staleValue(n, "session") != "" {
				return false
			}
		}
		return true
	})
	if v := mustGet(t, follower, "permanent", node.Linearizable); !v.Found {
		t.Fatal("a key without TTL was removed")
	}
}

func TestRemoveMember(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader()
	victim := followerOf(c, leader)

	ctx, cancel := ctxFor(5 * time.Second)
	defer cancel()
	if err := c.node(victim).Remove(ctx, c.cfgs[leader].ID); err == nil {
		t.Fatal("a follower accepted a membership change")
	}
	if err := c.node(leader).Remove(ctx, c.cfgs[victim].ID); err != nil {
		t.Fatal(err)
	}
	c.stop(victim)
	eventually(t, 5*time.Second, "the cluster shrinks to two members", func() bool {
		return len(c.node(leader).Status().Members) == 2
	})
	mustPut(t, c.node(leader), "k", "still writable")
}

func TestOperationsAfterShutdownFailCleanly(t *testing.T) {
	c := newCluster(t, 1, nil)
	n := c.node(0)
	mustPut(t, n, "k", "v")
	n.Shutdown()

	ctx, cancel := ctxFor(time.Second)
	defer cancel()
	if _, err := n.Apply(ctx, putCmd("k", "v2")); !errors.Is(err, node.ErrStopped) {
		t.Fatalf("apply after shutdown: %v, want ErrStopped", err)
	}
	if _, err := n.Get(ctx, "k", node.Stale); !errors.Is(err, node.ErrStopped) {
		t.Fatalf("get after shutdown: %v, want ErrStopped", err)
	}
}
