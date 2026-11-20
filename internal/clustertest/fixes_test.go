package clustertest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nirmal-22/raft-kv/internal/node"
	"github.com/Nirmal-22/raft-kv/internal/pb"
)

// The leader can be asked to remove itself. That used to take effect but
// report "send this to the leader" and leave the node in the member table.
func TestLeaderCanRemoveItself(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader()
	id := c.cfgs[leader].ID

	ctx, cancel := ctxFor(5 * time.Second)
	defer cancel()
	if err := c.node(leader).Remove(ctx, id); err != nil {
		t.Fatalf("leader removing itself: %v", err)
	}
	c.stop(leader)

	next := c.node(c.waitLeader(leader))
	eventually(t, 10*time.Second, "the remaining nodes agree on two members", func() bool {
		return len(next.Status().Members) == 2
	})
	mustPut(t, next, "k", "still writable")
	table, err := next.MemberTable()
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range table {
		if member == id {
			t.Fatalf("removed node %s is still in the member table %v", id, table)
		}
	}
}

func snapshotIndexes(dir string) int {
	entries, _ := os.ReadDir(filepath.Join(dir, "snapshots"))
	return len(entries)
}

// A restart must not reload the snapshot when the store on disk already holds
// that data, and must reload it when the store does not.
func TestRestartReloadsSnapshotOnlyWhenNeeded(t *testing.T) {
	small := func(cfg *node.Config) {
		cfg.SnapshotThreshold = 16
		cfg.SnapshotInterval = 50 * time.Millisecond
		cfg.TrailingLogs = 4
	}
	c := newCluster(t, 3, small)
	leader := c.waitLeader()
	victim := followerOf(c, leader)

	const keys = 80
	var last *pb.ApplyResult
	for i := 0; i < keys; i++ {
		last = mustPut(t, c.node(leader), fmt.Sprintf("r/%03d", i), fmt.Sprintf("v%d", i))
	}
	eventually(t, 10*time.Second, "the follower takes a snapshot and applies every key", func() bool {
		return snapshotIndexes(c.cfgs[victim].DataDir) > 0 && staleValue(c.node(victim), fmt.Sprintf("r/%03d", keys-1)) != ""
	})

	// Clean restart: the store is at least as new as the snapshot.
	c.stop(victim)
	n := c.start(victim)
	c.waitHealthy(3)
	if got := n.SnapshotRestores(); got != 0 {
		t.Fatalf("restart reloaded the snapshot %d time(s) although the store already had the data", got)
	}
	ctx, cancel := ctxFor(time.Second)
	res, err := n.Get(ctx, fmt.Sprintf("r/%03d", keys-1), node.Stale)
	cancel()
	if err != nil || !res.Found || res.Version != last.Version {
		t.Fatalf("after restart: %v %v, want version %d", res, err, last.Version)
	}
	mustPut(t, c.node(c.waitLeader()), "r/after", "ok")
	eventually(t, 5*time.Second, "the restarted node keeps following the log", func() bool {
		return staleValue(n, "r/after") == "ok"
	})

	// Now lose the store entirely, as a crash that wiped unsynced data would
	// in the worst case. The log no longer reaches back far enough, so the
	// node has to load the snapshot.
	c.stop(victim)
	if err := os.RemoveAll(filepath.Join(c.cfgs[victim].DataDir, "state")); err != nil {
		t.Fatal(err)
	}
	n = c.start(victim)
	eventually(t, 10*time.Second, "the node rebuilds its store", func() bool {
		return staleValue(n, "r/000") == "v0" && staleValue(n, "r/after") == "ok"
	})
	if got := n.SnapshotRestores(); got == 0 {
		t.Fatal("the store was empty but no snapshot was loaded; the data came back some other way")
	}
}

// A List reply of about 4.5 MiB is over gRPC's default message limit, so it
// used to work on the leader and fail when sent to a follower.
func TestLargeListThroughFollower(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped with -short")
	}
	c := newCluster(t, 3, nil)
	leader := c.waitLeader()

	const keys = 4500
	pad := strings.Repeat("x", 1000)
	var wg sync.WaitGroup
	var failed atomic.Int32
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < keys; i += 16 {
				ctx, cancel := ctxFor(10 * time.Second)
				_, err := c.node(leader).Apply(ctx, putCmd(fmt.Sprintf("%s/%05d", pad, i), "v"))
				cancel()
				if err != nil {
					failed.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	if failed.Load() > 0 {
		t.Fatalf("%d writes failed while loading keys", failed.Load())
	}

	ctx, cancel := ctxFor(20 * time.Second)
	defer cancel()
	got, err := c.node(followerOf(c, leader)).List(ctx, pad, 10000, node.Linearizable)
	if err != nil {
		t.Fatalf("list through a follower: %v", err)
	}
	if len(got) != keys {
		t.Fatalf("listed %d keys, want %d", len(got), keys)
	}
}

// Compare-and-swap under faults. Clients increment one counter with
// read-then-swap while the leader is partitioned and nodes restart. If a swap
// were ever applied twice, lost, or accepted against a stale version, the
// final value would not match the number of swaps the clients saw succeed.
//
// This covers what the Porcupine run leaves out: its model has no versions.
func TestCompareAndSwapCounterUnderFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped with -short")
	}
	c := newCluster(t, 3, nil)
	zero := uint64(0)
	ctx, cancel := ctxFor(5 * time.Second)
	_, err := c.node(c.waitLeader()).Apply(ctx, &pb.Command{Op: &pb.Command_Put{Put: &pb.Put{
		Key: "counter", Value: []byte("0"), IfVersion: &zero,
	}}})
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	var acked, unknown atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for id := 0; id < 5; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				n := c.node((id + i) % len(c.cfgs))
				if n == nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				ctx, cancel := ctxFor(400 * time.Millisecond)
				cur, err := n.Get(ctx, "counter", node.Linearizable)
				if err != nil || !cur.Found {
					cancel()
					continue
				}
				value, _ := strconv.Atoi(string(cur.Value))
				res, err := n.Apply(ctx, &pb.Command{Op: &pb.Command_Put{Put: &pb.Put{
					Key: "counter", Value: []byte(strconv.Itoa(value + 1)), IfVersion: &cur.Version,
				}}})
				cancel()
				switch {
				case err == nil && res.Status == pb.Status_STATUS_OK:
					acked.Add(1)
				case err == nil:
					// Version mismatch: another client got there first.
				case errors.Is(err, node.ErrNoLeader), errors.Is(err, node.ErrStopped):
					// Never proposed.
				default:
					unknown.Add(1)
				}
			}
		}(id)
	}

	deadline := time.Now().Add(4 * time.Second)
	for round := 0; time.Now().Before(deadline); round++ {
		time.Sleep(300 * time.Millisecond)
		if round%2 == 0 {
			if i := c.leaderIndex(); i >= 0 {
				n := c.node(i)
				n.SetIsolated(true)
				time.Sleep(400 * time.Millisecond)
				n.SetIsolated(false)
			}
		} else {
			i := round % len(c.cfgs)
			c.stop(i)
			time.Sleep(300 * time.Millisecond)
			c.start(i)
		}
	}
	close(stop)
	wg.Wait()
	c.waitHealthy(3)

	final, _ := strconv.Atoi(string(mustGet(t, c.node(c.waitLeader()), "counter", node.Linearizable).Value))
	lo, hi := acked.Load(), acked.Load()+unknown.Load()
	t.Logf("counter=%d, %d swaps acknowledged, %d with unknown outcome", final, lo, unknown.Load())
	if lo < 50 {
		t.Fatalf("only %d swaps succeeded; the cluster was unavailable for most of the test", lo)
	}
	if int64(final) < lo || int64(final) > hi {
		t.Fatalf("counter is %d but must be between %d (acknowledged swaps) and %d (plus unknown outcomes)", final, lo, hi)
	}
}

// A key's expiry must survive a change of leader: the new leader has to
// remove it, and must not remove it early.
func TestTTLSurvivesLeaderChange(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader()

	const ttl = 1500 * time.Millisecond
	// Start the clock before the write: the leader stamps the expiry while
	// handling it, so the key may not disappear before written+ttl.
	written := time.Now()
	ctx, cancel := ctxFor(5 * time.Second)
	_, err := c.node(leader).Apply(ctx, &pb.Command{Op: &pb.Command_Put{Put: &pb.Put{
		Key: "lease", Value: []byte("held"), TtlMs: ttl.Milliseconds(),
	}}})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	c.stop(leader)
	next := c.node(c.waitLeader(leader))

	got := mustGet(t, next, "lease", node.Linearizable)
	if elapsed := time.Since(written); elapsed < ttl-500*time.Millisecond && !got.Found {
		t.Fatalf("key expired %s after it was written with a %s ttl", elapsed, ttl)
	}
	eventually(t, 6*time.Second, "the new leader expires the key", func() bool {
		ctx, cancel := ctxFor(time.Second)
		defer cancel()
		res, err := next.Get(ctx, "lease", node.Linearizable)
		return err == nil && !res.Found
	})
	if elapsed := time.Since(written); elapsed < ttl {
		t.Fatalf("key was gone after %s, before its %s ttl", elapsed, ttl)
	}
}
