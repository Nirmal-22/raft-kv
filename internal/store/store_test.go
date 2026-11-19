package store

import (
	"bytes"
	"testing"

	"github.com/Nirmal-22/raft-kv/internal/pb"
)

func put(key, value string) *pb.Command {
	return &pb.Command{Op: &pb.Command_Put{Put: &pb.Put{Key: key, Value: []byte(value)}}}
}

func putTTL(key, value string, expiresAt int64) *pb.Command {
	return &pb.Command{Op: &pb.Command_Put{Put: &pb.Put{Key: key, Value: []byte(value), ExpiresAtMs: expiresAt}}}
}

func cas(key, value string, ifVersion uint64) *pb.Command {
	return &pb.Command{Op: &pb.Command_Put{Put: &pb.Put{Key: key, Value: []byte(value), IfVersion: &ifVersion}}}
}

func del(key string) *pb.Command {
	return &pb.Command{Op: &pb.Command_Delete{Delete: &pb.Delete{Key: key}}}
}

func expire(now int64) *pb.Command {
	return &pb.Command{Op: &pb.Command_Expire{Expire: &pb.Expire{NowMs: now}}}
}

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenMem()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func apply(t *testing.T, s *Store, index uint64, cmd *pb.Command) *pb.ApplyResult {
	t.Helper()
	res, err := s.Apply(index, cmd)
	if err != nil {
		t.Fatalf("apply %d: %v", index, err)
	}
	return res
}

func mustGet(t *testing.T, s *Store, key string) (Entry, bool) {
	t.Helper()
	e, ok, err := s.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	return e, ok
}

func TestPutGetDelete(t *testing.T) {
	s := newStore(t)

	res := apply(t, s, 1, put("a", "one"))
	if res.Status != pb.Status_STATUS_OK || res.Version != 1 {
		t.Fatalf("put: %v", res)
	}
	apply(t, s, 2, put("a", "two"))

	e, ok := mustGet(t, s, "a")
	if !ok || string(e.Value) != "two" || e.Version != 2 {
		t.Fatalf("get after overwrite: %+v ok=%v", e, ok)
	}
	if s.AppliedIndex() != 2 {
		t.Fatalf("applied index = %d, want 2", s.AppliedIndex())
	}

	if res := apply(t, s, 3, del("a")); res.Status != pb.Status_STATUS_OK {
		t.Fatalf("delete: %v", res)
	}
	if _, ok := mustGet(t, s, "a"); ok {
		t.Fatal("key still present after delete")
	}
	if res := apply(t, s, 4, del("a")); res.Status != pb.Status_STATUS_NOT_FOUND {
		t.Fatalf("second delete: %v", res)
	}
}

func TestCompareAndSwap(t *testing.T) {
	s := newStore(t)

	// if_version=0 means "create only".
	if res := apply(t, s, 1, cas("k", "v1", 0)); res.Status != pb.Status_STATUS_OK {
		t.Fatalf("create: %v", res)
	}
	res := apply(t, s, 2, cas("k", "again", 0))
	if res.Status != pb.Status_STATUS_VERSION_MISMATCH || res.CurrentVersion != 1 {
		t.Fatalf("second create should conflict: %v", res)
	}
	if res := apply(t, s, 3, cas("k", "v2", 1)); res.Status != pb.Status_STATUS_OK || res.Version != 3 {
		t.Fatalf("swap with right version: %v", res)
	}
	if res := apply(t, s, 4, cas("k", "v3", 1)); res.Status != pb.Status_STATUS_VERSION_MISMATCH {
		t.Fatalf("swap with stale version should conflict: %v", res)
	}
	e, _ := mustGet(t, s, "k")
	if string(e.Value) != "v2" {
		t.Fatalf("value = %q, want v2", e.Value)
	}
}

// A restart replays log entries the store already holds. They must be ignored,
// otherwise the replayed compare-and-swap below would succeed a second time.
func TestReplayedEntriesAreSkipped(t *testing.T) {
	s := newStore(t)
	apply(t, s, 1, put("k", "v1"))
	apply(t, s, 2, cas("k", "v2", 1))
	apply(t, s, 3, put("k", "v3"))

	apply(t, s, 1, put("k", "v1"))
	apply(t, s, 2, cas("k", "v2", 1))

	e, _ := mustGet(t, s, "k")
	if string(e.Value) != "v3" || e.Version != 3 {
		t.Fatalf("replay changed state: %+v", e)
	}
	if s.AppliedIndex() != 3 {
		t.Fatalf("applied index = %d, want 3", s.AppliedIndex())
	}
}

func TestExpire(t *testing.T) {
	s := newStore(t)
	apply(t, s, 1, putTTL("short", "x", 1000))
	apply(t, s, 2, putTTL("long", "x", 5000))
	apply(t, s, 3, put("forever", "x"))
	apply(t, s, 4, putTTL("renewed", "x", 1000))
	apply(t, s, 5, putTTL("renewed", "y", 9000)) // pushed back before it expired

	if next, _ := s.NextExpiry(); next != 1000 {
		t.Fatalf("next expiry = %d, want 1000", next)
	}

	// Reads never look at the clock: the key is there until Expire is applied.
	if _, ok := mustGet(t, s, "short"); !ok {
		t.Fatal("key vanished before an Expire command")
	}

	res := apply(t, s, 6, expire(2000))
	if res.Expired != 1 {
		t.Fatalf("expired %d keys, want 1", res.Expired)
	}
	for key, want := range map[string]bool{"short": false, "long": true, "forever": true, "renewed": true} {
		if _, ok := mustGet(t, s, key); ok != want {
			t.Errorf("%s present=%v, want %v", key, ok, want)
		}
	}
	if next, _ := s.NextExpiry(); next != 5000 {
		t.Fatalf("next expiry = %d, want 5000", next)
	}

	// Removing the TTL also removes the key from the expiry index.
	apply(t, s, 7, put("long", "kept"))
	apply(t, s, 8, expire(6000))
	if e, ok := mustGet(t, s, "long"); !ok || string(e.Value) != "kept" {
		t.Fatalf("key with TTL removed was expired: %+v ok=%v", e, ok)
	}
}

func TestListByPrefix(t *testing.T) {
	s := newStore(t)
	for i, k := range []string{"user/1", "user/2", "order/1", "user/3"} {
		apply(t, s, uint64(i+1), put(k, "v"))
	}
	keys, err := s.List("user/", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 || keys[0].Key != "user/1" || keys[2].Key != "user/3" {
		t.Fatalf("list user/: %v", keys)
	}
	if keys, _ := s.List("user/", 2); len(keys) != 2 {
		t.Fatalf("limit ignored: %d keys", len(keys))
	}
	if keys, _ := s.List("", 0); len(keys) != 4 {
		t.Fatalf("list all: %d keys", len(keys))
	}
}

func TestSnapshotRestore(t *testing.T) {
	src := newStore(t)
	apply(t, src, 1, put("a", "1"))
	apply(t, src, 2, putTTL("b", "2", 4000))
	apply(t, src, 3, &pb.Command{Op: &pb.Command_SetMember{SetMember: &pb.SetMember{
		Member: &pb.Member{Id: "n1", GrpcAddr: "127.0.0.1:1"},
	}}})

	snap := src.Snapshot()
	apply(t, src, 4, put("a", "changed after snapshot"))

	var buf bytes.Buffer
	if _, err := snap.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	snap.Close()

	dst := newStore(t)
	apply(t, dst, 1, put("stale", "must disappear"))
	if err := dst.Restore(&buf); err != nil {
		t.Fatal(err)
	}

	if dst.AppliedIndex() != 3 {
		t.Fatalf("applied index after restore = %d, want 3", dst.AppliedIndex())
	}
	if e, ok := mustGet(t, dst, "a"); !ok || string(e.Value) != "1" {
		t.Fatalf("a = %+v ok=%v, want the value at snapshot time", e, ok)
	}
	if _, ok := mustGet(t, dst, "stale"); ok {
		t.Fatal("restore kept a key that was not in the snapshot")
	}
	if next, _ := dst.NextExpiry(); next != 4000 {
		t.Fatalf("expiry index not restored: next = %d", next)
	}
	if m, ok, _ := dst.Member("n1"); !ok || m.GrpcAddr != "127.0.0.1:1" {
		t.Fatalf("member not restored: %v", m)
	}
}

func TestWatch(t *testing.T) {
	s := newStore(t)
	events, cancel := s.Watch("user/")
	defer cancel()

	apply(t, s, 1, put("order/1", "ignored"))
	apply(t, s, 2, put("user/1", "alice"))
	apply(t, s, 3, del("user/1"))

	first, second := <-events, <-events
	if first.Type != EventPut || first.Key != "user/1" || string(first.Value) != "alice" || first.Version != 2 {
		t.Fatalf("first event: %+v", first)
	}
	if second.Type != EventDelete || second.Key != "user/1" {
		t.Fatalf("second event: %+v", second)
	}
}

func TestSlowWatcherIsDropped(t *testing.T) {
	s := newStore(t)
	events, cancel := s.Watch("")
	defer cancel()

	for i := 1; i <= watchBuffer+10; i++ {
		apply(t, s, uint64(i), put("k", "v"))
	}
	n := 0
	for range events { // the channel must be closed, or this never returns
		n++
	}
	if n != watchBuffer {
		t.Fatalf("got %d buffered events, want %d", n, watchBuffer)
	}
}
