package store

import (
	"bytes"
	"fmt"
	"runtime"
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

// List needs versions, not values. It used to copy every value to get at the
// 16-byte header in front of it, so listing large values cost their full size.
func TestListDoesNotCopyValues(t *testing.T) {
	s := newStore(t)
	const keys, valueSize = 40, 1 << 20
	big := string(make([]byte, valueSize))
	for i := 0; i < keys; i++ {
		apply(t, s, uint64(i+1), put(fmt.Sprintf("big/%02d", i), big))
	}

	// Flush first: the bug only shows once values are in files, where reading
	// a value's header means loading the whole value (42 MiB per List here,
	// before the metadata index). The counter is process-wide and Pebble works
	// in the background, so take the smallest of several runs.
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	smallest := ^uint64(0)
	for run := 0; run < 5; run++ {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		infos, err := s.List("big/", 0)
		runtime.ReadMemStats(&after)
		if err != nil || len(infos) != keys {
			t.Fatalf("list: %d keys, err %v", len(infos), err)
		}
		if infos[3].Version != 4 {
			t.Fatalf("version = %d, want 4", infos[3].Version)
		}
		if d := after.TotalAlloc - before.TotalAlloc; d < smallest {
			smallest = d
		}
	}
	if smallest > 4<<20 {
		t.Fatalf("listing %d keys holding %d MiB allocated at least %d MiB; values are being copied",
			keys, keys*valueSize>>20, smallest>>20)
	}
}

func TestExpireIsBounded(t *testing.T) {
	s := newStore(t)
	const due = 2500
	for i := 0; i < due; i++ {
		apply(t, s, uint64(i+1), putTTL(fmt.Sprintf("k%04d", i), "v", int64(1000+i)))
	}
	apply(t, s, due+1, putTTL("later", "v", 900000))

	bounded := func(index uint64) *pb.ApplyResult {
		return apply(t, s, index, &pb.Command{Op: &pb.Command_Expire{Expire: &pb.Expire{NowMs: 500000, MaxKeys: 1000}}})
	}
	for i, want := range []uint32{1000, 1000, 500, 0} {
		if res := bounded(uint64(due + 2 + i)); res.Expired != want {
			t.Fatalf("expire command %d removed %d keys, want %d", i+1, res.Expired, want)
		}
		if i == 0 {
			// Oldest first: the first 1000 are gone, the rest are still due.
			if _, ok := mustGet(t, s, "k0999"); ok {
				t.Fatal("k0999 should have expired in the first batch")
			}
			if _, ok := mustGet(t, s, "k1000"); !ok {
				t.Fatal("k1000 expired in the first batch; the limit was ignored")
			}
		}
	}
	if next, _ := s.NextExpiry(); next != 900000 {
		t.Fatalf("next expiry = %d, want only the later key left", next)
	}
}

// Loading a snapshot changes the store without any per-key events, so a
// watcher must be cut off instead of silently missing those changes.
func TestRestoreCutsOffWatchers(t *testing.T) {
	src := newStore(t)
	apply(t, src, 1, put("a", "1"))
	snap := src.Snapshot()
	var buf bytes.Buffer
	if _, err := snap.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	snap.Close()

	dst := newStore(t)
	events, cancel := dst.Watch("")
	defer cancel()
	if err := dst.Restore(&buf); err != nil {
		t.Fatal(err)
	}
	if _, open := <-events; open {
		t.Fatal("watcher is still subscribed after a snapshot replaced the store")
	}

	// The hub still works for watchers that subscribe afterwards.
	again, cancelAgain := dst.Watch("")
	defer cancelAgain()
	apply(t, dst, 2, put("b", "2"))
	if ev := <-again; ev.Key != "b" {
		t.Fatalf("event after restore: %+v", ev)
	}
}

// The metadata index must change with the data in every code path, or List
// and compare-and-swap would disagree with Get.
func TestMetadataIndexFollowsData(t *testing.T) {
	s := newStore(t)
	apply(t, s, 1, put("a", "1"))
	apply(t, s, 2, putTTL("b", "2", 1000))
	apply(t, s, 3, put("c", "3"))
	apply(t, s, 4, put("a", "1b")) // overwrite: version moves
	apply(t, s, 5, del("c"))       // delete
	apply(t, s, 6, expire(2000))   // expire b

	infos, err := s.List("", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Key != "a" || infos[0].Version != 4 {
		t.Fatalf("list after overwrite, delete and expire: %v", infos)
	}
	// A swap against the listed version must succeed, and create-only must
	// work again for the deleted and the expired key.
	if res := apply(t, s, 7, cas("a", "1c", infos[0].Version)); res.Status != pb.Status_STATUS_OK {
		t.Fatalf("swap with the listed version: %v", res)
	}
	for i, key := range []string{"b", "c"} {
		if res := apply(t, s, uint64(8+i), cas(key, "new", 0)); res.Status != pb.Status_STATUS_OK {
			t.Fatalf("create-only on removed key %s: %v", key, res)
		}
	}

	// And it survives a snapshot.
	snap := s.Snapshot()
	var buf bytes.Buffer
	if _, err := snap.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	snap.Close()
	dst := newStore(t)
	if err := dst.Restore(&buf); err != nil {
		t.Fatal(err)
	}
	restored, _ := dst.List("", 0)
	if len(restored) != 3 || restored[0].Version != 7 {
		t.Fatalf("list after restore: %v", restored)
	}
}
