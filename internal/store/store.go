// Package store is the replicated state machine: a key-value map kept in
// Pebble (an LSM-tree storage engine). It knows nothing about Raft. It is
// handed commands with their log index and must produce the same state on
// every node.
package store

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"google.golang.org/protobuf/proto"

	"github.com/Nirmal-22/raft-kv/internal/pb"
)

// Key layout inside Pebble. One byte of prefix keeps the four kinds of record
// apart and lets each be scanned on its own.
const (
	prefixData   = 'd' // d<key>            -> version | expiresAt | value
	prefixExpiry = 'e' // e<expiresAt><key> -> empty; index of keys by expiry time
	prefixMember = 'm' // m<node id>        -> pb.Member
	prefixSystem = 's' // s<name>           -> internal bookkeeping
)

var appliedKey = []byte{prefixSystem, 'a', 'p', 'p', 'l', 'i', 'e', 'd'}

const valueHeader = 16 // 8 bytes version + 8 bytes expiresAt

// Entry is what a read returns for one key.
type Entry struct {
	Value       []byte
	Version     uint64
	ExpiresAtMs int64
}

// Store is safe for concurrent use. Writes arrive one at a time from the Raft
// apply loop; reads can come from any goroutine.
type Store struct {
	db      *pebble.DB
	applied atomic.Uint64
	hub     *Hub
	// restoreMu keeps readers out while a snapshot replaces the whole keyspace.
	restoreMu sync.RWMutex
}

// Open opens (or creates) a store in dir.
func Open(dir string) (*Store, error) {
	return open(dir, &pebble.Options{Logger: quietLogger{}})
}

// OpenMem opens a store that lives only in memory. Used by tests.
func OpenMem() (*Store, error) {
	return open("", &pebble.Options{FS: vfs.NewMem(), Logger: quietLogger{}})
}

func open(dir string, opts *pebble.Options) (*Store, error) {
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}
	s := &Store{db: db, hub: newHub()}
	idx, err := s.readApplied()
	if err != nil {
		db.Close()
		return nil, err
	}
	s.applied.Store(idx)
	return s, nil
}

func (s *Store) Close() error {
	s.hub.closeAll()
	return s.db.Close()
}

// AppliedIndex is the log index of the last command reflected in the store.
func (s *Store) AppliedIndex() uint64 { return s.applied.Load() }

// Watch subscribes to changes of keys that start with prefix.
func (s *Store) Watch(prefix string) (<-chan Event, func()) { return s.hub.subscribe(prefix) }

// Apply executes one command from the Raft log.
//
// The data change and the new applied index are written in a single Pebble
// batch, so they land together or not at all. After a restart Raft replays
// log entries the store may already contain; anything at or below the applied
// index is skipped. Without that check a replayed compare-and-swap would be
// judged against newer state and the replicas could diverge.
//
// Because of that guarantee the batch does not need an fsync: if the machine
// dies, the store loses a suffix of commands and the Raft log (which is
// fsynced) replays exactly that suffix.
func (s *Store) Apply(index uint64, cmd *pb.Command) (*pb.ApplyResult, error) {
	if index <= s.applied.Load() {
		return &pb.ApplyResult{Status: pb.Status_STATUS_UNSPECIFIED}, nil
	}
	batch := s.db.NewBatch()
	defer batch.Close()

	var (
		res    *pb.ApplyResult
		events []Event
		err    error
	)
	switch op := cmd.Op.(type) {
	case *pb.Command_Put:
		res, events, err = s.applyPut(batch, index, op.Put)
	case *pb.Command_Delete:
		res, events, err = s.applyDelete(batch, op.Delete)
	case *pb.Command_Expire:
		res, events, err = s.applyExpire(batch, op.Expire)
	case *pb.Command_SetMember:
		res, err = s.applySetMember(batch, op.SetMember)
	case *pb.Command_RemoveMember:
		res = &pb.ApplyResult{Status: pb.Status_STATUS_OK}
		err = batch.Delete(memberKey(op.RemoveMember.Id), nil)
	default:
		// An unknown command still has to advance the applied index, or the
		// node would try it again forever.
		res = &pb.ApplyResult{Status: pb.Status_STATUS_UNSPECIFIED}
	}
	if err != nil {
		return nil, err
	}

	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], index)
	if err := batch.Set(appliedKey, idx[:], nil); err != nil {
		return nil, err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return nil, fmt.Errorf("commit batch at index %d: %w", index, err)
	}
	s.applied.Store(index)
	for _, ev := range events {
		s.hub.publish(ev)
	}
	return res, nil
}

func (s *Store) applyPut(b *pebble.Batch, index uint64, p *pb.Put) (*pb.ApplyResult, []Event, error) {
	cur, found, err := s.get(p.Key)
	if err != nil {
		return nil, nil, err
	}
	if p.IfVersion != nil {
		var have uint64
		if found {
			have = cur.Version
		}
		if have != *p.IfVersion {
			return &pb.ApplyResult{Status: pb.Status_STATUS_VERSION_MISMATCH, CurrentVersion: have}, nil, nil
		}
	}
	if found && cur.ExpiresAtMs != 0 {
		if err := b.Delete(expiryKey(cur.ExpiresAtMs, p.Key), nil); err != nil {
			return nil, nil, err
		}
	}
	if err := b.Set(dataKey(p.Key), encodeValue(index, p.ExpiresAtMs, p.Value), nil); err != nil {
		return nil, nil, err
	}
	if p.ExpiresAtMs != 0 {
		if err := b.Set(expiryKey(p.ExpiresAtMs, p.Key), nil, nil); err != nil {
			return nil, nil, err
		}
	}
	ev := Event{Type: EventPut, Key: p.Key, Value: p.Value, Version: index}
	return &pb.ApplyResult{Status: pb.Status_STATUS_OK, Version: index}, []Event{ev}, nil
}

func (s *Store) applyDelete(b *pebble.Batch, d *pb.Delete) (*pb.ApplyResult, []Event, error) {
	cur, found, err := s.get(d.Key)
	if err != nil {
		return nil, nil, err
	}
	if d.IfVersion != nil {
		var have uint64
		if found {
			have = cur.Version
		}
		if have != *d.IfVersion {
			return &pb.ApplyResult{Status: pb.Status_STATUS_VERSION_MISMATCH, CurrentVersion: have}, nil, nil
		}
	}
	if !found {
		return &pb.ApplyResult{Status: pb.Status_STATUS_NOT_FOUND}, nil, nil
	}
	if err := b.Delete(dataKey(d.Key), nil); err != nil {
		return nil, nil, err
	}
	if cur.ExpiresAtMs != 0 {
		if err := b.Delete(expiryKey(cur.ExpiresAtMs, d.Key), nil); err != nil {
			return nil, nil, err
		}
	}
	ev := Event{Type: EventDelete, Key: d.Key, Version: cur.Version}
	return &pb.ApplyResult{Status: pb.Status_STATUS_OK, Version: cur.Version}, []Event{ev}, nil
}

// applyExpire walks the expiry index up to now and removes those keys. The
// index is sorted by time, so this touches only keys that are actually due.
func (s *Store) applyExpire(b *pebble.Batch, e *pb.Expire) (*pb.ApplyResult, []Event, error) {
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefixExpiry},
		UpperBound: expiryKey(e.NowMs+1, ""),
	})
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()

	var events []Event
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) < 9 {
			continue
		}
		ts := int64(binary.BigEndian.Uint64(k[1:9]))
		key := string(k[9:])
		if err := b.Delete(append([]byte(nil), k...), nil); err != nil {
			return nil, nil, err
		}
		cur, found, err := s.get(key)
		if err != nil {
			return nil, nil, err
		}
		if !found || cur.ExpiresAtMs != ts {
			continue // the key was rewritten since; this index entry is stale
		}
		if err := b.Delete(dataKey(key), nil); err != nil {
			return nil, nil, err
		}
		events = append(events, Event{Type: EventExpire, Key: key, Version: cur.Version})
	}
	if err := it.Error(); err != nil {
		return nil, nil, err
	}
	return &pb.ApplyResult{Status: pb.Status_STATUS_OK, Expired: uint32(len(events))}, events, nil
}

func (s *Store) applySetMember(b *pebble.Batch, m *pb.SetMember) (*pb.ApplyResult, error) {
	if m.Member == nil || m.Member.Id == "" {
		return &pb.ApplyResult{Status: pb.Status_STATUS_UNSPECIFIED}, nil
	}
	raw, err := proto.Marshal(m.Member)
	if err != nil {
		return nil, err
	}
	return &pb.ApplyResult{Status: pb.Status_STATUS_OK}, b.Set(memberKey(m.Member.Id), raw, nil)
}

// Get reads one key from this node's copy of the data.
func (s *Store) Get(key string) (Entry, bool, error) {
	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()
	return s.get(key)
}

func (s *Store) get(key string) (Entry, bool, error) {
	raw, closer, err := s.db.Get(dataKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, err
	}
	defer closer.Close()
	e, err := decodeValue(raw)
	return e, err == nil, err
}

// List returns keys that start with prefix, in order, without their values.
func (s *Store) List(prefix string, limit int) ([]*pb.KeyInfo, error) {
	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()
	lower := dataKey(prefix)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: prefixEnd(lower)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []*pb.KeyInfo
	for ok := it.First(); ok && (limit <= 0 || len(out) < limit); ok = it.Next() {
		e, err := decodeValue(it.Value())
		if err != nil {
			return nil, err
		}
		out = append(out, &pb.KeyInfo{Key: string(it.Key()[1:]), Version: e.Version, ExpiresAtMs: e.ExpiresAtMs})
	}
	return out, it.Error()
}

// NextExpiry returns the earliest expiry time of any key, or 0 if none expire.
func (s *Store) NextExpiry() (int64, error) {
	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefixExpiry},
		UpperBound: []byte{prefixExpiry + 1},
	})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	if !it.First() || len(it.Key()) < 9 {
		return 0, it.Error()
	}
	return int64(binary.BigEndian.Uint64(it.Key()[1:9])), nil
}

// Members returns every node recorded in the replicated state.
func (s *Store) Members() ([]*pb.Member, error) {
	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefixMember},
		UpperBound: []byte{prefixMember + 1},
	})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []*pb.Member
	for ok := it.First(); ok; ok = it.Next() {
		m := &pb.Member{}
		if err := proto.Unmarshal(it.Value(), m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, it.Error()
}

// Member looks up one node by id.
func (s *Store) Member(id string) (*pb.Member, bool, error) {
	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()
	raw, closer, err := s.db.Get(memberKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	m := &pb.Member{}
	if err := proto.Unmarshal(raw, m); err != nil {
		return nil, false, err
	}
	return m, true, nil
}

// ---------------------------------------------------------------- snapshots

// Snapshot is a frozen, consistent view of the whole store. Taking one is
// cheap (Pebble pins the current files); writing it out can take as long as
// it likes while new commands keep being applied.
type Snapshot struct{ snap *pebble.Snapshot }

func (s *Store) Snapshot() *Snapshot { return &Snapshot{snap: s.db.NewSnapshot()} }

func (sn *Snapshot) Close() error { return sn.snap.Close() }

// WriteTo streams every record as: key length, key, value length, value.
// The applied index is one of the records, so a restore knows where it is.
func (sn *Snapshot) WriteTo(w io.Writer) (int64, error) {
	it, err := sn.snap.NewIter(&pebble.IterOptions{})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	bw := bufio.NewWriterSize(w, 1<<16)
	var (
		n   int64
		buf [binary.MaxVarintLen64]byte
	)
	put := func(p []byte) error {
		l := binary.PutUvarint(buf[:], uint64(len(p)))
		if _, err := bw.Write(buf[:l]); err != nil {
			return err
		}
		_, err := bw.Write(p)
		n += int64(l + len(p))
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		if err := put(it.Key()); err != nil {
			return n, err
		}
		if err := put(it.Value()); err != nil {
			return n, err
		}
	}
	if err := it.Error(); err != nil {
		return n, err
	}
	return n, bw.Flush()
}

// Restore replaces the entire contents of the store with a snapshot.
func (s *Store) Restore(r io.Reader) error {
	s.restoreMu.Lock()
	defer s.restoreMu.Unlock()

	if err := s.db.DeleteRange([]byte{0x00}, []byte{0xff}, pebble.NoSync); err != nil {
		return err
	}
	br := bufio.NewReaderSize(r, 1<<16)
	batch := s.db.NewBatch()
	readChunk := func() ([]byte, error) {
		l, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, err
		}
		p := make([]byte, l)
		_, err = io.ReadFull(br, p)
		return p, err
	}
	for {
		k, err := readChunk()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			batch.Close()
			return fmt.Errorf("read snapshot key: %w", err)
		}
		v, err := readChunk()
		if err != nil {
			batch.Close()
			return fmt.Errorf("read snapshot value: %w", err)
		}
		if err := batch.Set(k, v, nil); err != nil {
			batch.Close()
			return err
		}
		if batch.Len() > 4<<20 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				return err
			}
			batch = s.db.NewBatch()
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	idx, err := s.readApplied()
	if err != nil {
		return err
	}
	s.applied.Store(idx)
	return nil
}

func (s *Store) readApplied() (uint64, error) {
	raw, closer, err := s.db.Get(appliedKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(raw) != 8 {
		return 0, fmt.Errorf("corrupt applied index (%d bytes)", len(raw))
	}
	return binary.BigEndian.Uint64(raw), nil
}

// ----------------------------------------------------------------- encoding

func dataKey(key string) []byte {
	return append([]byte{prefixData}, key...)
}

func memberKey(id string) []byte {
	return append([]byte{prefixMember}, id...)
}

// expiryKey sorts by time first (big-endian), then by key.
func expiryKey(expiresAtMs int64, key string) []byte {
	b := make([]byte, 9, 9+len(key))
	b[0] = prefixExpiry
	binary.BigEndian.PutUint64(b[1:], uint64(expiresAtMs))
	return append(b, key...)
}

// prefixEnd returns the smallest key that is greater than every key starting
// with p, for use as an exclusive upper bound.
func prefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // p is all 0xff: no upper bound
}

func encodeValue(version uint64, expiresAtMs int64, value []byte) []byte {
	b := make([]byte, valueHeader+len(value))
	binary.BigEndian.PutUint64(b[0:8], version)
	binary.BigEndian.PutUint64(b[8:16], uint64(expiresAtMs))
	copy(b[valueHeader:], value)
	return b
}

func decodeValue(raw []byte) (Entry, error) {
	if len(raw) < valueHeader {
		return Entry{}, fmt.Errorf("corrupt value (%d bytes)", len(raw))
	}
	return Entry{
		Version:     binary.BigEndian.Uint64(raw[0:8]),
		ExpiresAtMs: int64(binary.BigEndian.Uint64(raw[8:16])),
		Value:       append([]byte(nil), raw[valueHeader:]...),
	}, nil
}

// quietLogger drops Pebble's informational logging but keeps fatal errors.
type quietLogger struct{}

func (quietLogger) Infof(string, ...interface{})  {}
func (quietLogger) Errorf(string, ...interface{}) {}
func (quietLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}
