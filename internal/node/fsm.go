package node

import (
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	"github.com/Nirmal-22/raft-kv/internal/metrics"
	"github.com/Nirmal-22/raft-kv/internal/pb"
	"github.com/Nirmal-22/raft-kv/internal/store"
)

// fsm connects Raft to the store. Raft calls Apply once an entry is committed
// (stored on a majority of nodes), in log order, on every node.
type fsm struct {
	store   *store.Store
	metrics *metrics.Metrics
}

var _ raft.FSM = (*fsm)(nil)

func (f *fsm) Apply(l *raft.Log) interface{} {
	start := time.Now()
	cmd := &pb.Command{}
	if err := proto.Unmarshal(l.Data, cmd); err != nil {
		// Every node sees the same bytes, so every node rejects it the same way.
		return fmt.Errorf("decode command at index %d: %w", l.Index, err)
	}
	res, err := f.store.Apply(l.Index, cmd)
	if err != nil {
		// A storage failure here means this node can no longer follow the log.
		// Skipping the entry would silently diverge from the other replicas,
		// so stop. On restart Raft replays from the last applied index.
		panic(fmt.Sprintf("apply index %d: %v", l.Index, err))
	}
	f.metrics.ApplyDuration.Observe(time.Since(start).Seconds())
	if res.Expired > 0 {
		f.metrics.KeysExpired.Add(float64(res.Expired))
	}
	return res
}

// Snapshot is called between two Apply calls, so the view it pins is a clean
// cut of the log. It must return quickly; the slow part happens in Persist.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{snap: f.store.Snapshot()}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.store.Restore(rc)
}

type fsmSnapshot struct{ snap *store.Snapshot }

// Persist runs while new commands keep being applied. That is safe because the
// Pebble snapshot is frozen.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := s.snap.WriteTo(sink); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() { s.snap.Close() }
