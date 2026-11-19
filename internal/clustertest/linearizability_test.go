package clustertest

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/Nirmal-22/raft-kv/internal/node"
	"github.com/Nirmal-22/raft-kv/internal/pb"
)

// Linearizability is the guarantee this store makes for writes and default
// reads: the result is the same as if every operation had happened at one
// instant somewhere between its start and its end, in a single order that all
// clients agree on.
//
// The test does not assume the implementation is right. It records what
// clients actually saw (with timestamps) while the cluster is being broken,
// then hands that history to Porcupine, which searches for an order of the
// operations that explains every result. If none exists, the store lied.

type opKind uint8

const (
	opGet opKind = iota
	opPut
	opDelete
)

type kvInput struct {
	kind  opKind
	key   string
	value string
}

type kvOutput struct {
	value string // for gets; "" means the key was absent
}

// kvModel describes a correct single-copy key-value store. Each key is an
// independent register, so the history is split by key, which keeps the
// search small.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var order []string
		for _, op := range history {
			k := op.Input.(kvInput).key
			if _, seen := byKey[k]; !seen {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(order))
		for _, k := range order {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(kvInput)
		switch in.kind {
		case opPut:
			return true, in.value
		case opDelete:
			return true, ""
		default:
			return output.(kvOutput).value == state.(string), state
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		switch in.kind {
		case opPut:
			return fmt.Sprintf("put(%s, %s)", in.key, in.value)
		case opDelete:
			return fmt.Sprintf("delete(%s)", in.key)
		default:
			return fmt.Sprintf("get(%s) -> %q", in.key, output.(kvOutput).value)
		}
	},
}

// The checker must be able to say no. This history is impossible for a
// correct store: the put finished before the get began, yet the get saw
// nothing.
func TestCheckerRejectsAStaleRead(t *testing.T) {
	history := []porcupine.Operation{
		{ClientId: 0, Input: kvInput{kind: opPut, key: "k", value: "v1"}, Call: 0, Output: kvOutput{}, Return: 10},
		{ClientId: 1, Input: kvInput{kind: opGet, key: "k"}, Call: 20, Output: kvOutput{value: ""}, Return: 30},
	}
	if porcupine.CheckOperations(kvModel, history) {
		t.Fatal("the checker accepted a read that missed a completed write")
	}

	// Overlapping operations may be ordered either way, so this one is fine.
	history[1].Call = 5
	if !porcupine.CheckOperations(kvModel, history) {
		t.Fatal("the checker rejected a read that overlapped the write")
	}
}

type recorder struct {
	mu      sync.Mutex
	start   time.Time
	ops     []porcupine.Operation
	unknown int
}

func (r *recorder) now() int64 { return time.Since(r.start).Nanoseconds() }

func (r *recorder) add(client int, in kvInput, call int64, out kvOutput, ret int64) {
	r.mu.Lock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
	r.mu.Unlock()
}

// addUnknown records a write whose outcome the client never learned (a
// timeout, a dropped connection). It might have been applied, at any time
// from the call onwards, so it is given an open-ended interval. Dropping it
// instead would hide real bugs: a later read could see its value.
func (r *recorder) addUnknown(in kvInput, call int64) {
	r.mu.Lock()
	r.unknown++
	r.ops = append(r.ops, porcupine.Operation{
		ClientId: 1000 + r.unknown, Input: in, Call: call, Output: kvOutput{}, Return: math.MaxInt64 / 2,
	})
	r.mu.Unlock()
}

// pruneUnobservedPuts removes unknown-outcome puts whose value no read ever
// returned. Every put in this test writes a unique value, so such a put cannot
// be needed to explain any result, and a history is linearizable with it
// exactly when it is linearizable without it (it can always be ordered last).
// Each open-ended operation multiplies the checker's search, so leaving these
// out is what keeps the check to a few seconds. Unknown deletes stay: the
// "absent" they produce is not unique.
func pruneUnobservedPuts(ops []porcupine.Operation) ([]porcupine.Operation, int) {
	seen := map[string]bool{}
	for _, op := range ops {
		if in := op.Input.(kvInput); in.kind == opGet {
			seen[op.Output.(kvOutput).value] = true
		}
	}
	kept := make([]porcupine.Operation, 0, len(ops))
	dropped := 0
	for _, op := range ops {
		in := op.Input.(kvInput)
		if in.kind == opPut && op.ClientId >= 1000 && !seen[in.value] {
			dropped++
			continue
		}
		kept = append(kept, op)
	}
	return kept, dropped
}

func TestLinearizableUnderPartitionsAndRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped with -short")
	}
	c := newCluster(t, 3, nil)

	const (
		clients  = 6
		duration = 6 * time.Second
	)
	keys := []string{"a", "b", "c", "d"}
	rec := &recorder{start: time.Now()}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var seq atomic.Int64

	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id) + 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Any node, including one that is cut off or about to stop.
				n := c.node(rng.Intn(len(c.cfgs)))
				if n == nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				in := kvInput{key: keys[rng.Intn(len(keys))]}
				switch p := rng.Intn(10); {
				case p < 5:
					in.kind = opGet
				case p < 9:
					in.kind = opPut
					in.value = fmt.Sprintf("c%d-%d", id, seq.Add(1))
				default:
					in.kind = opDelete
				}

				ctx, cancel := ctxFor(400 * time.Millisecond)
				call := rec.now()
				var (
					out kvOutput
					err error
				)
				switch in.kind {
				case opGet:
					var res *pb.GetResponse
					if res, err = n.Get(ctx, in.key, node.Linearizable); err == nil && res.Found {
						out.value = string(res.Value)
					}
				case opPut:
					_, err = n.Apply(ctx, putCmd(in.key, in.value))
				case opDelete:
					_, err = n.Apply(ctx, delCmd(in.key))
				}
				ret := rec.now()
				cancel()

				switch {
				case err == nil:
					rec.add(id, in, call, out, ret)
				case in.kind == opGet:
					// A failed read changed nothing and told the client nothing.
				case errors.Is(err, node.ErrNoLeader), errors.Is(err, node.ErrStopped):
					// The node guarantees these writes were never proposed.
				default:
					rec.addUnknown(in, call)
				}
				time.Sleep(time.Duration(rng.Intn(3)) * time.Millisecond)
			}
		}(id)
	}

	// The nemesis: alternately cut the leader off from the others, and stop
	// and restart a node.
	faults := 0
	deadline := time.Now().Add(duration)
	rng := rand.New(rand.NewSource(42))
	for round := 0; time.Now().Before(deadline); round++ {
		time.Sleep(300 * time.Millisecond)
		if round%2 == 0 {
			if i := c.leaderIndex(); i >= 0 {
				n := c.node(i)
				n.SetIsolated(true)
				time.Sleep(500 * time.Millisecond)
				n.SetIsolated(false)
				faults++
			}
		} else {
			i := rng.Intn(len(c.cfgs))
			c.stop(i)
			time.Sleep(400 * time.Millisecond)
			c.start(i)
			faults++
		}
	}
	close(stop)
	wg.Wait()

	completed := len(rec.ops) - rec.unknown
	history, dropped := pruneUnobservedPuts(rec.ops)
	t.Logf("%d operations completed, %d faults injected; %d writes had an unknown outcome, of which %d were never observed and are left out",
		completed, faults, rec.unknown, dropped)
	if completed < 200 {
		t.Fatalf("only %d operations completed; the cluster was unavailable for most of the test", completed)
	}

	result, info := porcupine.CheckOperationsVerbose(kvModel, history, 60*time.Second)
	switch result {
	case porcupine.Ok:
	case porcupine.Unknown:
		t.Fatalf("the checker ran out of time on %d operations; the history is unverified", len(history))
	default:
		path := "linearizability-violation.html"
		if err := porcupine.VisualizePath(kvModel, info, path); err == nil {
			t.Logf("wrote a visualisation of the violation to %s", path)
		}
		t.Fatal("history is NOT linearizable: some client observed a result no single order of operations can explain")
	}
}
