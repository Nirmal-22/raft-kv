package node

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

var errIsolated = errors.New("node is isolated from the network")

// faultNet lets a node be cut off from its peers on demand, to imitate a
// network partition. Raft traffic goes through streamLayer below; the gRPC
// client and server check Isolated() themselves.
//
// It exists for the partition tests and the optional chaos endpoint. In normal
// operation it is never switched on and only costs an atomic load per dial.
type faultNet struct {
	isolated atomic.Bool
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
}

func newFaultNet() *faultNet { return &faultNet{conns: make(map[net.Conn]struct{})} }

func (f *faultNet) Isolated() bool { return f.isolated.Load() }

// SetIsolated cuts the node off (true) or reconnects it (false). Cutting off
// also closes every open Raft connection, because Raft keeps connections
// pooled and would otherwise keep talking over them.
func (f *faultNet) SetIsolated(v bool) {
	f.mu.Lock()
	f.isolated.Store(v)
	var conns []net.Conn
	if v {
		for c := range f.conns {
			conns = append(conns, c)
		}
	}
	f.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// track registers a connection so SetIsolated can close it. The isolation
// flag is re-checked under the same lock SetIsolated holds: a dial that was
// already in flight when the node was cut off must not produce a live
// connection afterwards. (An earlier version checked only before dialing; now
// and then one connection slipped through and kept a "partitioned" leader in
// touch with a follower, so it never lost its majority.)
func (f *faultNet) track(c net.Conn) (net.Conn, error) {
	tc := &trackedConn{Conn: c, f: f}
	f.mu.Lock()
	if f.isolated.Load() {
		f.mu.Unlock()
		c.Close()
		return nil, errIsolated
	}
	f.conns[tc] = struct{}{}
	f.mu.Unlock()
	return tc, nil
}

type trackedConn struct {
	net.Conn
	f    *faultNet
	once sync.Once
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.f.mu.Lock()
		delete(c.f.conns, c)
		c.f.mu.Unlock()
	})
	return c.Conn.Close()
}

// streamLayer is the TCP transport Raft uses, with the isolation switch wired
// in: an isolated node refuses to dial and drops whatever it accepts.
type streamLayer struct {
	net.Listener
	f *faultNet
}

var _ raft.StreamLayer = (*streamLayer)(nil)

func (s *streamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	if s.f.Isolated() {
		return nil, errIsolated
	}
	c, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, err
	}
	return s.f.track(c)
}

func (s *streamLayer) Accept() (net.Conn, error) {
	for {
		c, err := s.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if tc, err := s.f.track(c); err == nil {
			return tc, nil
		}
		// Isolated: the connection was closed. Keep accepting (and dropping)
		// so the listener stays usable once the node is reconnected.
	}
}
