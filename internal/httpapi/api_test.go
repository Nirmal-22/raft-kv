package httpapi

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Nirmal-22/raft-kv/internal/node"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// newServer starts a one-node cluster behind the real HTTP handler.
func newServer(t *testing.T, opts Options) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	n, err := node.New(node.Config{
		ID:                 "n1",
		DataDir:            filepath.Join(t.TempDir(), "n1"),
		RaftAddr:           freeAddr(t),
		GRPCAddr:           freeAddr(t),
		HTTPAddr:           "127.0.0.1:0",
		Bootstrap:          true,
		HeartbeatTimeout:   50 * time.Millisecond,
		ElectionTimeout:    50 * time.Millisecond,
		LeaderLeaseTimeout: 50 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
		TickInterval:       20 * time.Millisecond,
		LogLevel:           "off",
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(n, opts))
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
		n.Shutdown()
	})
	deadline := time.Now().Add(10 * time.Second)
	for !n.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("single node never became leader")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return srv
}

type response struct {
	code   int
	body   string
	header http.Header
}

func (r response) json(t *testing.T) map[string]any {
	t.Helper()
	out := map[string]any{}
	if err := json.Unmarshal([]byte(r.body), &out); err != nil {
		t.Fatalf("response is not JSON: %q", r.body)
	}
	return out
}

func do(t *testing.T, srv *httptest.Server, method, path, body string) response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return response{code: res.StatusCode, body: string(raw), header: res.Header}
}

func TestPutGetDelete(t *testing.T) {
	srv := newServer(t, Options{})

	put := do(t, srv, http.MethodPut, "/v1/kv/users/42", "alice")
	if put.code != http.StatusOK {
		t.Fatalf("put: %d %s", put.code, put.body)
	}
	version := fmt.Sprint(put.json(t)["version"])

	get := do(t, srv, http.MethodGet, "/v1/kv/users/42", "")
	if get.code != http.StatusOK || get.body != "alice" {
		t.Fatalf("get: %d %q", get.code, get.body)
	}
	if got := get.header.Get("X-Kv-Version"); got != version {
		t.Fatalf("X-Kv-Version = %q, want %q", got, version)
	}
	if stale := do(t, srv, http.MethodGet, "/v1/kv/users/42?consistency=stale", ""); stale.body != "alice" {
		t.Fatalf("stale get: %d %q", stale.code, stale.body)
	}

	if del := do(t, srv, http.MethodDelete, "/v1/kv/users/42", ""); del.code != http.StatusOK {
		t.Fatalf("delete: %d %s", del.code, del.body)
	}
	if again := do(t, srv, http.MethodGet, "/v1/kv/users/42", ""); again.code != http.StatusNotFound {
		t.Fatalf("get after delete: %d", again.code)
	}
	if again := do(t, srv, http.MethodDelete, "/v1/kv/users/42", ""); again.code != http.StatusNotFound {
		t.Fatalf("second delete: %d", again.code)
	}
}

func TestCompareAndSwapReturns409(t *testing.T) {
	srv := newServer(t, Options{})

	if r := do(t, srv, http.MethodPut, "/v1/kv/lock?if_version=0", "owner-a"); r.code != http.StatusOK {
		t.Fatalf("create-only put: %d %s", r.code, r.body)
	}
	taken := do(t, srv, http.MethodPut, "/v1/kv/lock?if_version=0", "owner-b")
	if taken.code != http.StatusConflict {
		t.Fatalf("second create-only put: %d, want 409", taken.code)
	}
	current := fmt.Sprint(taken.json(t)["current_version"])

	if r := do(t, srv, http.MethodPut, "/v1/kv/lock?if_version="+current, "owner-a-renewed"); r.code != http.StatusOK {
		t.Fatalf("swap with the reported version: %d %s", r.code, r.body)
	}
	if r := do(t, srv, http.MethodDelete, "/v1/kv/lock?if_version="+current, ""); r.code != http.StatusConflict {
		t.Fatalf("delete with a stale version: %d, want 409", r.code)
	}
	if got := do(t, srv, http.MethodGet, "/v1/kv/lock", ""); got.body != "owner-a-renewed" {
		t.Fatalf("value = %q", got.body)
	}
}

func TestTTL(t *testing.T) {
	srv := newServer(t, Options{})

	if r := do(t, srv, http.MethodPut, "/v1/kv/session?ttl=150ms", "token"); r.code != http.StatusOK {
		t.Fatalf("put with ttl: %d %s", r.code, r.body)
	}
	got := do(t, srv, http.MethodGet, "/v1/kv/session", "")
	if got.code != http.StatusOK || got.header.Get("X-Kv-Expires-At") == "" {
		t.Fatalf("get before expiry: %d, expires header %q", got.code, got.header.Get("X-Kv-Expires-At"))
	}
	deadline := time.Now().Add(5 * time.Second)
	for do(t, srv, http.MethodGet, "/v1/kv/session", "").code != http.StatusNotFound {
		if time.Now().After(deadline) {
			t.Fatal("key with a 150ms ttl is still there after 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestListKeys(t *testing.T) {
	srv := newServer(t, Options{})
	for _, k := range []string{"a/1", "a/2", "b/1"} {
		do(t, srv, http.MethodPut, "/v1/kv/"+k, "v")
	}
	res := do(t, srv, http.MethodGet, "/v1/keys?prefix=a/", "")
	keys := res.json(t)["keys"].([]any)
	if res.code != http.StatusOK || len(keys) != 2 || keys[0].(map[string]any)["key"] != "a/1" {
		t.Fatalf("list: %d %s", res.code, res.body)
	}
	if res := do(t, srv, http.MethodGet, "/v1/keys?limit=1", ""); len(res.json(t)["keys"].([]any)) != 1 {
		t.Fatalf("limit=1: %s", res.body)
	}
}

func TestBadRequests(t *testing.T) {
	srv := newServer(t, Options{})
	cases := []struct{ method, path, body string }{
		{http.MethodPut, "/v1/kv/k?ttl=soon", "v"},
		{http.MethodPut, "/v1/kv/k?ttl=-5s", "v"},
		{http.MethodPut, "/v1/kv/k?if_version=latest", "v"},
		{http.MethodGet, "/v1/kv/k?consistency=eventual", ""},
		{http.MethodGet, "/v1/keys?limit=0", ""},
		{http.MethodPut, "/v1/kv/" + strings.Repeat("k", maxKeyBytes+1), "v"},
	}
	for _, tc := range cases {
		if r := do(t, srv, tc.method, tc.path, tc.body); r.code != http.StatusBadRequest {
			t.Errorf("%s %.40s: %d, want 400", tc.method, tc.path, r.code)
		}
	}
	big := do(t, srv, http.MethodPut, "/v1/kv/big", strings.Repeat("x", maxValueBytes+1))
	if big.code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized value: %d, want 413", big.code)
	}
	if r := do(t, srv, http.MethodPost, "/debug/isolate?on=true", ""); r.code != http.StatusNotFound {
		t.Errorf("chaos endpoint without --chaos: %d, want 404", r.code)
	}
}

func TestWatchStreamsChanges(t *testing.T) {
	srv := newServer(t, Options{})

	res, err := http.Get(srv.URL + "/v1/watch?prefix=jobs/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q", ct)
	}
	lines := make(chan string, 32)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(res.Body)
		for sc.Scan() {
			if line := sc.Text(); strings.HasPrefix(line, "event:") || strings.HasPrefix(line, "data:") {
				lines <- line
			}
		}
	}()
	next := func() string {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatal("watch stream closed early")
			}
			return l
		case <-time.After(5 * time.Second):
			t.Fatal("no watch event within 5s")
			return ""
		}
	}

	do(t, srv, http.MethodPut, "/v1/kv/other/1", "not watched")
	do(t, srv, http.MethodPut, "/v1/kv/jobs/7", "queued")
	do(t, srv, http.MethodDelete, "/v1/kv/jobs/7", "")

	if l := next(); l != "event: put" {
		t.Fatalf("first event line: %q", l)
	}
	var payload struct {
		Key     string `json:"key"`
		Value   string `json:"value"`
		Version uint64 `json:"version"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(next(), "data: ")), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Key != "jobs/7" || payload.Value != "queued" || payload.Version == 0 {
		t.Fatalf("put payload: %+v", payload)
	}
	if l := next(); l != "event: delete" {
		t.Fatalf("second event line: %q", l)
	}
}

func TestClusterStatusHealthAndMetrics(t *testing.T) {
	srv := newServer(t, Options{Chaos: true})
	do(t, srv, http.MethodPut, "/v1/kv/k", "v")

	st := do(t, srv, http.MethodGet, "/v1/cluster", "").json(t)
	if st["state"] != "Leader" || st["leader_id"] != "n1" || len(st["members"].([]any)) != 1 {
		t.Fatalf("cluster status: %v", st)
	}
	if r := do(t, srv, http.MethodGet, "/healthz", ""); r.code != http.StatusOK {
		t.Fatalf("healthz: %d", r.code)
	}
	if r := do(t, srv, http.MethodGet, "/readyz", ""); r.code != http.StatusOK {
		t.Fatalf("readyz: %d %s", r.code, r.body)
	}

	metrics := do(t, srv, http.MethodGet, "/metrics", "").body
	for _, want := range []string{
		"raftkv_raft_is_leader 1",
		`raftkv_http_request_duration_seconds_count{method="PUT",route="/v1/kv/*key",status="200"} 1`,
		"raftkv_fsm_apply_duration_seconds_count",
		"raftkv_raft_applied_index",
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("/metrics is missing %q", want)
		}
	}

	// With --chaos the node can be cut off; a one-node cluster then has no
	// majority, so a write must fail instead of being acknowledged.
	if r := do(t, srv, http.MethodPost, "/debug/isolate?on=true", ""); r.code != http.StatusOK {
		t.Fatalf("isolate: %d %s", r.code, r.body)
	}
	if r := do(t, srv, http.MethodPost, "/debug/isolate?on=false", ""); r.code != http.StatusOK {
		t.Fatalf("heal: %d %s", r.code, r.body)
	}
	if r := do(t, srv, http.MethodDelete, "/v1/cluster/members/n9", ""); r.code != http.StatusOK && r.code != http.StatusServiceUnavailable {
		t.Fatalf("removing an unknown member: %d %s", r.code, r.body)
	}
}
