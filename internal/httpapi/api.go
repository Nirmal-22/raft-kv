// Package httpapi is the client-facing REST API, built on Gin. Every node
// serves it; a node that is not the leader forwards the request for you.
package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Nirmal-22/raft-kv/internal/node"
	"github.com/Nirmal-22/raft-kv/internal/pb"
)

const (
	maxKeyBytes   = 1024
	maxValueBytes = 1 << 20
)

type Options struct {
	// RequestTimeout bounds every request except watch streams.
	RequestTimeout time.Duration
	// Chaos enables POST /debug/isolate, which cuts the node off from its
	// peers. For demos and tests only.
	Chaos bool
}

type api struct {
	n    *node.Node
	opts Options
}

// New builds the HTTP handler for one node.
func New(n *node.Node, opts Options) http.Handler {
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = 5 * time.Second
	}
	a := &api{n: n, opts: opts}

	r := gin.New()
	r.Use(gin.Recovery(), a.observe)
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", a.ready)
	r.GET("/metrics", gin.WrapH(promhttp.HandlerFor(n.Metrics.Registry, promhttp.HandlerOpts{})))

	// Trace only the data API, not health checks and metric scrapes.
	v1 := r.Group("/v1", otelgin.Middleware("raft-kv"))
	v1.GET("/watch", a.watch) // long-lived: no request timeout
	timed := v1.Group("", a.timeout)
	timed.PUT("/kv/*key", a.put)
	timed.GET("/kv/*key", a.get)
	timed.DELETE("/kv/*key", a.delete)
	timed.GET("/keys", a.list)
	timed.GET("/cluster", func(c *gin.Context) { c.JSON(http.StatusOK, n.Status()) })
	timed.DELETE("/cluster/members/:id", a.removeMember)

	if opts.Chaos {
		r.POST("/debug/isolate", a.isolate)
	}
	return r
}

// observe records one histogram sample per request, labelled by route pattern
// (not the raw path, which would create one time series per key).
func (a *api) observe(c *gin.Context) {
	start := time.Now()
	c.Next()
	route := c.FullPath()
	if route == "" {
		route = "unmatched"
	}
	a.n.Metrics.HTTPDuration.
		WithLabelValues(c.Request.Method, route, strconv.Itoa(c.Writer.Status())).
		Observe(time.Since(start).Seconds())
}

func (a *api) timeout(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), a.opts.RequestTimeout)
	defer cancel()
	c.Request = c.Request.WithContext(ctx)
	c.Next()
}

func (a *api) ready(c *gin.Context) {
	st := a.n.Status()
	if st.LeaderID == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "no leader", "state": st.State})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready", "state": st.State, "leader_id": st.LeaderID})
}

func (a *api) put(c *gin.Context) {
	key, ok := keyParam(c)
	if !ok {
		return
	}
	value, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxValueBytes))
	if err != nil {
		fail(c, http.StatusRequestEntityTooLarge, fmt.Sprintf("value is larger than %d bytes", maxValueBytes))
		return
	}
	put := &pb.Put{Key: key, Value: value}
	if raw := c.Query("ttl"); raw != "" {
		ttl, err := time.ParseDuration(raw)
		if err != nil || ttl <= 0 {
			fail(c, http.StatusBadRequest, "ttl must be a positive duration such as 30s or 5m")
			return
		}
		put.TtlMs = ttl.Milliseconds()
	}
	if put.IfVersion, ok = ifVersion(c); !ok {
		return
	}
	res, err := a.n.Apply(c.Request.Context(), &pb.Command{Op: &pb.Command_Put{Put: put}})
	if err != nil {
		writeError(c, err, true)
		return
	}
	if res.Status == pb.Status_STATUS_VERSION_MISMATCH {
		conflict(c, res)
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "version": res.Version})
}

func (a *api) get(c *gin.Context) {
	key, ok := keyParam(c)
	if !ok {
		return
	}
	consistency, ok := consistencyParam(c)
	if !ok {
		return
	}
	res, err := a.n.Get(c.Request.Context(), key, consistency)
	if err != nil {
		writeError(c, err, false)
		return
	}
	if !res.Found {
		fail(c, http.StatusNotFound, "key not found")
		return
	}
	c.Header("X-Kv-Version", strconv.FormatUint(res.Version, 10))
	if res.ExpiresAtMs != 0 {
		c.Header("X-Kv-Expires-At", time.UnixMilli(res.ExpiresAtMs).UTC().Format(time.RFC3339Nano))
	}
	c.Data(http.StatusOK, "application/octet-stream", res.Value)
}

func (a *api) delete(c *gin.Context) {
	key, ok := keyParam(c)
	if !ok {
		return
	}
	del := &pb.Delete{Key: key}
	if del.IfVersion, ok = ifVersion(c); !ok {
		return
	}
	res, err := a.n.Apply(c.Request.Context(), &pb.Command{Op: &pb.Command_Delete{Delete: del}})
	if err != nil {
		writeError(c, err, true)
		return
	}
	switch res.Status {
	case pb.Status_STATUS_VERSION_MISMATCH:
		conflict(c, res)
	case pb.Status_STATUS_NOT_FOUND:
		fail(c, http.StatusNotFound, "key not found")
	default:
		c.JSON(http.StatusOK, gin.H{"key": key, "deleted": true})
	}
}

func (a *api) list(c *gin.Context) {
	consistency, ok := consistencyParam(c)
	if !ok {
		return
	}
	limit := 100
	if raw := c.Query("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 || v > 10000 {
			fail(c, http.StatusBadRequest, "limit must be between 1 and 10000")
			return
		}
		limit = v
	}
	keys, err := a.n.List(c.Request.Context(), c.Query("prefix"), limit, consistency)
	if err != nil {
		writeError(c, err, false)
		return
	}
	type item struct {
		Key         string `json:"key"`
		Version     uint64 `json:"version"`
		ExpiresAtMs int64  `json:"expires_at_ms,omitempty"`
	}
	out := make([]item, 0, len(keys))
	for _, k := range keys {
		out = append(out, item{Key: k.Key, Version: k.Version, ExpiresAtMs: k.ExpiresAtMs})
	}
	c.JSON(http.StatusOK, gin.H{"keys": out})
}

// watch streams changes as Server-Sent Events. It reports what this node
// applies, so a lagging follower delivers events late. A client that falls
// too far behind, or whose node loads a snapshot, gets a final "dropped" event
// and must re-read and reconnect; events are not replayed.
func (a *api) watch(c *gin.Context) {
	events, cancel := a.n.Watch(c.Query("prefix"))
	defer cancel()

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	fmt.Fprint(c.Writer, ": watching\n\n")
	c.Writer.Flush()

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-keepalive.C:
			fmt.Fprint(c.Writer, ": ping\n\n")
		case ev, open := <-events:
			if !open {
				fmt.Fprint(c.Writer, "event: dropped\ndata: {\"reason\":\"events were missed (fell behind, or this node loaded a snapshot); re-read and reconnect\"}\n\n")
				c.Writer.Flush()
				return
			}
			payload := map[string]any{"key": ev.Key, "version": ev.Version}
			if ev.Type == "put" {
				// JSON strings must be valid UTF-8. Anything else goes out
				// base64-encoded instead of being silently altered.
				if utf8.Valid(ev.Value) {
					payload["value"] = string(ev.Value)
				} else {
					payload["value_base64"] = base64.StdEncoding.EncodeToString(ev.Value)
				}
			}
			data, _ := json.Marshal(payload)
			fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", ev.Type, data)
		}
		c.Writer.Flush()
	}
}

func (a *api) removeMember(c *gin.Context) {
	err := a.n.Remove(c.Request.Context(), c.Param("id"))
	if errors.Is(err, raft.ErrNotLeader) {
		c.JSON(http.StatusMisdirectedRequest, gin.H{
			"error":       "membership changes must be sent to the leader",
			"leader_http": a.n.Status().LeaderHTTP,
		})
		return
	}
	if err != nil {
		writeError(c, err, true)
		return
	}
	c.JSON(http.StatusOK, gin.H{"removed": c.Param("id")})
}

func (a *api) isolate(c *gin.Context) {
	on, err := strconv.ParseBool(c.Query("on"))
	if err != nil {
		fail(c, http.StatusBadRequest, "on must be true or false")
		return
	}
	a.n.SetIsolated(on)
	c.JSON(http.StatusOK, gin.H{"node_id": a.n.ID(), "isolated": on})
}

// ------------------------------------------------------------------ helpers

func keyParam(c *gin.Context) (string, bool) {
	key := strings.TrimPrefix(c.Param("key"), "/")
	if key == "" || len(key) > maxKeyBytes {
		fail(c, http.StatusBadRequest, fmt.Sprintf("key must be 1 to %d bytes", maxKeyBytes))
		return "", false
	}
	return key, true
}

func ifVersion(c *gin.Context) (*uint64, bool) {
	raw := c.Query("if_version")
	if raw == "" {
		return nil, true
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		fail(c, http.StatusBadRequest, "if_version must be a non-negative integer (0 means the key must not exist)")
		return nil, false
	}
	return &v, true
}

func consistencyParam(c *gin.Context) (node.Consistency, bool) {
	switch v := node.Consistency(c.DefaultQuery("consistency", string(node.Linearizable))); v {
	case node.Linearizable, node.Stale:
		return v, true
	default:
		fail(c, http.StatusBadRequest, "consistency must be linearizable or stale")
		return "", false
	}
}

func fail(c *gin.Context, code int, msg string) {
	c.AbortWithStatusJSON(code, gin.H{"error": msg})
}

func conflict(c *gin.Context, res *pb.ApplyResult) {
	c.AbortWithStatusJSON(http.StatusConflict, gin.H{
		"error":           "version mismatch",
		"current_version": res.CurrentVersion,
	})
}

// writeError turns a failure into a status code. The distinction that matters
// to a client is whether a write is known not to have happened (503 with
// "no leader") or might have (the other cases).
func writeError(c *gin.Context, err error, isWrite bool) {
	unknown := ""
	if isWrite {
		unknown = "; the write may or may not have been applied"
	}
	switch {
	case errors.Is(err, node.ErrNoLeader):
		c.Header("Retry-After", "1")
		fail(c, http.StatusServiceUnavailable, "no leader available; nothing was written, retry shortly")
	case errors.Is(err, context.DeadlineExceeded), status.Code(err) == codes.DeadlineExceeded:
		fail(c, http.StatusGatewayTimeout, "timed out"+unknown)
	default:
		fail(c, http.StatusServiceUnavailable, err.Error()+unknown)
	}
}
