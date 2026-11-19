// kvd runs one node of a raft-kv cluster.
//
//	kvd --id n1 --data ./data/n1 --http 127.0.0.1:8001 --grpc 127.0.0.1:9001 --raft 127.0.0.1:7001 --bootstrap
//	kvd --id n2 --data ./data/n2 --http 127.0.0.1:8002 --grpc 127.0.0.1:9002 --raft 127.0.0.1:7002 --join 127.0.0.1:9001
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Nirmal-22/raft-kv/internal/httpapi"
	"github.com/Nirmal-22/raft-kv/internal/node"
	"github.com/Nirmal-22/raft-kv/internal/tracing"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kvd:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		cfg   node.Config
		join  string
		chaos bool
		trace string
	)
	flag.StringVar(&cfg.ID, "id", "", "unique node id (required)")
	flag.StringVar(&cfg.DataDir, "data", "", "data directory (default ./data/<id>)")
	flag.StringVar(&cfg.HTTPAddr, "http", "127.0.0.1:8001", "client HTTP address")
	flag.StringVar(&cfg.GRPCAddr, "grpc", "127.0.0.1:9001", "node-to-node gRPC address")
	flag.StringVar(&cfg.RaftAddr, "raft", "127.0.0.1:7001", "Raft replication address")
	flag.BoolVar(&cfg.Bootstrap, "bootstrap", false, "start a new cluster with this node as its only member")
	flag.StringVar(&join, "join", "", "comma-separated gRPC addresses of nodes already in the cluster")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "debug, info, warn or error")
	flag.BoolVar(&chaos, "chaos", false, "enable POST /debug/isolate (cuts this node off from its peers)")
	flag.StringVar(&trace, "trace", "", "tracing: stdout (print spans) or otlp (send to a collector); off when empty")
	flag.Parse()

	if cfg.ID == "" {
		return errors.New("--id is required")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data/" + cfg.ID
	}
	for _, addr := range strings.Split(join, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			cfg.Join = append(cfg.Join, addr)
		}
	}

	if trace != "" {
		flush, err := tracing.Setup(context.Background(), trace, cfg.ID, os.Stdout)
		if err != nil {
			return err
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			flush(ctx)
		}()
	}

	n, err := node.New(cfg)
	if err != nil {
		return err
	}
	defer n.Shutdown()

	gin.SetMode(gin.ReleaseMode)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.New(n, httpapi.Options{Chaos: chaos}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	fmt.Fprintf(os.Stderr, "kvd: node %s serving http=%s grpc=%s raft=%s\n", cfg.ID, cfg.HTTPAddr, n.GRPCAddr(), n.RaftAddr())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errc:
		return err
	case <-sigs:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	srv.Shutdown(ctx) // watch streams are long-lived; do not wait forever for them
	return nil
}
