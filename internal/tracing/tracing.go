// Package tracing switches on OpenTelemetry tracing for a node.
//
// The HTTP handler and the gRPC client and server are always instrumented;
// until Setup installs a real provider those hooks do nothing. With tracing
// on, a request that a follower forwards to the leader appears as one trace:
// an HTTP span on the follower, with the leader's gRPC span as its child. The
// trace id travels in gRPC metadata (W3C traceparent).
package tracing

import (
	"context"
	"fmt"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Setup installs a global tracer provider.
//
//	mode "stdout": print finished spans as JSON to w, one per line.
//	mode "otlp":   send spans over OTLP/gRPC to a collector such as Jaeger or
//	               Tempo. The address comes from OTEL_EXPORTER_OTLP_ENDPOINT
//	               (default localhost:4317).
//
// The returned function flushes buffered spans; call it before exiting.
func Setup(ctx context.Context, mode, nodeID string, w io.Writer) (func(context.Context) error, error) {
	var (
		exporter sdktrace.SpanExporter
		err      error
	)
	switch mode {
	case "stdout":
		exporter, err = stdouttrace.New(stdouttrace.WithWriter(w))
	case "otlp":
		exporter, err = otlptracegrpc.New(ctx)
	default:
		return nil, fmt.Errorf("unknown trace mode %q (use stdout or otlp)", mode)
	}
	if err != nil {
		return nil, err
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", "raft-kv"),
		attribute.String("service.instance.id", nodeID),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown, nil
}
