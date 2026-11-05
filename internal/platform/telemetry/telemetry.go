// Package telemetry wires up OpenTelemetry tracing and the Prometheus
// metrics registry.
//
// Metrics are pulled by Prometheus from the admin server (/metrics). Traces
// are pushed via OTLP/HTTP to any compatible backend (OpenTelemetry
// Collector, Jaeger, Tempo, Honeycomb, Datadog, ...). The exporter honours
// the standard OTEL_EXPORTER_OTLP_* environment variables, e.g.:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
//	OTEL_EXPORTER_OTLP_HEADERS=x-api-key=secret
package telemetry

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/Oxyrus/orderbook/internal/buildinfo"
	"github.com/Oxyrus/orderbook/internal/config"
)

// NewRegistry returns a Prometheus registry pre-populated with Go runtime,
// process and build-info collectors. A dedicated registry (instead of the
// global default) keeps tests isolated.
func NewRegistry(serviceName string) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "app_build_info",
			Help: "Build information; value is always 1.",
			ConstLabels: prometheus.Labels{
				"service": serviceName,
				"version": buildinfo.Version,
				"commit":  buildinfo.Commit,
			},
		}, func() float64 { return 1 }),
	)
	return reg
}

// ShutdownFunc flushes and stops telemetry exporters.
type ShutdownFunc func(context.Context) error

// SetupTracing installs the global TracerProvider and W3C propagators. When
// tracing is disabled, only propagators are installed (incoming trace
// context is still forwarded) and a no-op shutdown is returned.
func SetupTracing(ctx context.Context, cfg config.Config) (ShutdownFunc, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	if !cfg.Telemetry.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(buildinfo.Version),
		semconv.DeploymentEnvironmentNameKey.String(cfg.Environment),
	))
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.Telemetry.SampleRatio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
