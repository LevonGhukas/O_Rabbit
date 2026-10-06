package telemetry

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TracingEnabled reports whether an OTLP endpoint is configured through the
// standard OTEL_EXPORTER_OTLP_(TRACES_)ENDPOINT variables.
func TracingEnabled() bool {
	return strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")) != "" ||
		strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != ""
}

// InitTracing installs a global tracer provider that exports spans over OTLP
// gRPC when TracingEnabled. The exporter, sampler and resource honour the
// standard OTEL_* variables (for example OTEL_TRACES_SAMPLER,
// OTEL_RESOURCE_ATTRIBUTES). Without an endpoint the global no-op provider
// stays in place. The returned function flushes and stops the exporter.
func InitTracing(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if !TracingEnabled() {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	// Later options win, so OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES can
	// override the default service name.
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", serviceName)),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
