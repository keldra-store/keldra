//! Process-wide stdout logging and optional OTLP metrics and traces.

use std::time::Duration;

use anyhow::{Context, Result};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, Value};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{Instrument, PeriodicReader, SdkMeterProvider, Stream};
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider};
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::{Layer as _, SubscriberExt as _};
use tracing_subscriber::util::SubscriberInitExt as _;

const SERVICE_NAME: &str = "anvil";
const TRACE_QUEUE_SIZE: usize = 2_048;
const TRACE_EXPORT_BATCH_SIZE: usize = 512;
const METRIC_CARDINALITY_LIMIT: usize = 128;
const OTLP_EXPORT_TIMEOUT: Duration = Duration::from_secs(5);

/// Startup-only observability settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservabilityConfig {
    pub node_id: u16,
    pub otlp_endpoint: Option<String>,
}

/// Owns the optional OpenTelemetry providers until graceful process shutdown.
pub struct Observability {
    providers: Option<TelemetryProviders>,
}

impl Observability {
    /// Install structured stdout logging and, when configured, OTLP metrics and
    /// traces. An absent or whitespace-only endpoint does not construct an
    /// exporter or start an OpenTelemetry worker.
    pub fn init(config: ObservabilityConfig) -> Result<Self> {
        let Some(endpoint) = configured_endpoint(config.otlp_endpoint.as_deref()) else {
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(std::io::stdout)
                        .with_filter(EnvFilter::from_default_env()),
                )
                .try_init()
                .context("install stdout tracing subscriber")?;
            tracing::info!(node_id = config.node_id, "OTLP export disabled");
            return Ok(Self { providers: None });
        };

        let providers = TelemetryProviders::build(endpoint, config.node_id)?;
        let tracer = providers.tracer_provider.tracer("anvil-server");
        tracing_subscriber::registry()
            .with(MetricsLayer::new(providers.meter_provider.clone()))
            .with(OpenTelemetryLayer::new(tracer))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stdout)
                    .with_filter(EnvFilter::from_default_env()),
            )
            .try_init()
            .context("install stdout and OpenTelemetry tracing subscriber")?;
        tracing::info!(
            node_id = config.node_id,
            transport = "http/protobuf",
            "OTLP metrics and traces enabled"
        );
        Ok(Self {
            providers: Some(providers),
        })
    }

    /// Flush and stop the optional providers without blocking a Tokio worker.
    pub async fn shutdown(self) -> Result<()> {
        let Some(providers) = self.providers else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || providers.shutdown())
            .await
            .context("join OpenTelemetry shutdown worker")?
    }
}

struct TelemetryProviders {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
}

impl TelemetryProviders {
    fn build(endpoint: &str, node_id: u16) -> Result<Self> {
        let resource = telemetry_resource(node_id);
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(endpoint)
            .with_timeout(OTLP_EXPORT_TIMEOUT)
            .build()
            .context("configure OTLP trace exporter")?;
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(endpoint)
            .with_timeout(OTLP_EXPORT_TIMEOUT)
            .build()
            .context("configure OTLP metric exporter")?;

        let batch_config = BatchConfigBuilder::default()
            .with_max_queue_size(TRACE_QUEUE_SIZE)
            .with_max_export_batch_size(TRACE_EXPORT_BATCH_SIZE)
            .build();
        let span_processor = BatchSpanProcessor::builder(span_exporter)
            .with_batch_config(batch_config)
            .build();
        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_span_processor(span_processor)
            .build();

        let metric_reader = PeriodicReader::builder(metric_exporter).build();
        let meter_provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(metric_reader)
            .with_view(|_: &Instrument| {
                Stream::builder()
                    .with_cardinality_limit(METRIC_CARDINALITY_LIMIT)
                    .build()
                    .ok()
            })
            .build();

        Ok(Self {
            tracer_provider,
            meter_provider,
        })
    }

    fn shutdown(self) -> Result<()> {
        // Compute both results before returning either error so one failed
        // signal cannot prevent the other provider from shutting down.
        let metric_result = self
            .meter_provider
            .shutdown()
            .context("shut down OpenTelemetry meter provider");
        let trace_result = self
            .tracer_provider
            .shutdown()
            .context("shut down OpenTelemetry tracer provider");
        metric_result.and(trace_result)
    }
}

fn configured_endpoint(endpoint: Option<&str>) -> Option<&str> {
    endpoint.map(str::trim).filter(|value| !value.is_empty())
}

fn telemetry_resource(node_id: u16) -> Resource {
    Resource::builder_empty()
        .with_service_name(SERVICE_NAME)
        .with_attributes([
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("node.id", Value::I64(i64::from(node_id))),
        ])
        .build()
}

#[cfg(test)]
mod tests {
    use opentelemetry::Key;

    use super::*;

    #[test]
    fn absent_or_empty_endpoint_keeps_otlp_disabled() {
        assert_eq!(configured_endpoint(None), None);
        assert_eq!(configured_endpoint(Some("")), None);
        assert_eq!(configured_endpoint(Some("  \t")), None);
        assert_eq!(
            configured_endpoint(Some("  http://collector:4318/  ")),
            Some("http://collector:4318/")
        );
    }

    #[test]
    fn resource_identifies_service_version_and_node() {
        let resource = telemetry_resource(41);
        assert_eq!(
            resource.get(&Key::new("service.name")),
            Some(Value::from(SERVICE_NAME))
        );
        assert_eq!(
            resource.get(&Key::new("service.version")),
            Some(Value::from("0.5.2"))
        );
        assert_eq!(resource.get(&Key::new("node.id")), Some(Value::I64(41)));
    }

    #[test]
    fn exporter_buffers_have_explicit_bounds() {
        assert!(TRACE_QUEUE_SIZE > 0);
        assert!(TRACE_EXPORT_BATCH_SIZE <= TRACE_QUEUE_SIZE);
        assert!(METRIC_CARDINALITY_LIMIT > 0);
        assert!(OTLP_EXPORT_TIMEOUT > Duration::ZERO);
    }

    #[tokio::test]
    async fn disabled_shutdown_needs_no_provider_or_collector() {
        Observability { providers: None }.shutdown().await.unwrap();
    }
}
