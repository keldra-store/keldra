//! Process-wide stdout logging and optional OTLP metrics and traces.

use std::time::Duration;

use anyhow::{Context, Result};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{Key, KeyValue, Value};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{Instrument, PeriodicReader, SdkMeterProvider, Stream};
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider};
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::{Layer as _, SubscriberExt as _};
use tracing_subscriber::util::SubscriberInitExt as _;

mod runtime;

pub(crate) use runtime::RuntimeMetricsTask;

const SERVICE_NAME: &str = "anvil";
const TRACE_QUEUE_SIZE: usize = 2_048;
const TRACE_EXPORT_BATCH_SIZE: usize = 512;
const METRIC_CARDINALITY_LIMIT: usize = 128;
const OTLP_EXPORT_TIMEOUT: Duration = Duration::from_secs(5);
const OTLP_TRACES_PATH: &str = "/v1/traces";
const OTLP_METRICS_PATH: &str = "/v1/metrics";

// `tracing-opentelemetry` turns every non-instrument event field into a metric
// attribute. Keep the accepted set deliberately small and bounded: messages,
// object/index identities, paths, and errors remain useful on logs and spans,
// but must never create metric time series.
const METRIC_ATTRIBUTE_KEYS: &[&str] = &[
    "index.kind",
    "index.phase",
    "index.level",
    "builder.phase",
    "recovery.action",
    "compaction.input_level",
    "compaction.output_level",
    "compaction.lane_limit_reason",
    "surface",
    "phase",
    "result",
    "grpc_status_code",
    "gateway",
    "component",
    "reason",
    "definition_state.domain",
];

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
        let trace_endpoint = otlp_signal_endpoint(endpoint, OTLP_TRACES_PATH);
        let metric_endpoint = otlp_signal_endpoint(endpoint, OTLP_METRICS_PATH);
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(trace_endpoint)
            .with_timeout(OTLP_EXPORT_TIMEOUT)
            .build()
            .context("configure OTLP trace exporter")?;
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(metric_endpoint)
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
            .with_view(metric_stream)
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

fn metric_stream(_: &Instrument) -> Option<Stream> {
    Stream::builder()
        .with_allowed_attribute_keys(METRIC_ATTRIBUTE_KEYS.iter().copied().map(Key::new))
        .with_cardinality_limit(METRIC_CARDINALITY_LIMIT)
        .build()
        .ok()
}

fn configured_endpoint(endpoint: Option<&str>) -> Option<&str> {
    endpoint.map(str::trim).filter(|value| !value.is_empty())
}

fn otlp_signal_endpoint(base: &str, signal_path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), signal_path)
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
    use std::sync::{Arc, Mutex};

    use anvil_index::IndexKind;
    use anvil_index::compaction::{CompactionParallelism, CompactionProgress};
    use axum::Router;
    use axum::extract::{Request, State};
    use axum::http::StatusCode;
    use opentelemetry::Key;
    use opentelemetry::trace::{Span as _, Tracer as _};
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::Temporality;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use tracing_subscriber::layer::SubscriberExt as _;

    use crate::index_runtime::telemetry::{
        BuilderProgress, BuilderProgressPhase, CompactionInputTotals, CompactionTelemetry,
        IndexTelemetryIdentity,
    };

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct RecordingMetricExporter {
        i64_sums: Arc<Mutex<Vec<RecordedI64Sum>>>,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedI64Sum {
        name: String,
        points: Vec<RecordedI64Point>,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedI64Point {
        value: i64,
        attribute_keys: Vec<String>,
    }

    impl PushMetricExporter for RecordingMetricExporter {
        fn export(
            &self,
            metrics: &ResourceMetrics,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            let recorded = metrics
                .scope_metrics()
                .flat_map(|scope| scope.metrics())
                .filter_map(|metric| {
                    let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() else {
                        return None;
                    };
                    Some(RecordedI64Sum {
                        name: metric.name().to_owned(),
                        points: sum
                            .data_points()
                            .map(|point| {
                                let mut attribute_keys = point
                                    .attributes()
                                    .map(|attribute| attribute.key.as_str().to_owned())
                                    .collect::<Vec<_>>();
                                attribute_keys.sort();
                                RecordedI64Point {
                                    value: point.value(),
                                    attribute_keys,
                                }
                            })
                            .collect(),
                    })
                })
                .collect::<Vec<_>>();
            let output = self.i64_sums.clone();
            async move {
                output.lock().unwrap().extend(recorded);
                Ok(())
            }
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }

        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }

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
    fn signal_paths_are_appended_to_the_generic_endpoint() {
        assert_eq!(
            otlp_signal_endpoint("http://collector:4318", OTLP_TRACES_PATH),
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            otlp_signal_endpoint("http://collector:4318/", OTLP_METRICS_PATH),
            "http://collector:4318/v1/metrics"
        );
        assert_eq!(
            otlp_signal_endpoint("http://collector:4318/otlp/", OTLP_TRACES_PATH),
            "http://collector:4318/otlp/v1/traces"
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
            Some(Value::from(env!("CARGO_PKG_VERSION")))
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

    #[test]
    fn metric_attributes_are_bounded_and_compaction_active_cancels() {
        let exporter = RecordingMetricExporter::default();
        let metric_reader = PeriodicReader::builder(exporter.clone()).build();
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(metric_reader)
            .with_view(metric_stream)
            .build();
        let subscriber =
            tracing_subscriber::registry().with(MetricsLayer::new(meter_provider.clone()));

        tracing::subscriber::with_default(subscriber, || {
            let identity = IndexTelemetryIdentity {
                index_id: 99,
                tenant_id: 101,
                bucket_id: 102,
                kind: IndexKind::TypedJson,
            };
            for phase in [BuilderProgressPhase::Rebuild, BuilderProgressPhase::CatchUp] {
                let progress = BuilderProgress::start(identity, phase);
                progress.complete();
            }
            let telemetry = CompactionTelemetry::start(
                identity,
                0,
                1,
                CompactionInputTotals {
                    runs: 2,
                    records: 10,
                    bytes: 100,
                },
                CompactionParallelism::serial(),
                1,
                CompactionProgress::default(),
            )
            .unwrap();
            telemetry.complete();

            tracing::info!(
                index.kind = "typed_json",
                index.id = 4_294_967_296_u64,
                error = "customer-specific failure",
                counter.anvil_metric_attribute_filter_test = 1_i64,
                "a human-readable message must not become a metric attribute"
            );
        });
        meter_provider.force_flush().unwrap();

        let recorded = exporter.i64_sums.lock().unwrap();
        for name in [
            "anvil_index_rebuild_active",
            "anvil_index_catch_up_active",
            "anvil_index_compaction_active",
        ] {
            let active = recorded
                .iter()
                .rev()
                .find(|sum| sum.name == name)
                .unwrap_or_else(|| panic!("{name} metric was exported"));
            assert_eq!(active.points.len(), 1, "{name}");
            assert_eq!(active.points[0].value, 0, "{name}");
            assert_eq!(active.points[0].attribute_keys, ["index.kind"], "{name}");
        }

        let filtered = recorded
            .iter()
            .rev()
            .find(|sum| sum.name == "anvil_metric_attribute_filter_test")
            .expect("attribute-filter metric was exported");
        assert_eq!(filtered.points.len(), 1);
        assert_eq!(filtered.points[0].attribute_keys, ["index.kind"]);
        assert!(
            filtered.points[0]
                .attribute_keys
                .iter()
                .all(|key| key != "message" && key != "index.id" && key != "error")
        );
        drop(recorded);
        meter_provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn disabled_shutdown_needs_no_provider_or_collector() {
        Observability { providers: None }.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exporters_post_to_signal_specific_paths() {
        type Requests = Arc<Mutex<Vec<(String, String)>>>;

        async fn record_request(State(requests): State<Requests>, request: Request) -> StatusCode {
            requests.lock().unwrap().push((
                request.method().as_str().to_owned(),
                request.uri().path().to_owned(),
            ));
            StatusCode::OK
        }

        let requests = Requests::default();
        let app = Router::new()
            .fallback(record_request)
            .with_state(requests.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let providers = TelemetryProviders::build(&format!("http://{address}/"), 7).unwrap();
        let tracer = providers.tracer_provider.tracer("route-test");
        tracer.start("route-test-span").end();
        let subscriber = tracing_subscriber::registry()
            .with(MetricsLayer::new(providers.meter_provider.clone()));
        tracing::subscriber::with_default(subscriber, || {
            // MetricsLayer intentionally has no level filter even though the
            // stdout layer normally hides these periodic debug events.
            tracing::debug!(gauge.route_test_gauge = 1_u64);
        });

        tokio::task::spawn_blocking(move || providers.shutdown())
            .await
            .unwrap()
            .unwrap();

        let mut requests = requests.lock().unwrap().clone();
        requests.sort();
        assert_eq!(
            requests,
            vec![
                ("POST".to_owned(), OTLP_METRICS_PATH.to_owned()),
                ("POST".to_owned(), OTLP_TRACES_PATH.to_owned()),
            ]
        );

        server.abort();
        let _ = server.await;
    }
}
