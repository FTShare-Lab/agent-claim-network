//! TracerProvider 初始化与 RAII 生命周期管理。

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Context;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_langfuse::ExporterBuilder;
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;

use crate::config::LangfuseConfig;
use crate::config::DEFAULT_LANGFUSE_SERVICE_NAME;

/// `init_tracer` 时写入，`tracer()` 读取；未初始化时回退到默认值。
static SERVICE_NAME: OnceLock<String> = OnceLock::new();

/// 返回全局 tracer，tracer name 为配置中的 `service_name`。
/// 未初始化 TracerProvider 时返回 no-op tracer，零开销。
pub fn tracer() -> opentelemetry::global::BoxedTracer {
    global::tracer(
        SERVICE_NAME
            .get()
            .map(|s| s.as_str())
            .unwrap_or(DEFAULT_LANGFUSE_SERVICE_NAME),
    )
}

/// 持有 SdkTracerProvider 的 RAII guard，drop 时触发 flush + shutdown。
pub struct TracerGuard {
    provider: SdkTracerProvider,
}

impl TracerGuard {
    /// 主动 flush，用于进程退出前确保所有 span 已发送。
    pub fn flush(&self) {
        if let Err(e) = self.provider.force_flush() {
            log::warn!(target: "tracing", "TracerProvider flush 失败: {e:#}");
        }
    }
}

impl Drop for TracerGuard {
    fn drop(&mut self) {
        // force_flush + shutdown；失败只记日志，不 panic
        if let Err(e) = self.provider.force_flush() {
            log::warn!(target: "tracing", "TracerGuard drop flush 失败: {e:#}");
        } else {
            log::info!(target: "tracing", "TracerGuard drop flushed");
        }

        if let Err(e) = self.provider.shutdown() {
            log::warn!(target: "tracing", "TracerGuard drop shutdown 失败: {e:#}");
        } else {
            log::info!(target: "tracing", "TracerGuard drop shutdown ok");
        }
    }
}

/// 初始化全局 TracerProvider。
///
/// `enabled=false` 时返回 `Ok(None)`，调用方无需特殊处理。
/// 初始化失败仅 warn 日志，不阻塞业务启动（返回 `Ok(None)` 降级）。
pub fn init_tracer(cfg: &LangfuseConfig) -> anyhow::Result<Option<TracerGuard>> {
    // 无论是否 enabled，都把 service_name 写入 OnceLock，确保 tracer() 拿到正确名字
    let _ = SERVICE_NAME.set(cfg.service_name.clone());

    if !cfg.enabled {
        return Ok(None);
    }

    let public_key = cfg
        .public_key
        .as_deref()
        .context("langfuse.enabled=true 但 LANGFUSE_PUBLIC_KEY 未设置")?;
    let secret_key = cfg
        .secret_key
        .as_deref()
        .context("langfuse.enabled=true 但 LANGFUSE_SECRET_KEY 未设置")?;

    let exporter = ExporterBuilder::new()
        .with_endpoint(&cfg.endpoint)
        .with_basic_auth(public_key, secret_key)
        .with_timeout(Duration::from_secs(30))
        .build()
        .context("构建 Langfuse SpanExporter 失败")?;

    let processor = BatchSpanProcessor::builder(exporter, Tokio).build();
    // let processor = SimpleSpanProcessor::new(exporter);

    // 临时调试：stdout processor，span 结束即打印 JSON 到终端
    // let stdout_processor = SimpleSpanProcessor::new(StdoutSpanExporter::default());

    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        // .with_span_processor(stdout_processor)
        .with_resource(
            Resource::builder()
                .with_attributes(vec![KeyValue::new(
                    "service.name",
                    cfg.service_name.clone(),
                )])
                .build(),
        )
        .build();

    global::set_tracer_provider(provider.clone());

    log::info!(
        target: "tracing",
        "Langfuse OTLP tracer 已初始化 (service={}, endpoint={})",
        cfg.service_name, cfg.endpoint
    );
    Ok(Some(TracerGuard { provider }))
}
