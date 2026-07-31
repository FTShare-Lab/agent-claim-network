//! Langfuse OTLP tracing 初始化与生命周期管理。
//!
//! 通过 OpenTelemetry 协议将 LLM 调用和业务事件发送到 Langfuse，实现可视化 tracing。
//! `enabled=false` 时零开销（不初始化 tracer，所有 span 操作走 OTel no-op 实现）。

mod init;
mod llm_span;

pub use init::{init_tracer, tracer, TracerGuard};
pub use llm_span::record_llm_response;
