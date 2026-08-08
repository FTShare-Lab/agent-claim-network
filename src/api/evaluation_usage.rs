//! 仅评测模式启用的 LLM token usage 记录。
//!
//! 普通运行不进入 task-local 作用域，`record_evaluation_usage` 直接返回，
//! 因此该模块对交互式 session 无任何开销与行为影响。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::Value;

/// 单次 LLM HTTP attempt 的 OpenAI Chat / Responses usage 投影。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct EvaluationUsage {
    pub request_sequence: u64,
    pub response_received: bool,
    pub model: Option<String>,
    pub is_complete: bool,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub reasoning_tokens: u64,
}

impl EvaluationUsage {
    /// 解析 `usage`；缺失字段按 0 计，并保留响应是否具备完整计量所需字段。
    fn from_response(usage: Option<&Value>, model: Option<&str>) -> Self {
        let usage = usage.unwrap_or(&Value::Null);
        Self {
            request_sequence: 0,
            response_received: true,
            model: model.map(str::to_owned),
            is_complete: model.is_some()
                && u64_at_any_option(usage, &[&["input_tokens"], &["prompt_tokens"]]).is_some()
                && u64_at_any_option(usage, &[&["output_tokens"], &["completion_tokens"]])
                    .is_some(),
            input_tokens: u64_at_any(usage, &[&["input_tokens"], &["prompt_tokens"]]),
            output_tokens: u64_at_any(usage, &[&["output_tokens"], &["completion_tokens"]]),
            cache_read_tokens: u64_at_any(
                usage,
                &[
                    &["input_tokens_details", "cached_tokens"],
                    &["prompt_tokens_details", "cached_tokens"],
                ],
            ),
            reasoning_tokens: u64_at_any(
                usage,
                &[
                    &["output_tokens_details", "reasoning_tokens"],
                    &["completion_tokens_details", "reasoning_tokens"],
                ],
            ),
        }
    }
}

fn u64_at_any(value: &Value, paths: &[&[&str]]) -> u64 {
    u64_at_any_option(value, paths).unwrap_or(0)
}

fn u64_at_any_option(value: &Value, paths: &[&[&str]]) -> Option<u64> {
    paths.iter().find_map(|path| u64_at_option(value, path))
}

fn u64_at_option(value: &Value, path: &[&str]) -> Option<u64> {
    let mut current = value;
    for key in path {
        current = current.get(key)?;
    }
    current.as_u64()
}

/// 累计一个 attempt 内所有模型请求的 usage。
#[derive(Debug, Default)]
pub struct EvaluationUsageRecorder {
    records: Mutex<Vec<EvaluationUsage>>,
    next_sequence: AtomicU64,
    audit_incomplete: AtomicBool,
}

impl EvaluationUsageRecorder {
    /// 已收到模型响应的数量；不把网络失败或 HTTP 错误当作一步。
    pub fn response_count(&self) -> usize {
        match self.records.lock() {
            Ok(records) => records
                .iter()
                .filter(|record| record.response_received)
                .count(),
            Err(poisoned) => {
                self.audit_incomplete.store(true, Ordering::Release);
                poisoned
                    .into_inner()
                    .iter()
                    .filter(|record| record.response_received)
                    .count()
            }
        }
    }

    pub fn audit_is_incomplete(&self) -> bool {
        self.audit_incomplete.load(Ordering::Acquire)
    }

    pub fn take_records(&self) -> Vec<EvaluationUsage> {
        match self.records.lock() {
            Ok(mut records) => std::mem::take(&mut *records),
            Err(poisoned) => {
                self.audit_incomplete.store(true, Ordering::Release);
                std::mem::take(&mut *poisoned.into_inner())
            }
        }
    }

    fn start_request(&self) -> u64 {
        let request_sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        match self.records.lock() {
            Ok(mut records) => records.push(EvaluationUsage {
                request_sequence,
                ..EvaluationUsage::default()
            }),
            Err(poisoned) => {
                self.audit_incomplete.store(true, Ordering::Release);
                poisoned.into_inner().push(EvaluationUsage {
                    request_sequence,
                    ..EvaluationUsage::default()
                });
            }
        }
        request_sequence
    }

    fn record_response(&self, request_sequence: u64, usage: Option<&Value>, model: Option<&str>) {
        let usage = EvaluationUsage {
            request_sequence,
            ..EvaluationUsage::from_response(usage, model)
        };
        match self.records.lock() {
            Ok(mut records) => {
                if let Some(record) = records
                    .iter_mut()
                    .find(|record| record.request_sequence == request_sequence)
                {
                    *record = usage;
                } else {
                    self.audit_incomplete.store(true, Ordering::Release);
                }
            }
            Err(_) => {
                self.audit_incomplete.store(true, Ordering::Release);
            }
        }
    }
}

tokio::task_local! {
    static EVALUATION_USAGE_RECORDER: Arc<EvaluationUsageRecorder>;
}

/// 在一个评测 future 内启用逐请求 usage 记录。
pub async fn with_evaluation_usage_recording<T>(
    recorder: Arc<EvaluationUsageRecorder>,
    future: impl std::future::Future<Output = T>,
) -> T {
    EVALUATION_USAGE_RECORDER.scope(recorder, future).await
}

/// 在每次真实 provider HTTP attempt 前建立不完整记录；不在评测上下文中时是 no-op。
pub(crate) fn record_evaluation_request_started() -> Option<u64> {
    EVALUATION_USAGE_RECORDER
        .try_with(|recorder| recorder.start_request())
        .ok()
}

/// 成功收到 provider 响应后补齐对应 attempt 的 usage。
pub(crate) fn record_evaluation_usage(
    request_sequence: Option<u64>,
    usage: Option<&Value>,
    model: Option<&str>,
) {
    let Some(request_sequence) = request_sequence else {
        return;
    };
    let _ = EVALUATION_USAGE_RECORDER.try_with(|recorder| {
        recorder.record_response(request_sequence, usage, model);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_openai_compatible_usage_including_reasoning_and_cache() {
        let usage = EvaluationUsage::from_response(
            Some(&json!({
                "prompt_tokens": 42480,
                "completion_tokens": 8193,
                "prompt_tokens_details": {"cached_tokens": 42368},
                "completion_tokens_details": {"reasoning_tokens": 8100},
                "total_tokens": 50673
            })),
            Some("actual-model"),
        );

        assert_eq!(
            usage,
            EvaluationUsage {
                request_sequence: 0,
                response_received: true,
                model: Some("actual-model".into()),
                is_complete: true,
                input_tokens: 42480,
                output_tokens: 8193,
                cache_read_tokens: 42368,
                reasoning_tokens: 8100,
            }
        );
    }

    #[test]
    fn parses_openai_responses_usage_including_reasoning_and_cache() {
        let usage = EvaluationUsage::from_response(
            Some(&json!({
                "input_tokens": 42480,
                "input_tokens_details": {"cached_tokens": 42368},
                "output_tokens": 8193,
                "output_tokens_details": {"reasoning_tokens": 8100}
            })),
            Some("actual-responses-model"),
        );

        assert_eq!(
            usage,
            EvaluationUsage {
                request_sequence: 0,
                response_received: true,
                model: Some("actual-responses-model".into()),
                is_complete: true,
                input_tokens: 42480,
                output_tokens: 8193,
                cache_read_tokens: 42368,
                reasoning_tokens: 8100,
            }
        );
    }

    #[test]
    fn missing_model_or_usage_fields_are_marked_incomplete() {
        assert_eq!(
            EvaluationUsage::from_response(Some(&json!({"prompt_tokens": 19})), None),
            EvaluationUsage {
                response_received: true,
                model: None,
                input_tokens: 19,
                ..EvaluationUsage::default()
            }
        );
        assert_eq!(
            EvaluationUsage::from_response(Some(&json!({"prompt_tokens": "19"})), None),
            EvaluationUsage {
                response_received: true,
                ..EvaluationUsage::default()
            }
        );
        assert!(
            !EvaluationUsage::from_response(
                Some(&json!({"prompt_tokens": 19, "completion_tokens": 2})),
                None,
            )
            .is_complete
        );
        assert!(!EvaluationUsage::from_response(None, Some("actual-model")).is_complete);
        assert!(
            !EvaluationUsage::from_response(
                Some(&json!({"prompt_tokens": 19})),
                Some("actual-model"),
            )
            .is_complete
        );
    }

    #[tokio::test]
    async fn records_only_inside_evaluation_scope() {
        record_evaluation_usage(None, Some(&json!({"prompt_tokens": 1})), None);

        let recorder = Arc::new(EvaluationUsageRecorder::default());
        with_evaluation_usage_recording(recorder.clone(), async {
            let first = record_evaluation_request_started().unwrap();
            record_evaluation_usage(
                Some(first),
                Some(&json!({"prompt_tokens": 7, "completion_tokens": 3})),
                Some("actual-model"),
            );
            let second = record_evaluation_request_started().unwrap();
            record_evaluation_usage(Some(second), None, None);
        })
        .await;

        assert_eq!(recorder.response_count(), 2);
        let records = recorder.take_records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].request_sequence, 1);
        assert_eq!(records[1].request_sequence, 2);
        assert!(records[0].response_received);
        assert!(records[1].response_received);
        assert_eq!(records[0].input_tokens, 7);
        assert_eq!(records[0].output_tokens, 3);
        assert_eq!(records[0].model.as_deref(), Some("actual-model"));
        assert!(records[0].is_complete);
        assert!(records[1].response_received);
        assert!(!records[1].is_complete);
    }

    #[tokio::test]
    async fn failed_request_stays_visible_as_incomplete_attempt() {
        let recorder = Arc::new(EvaluationUsageRecorder::default());
        with_evaluation_usage_recording(recorder.clone(), async {
            let completed = record_evaluation_request_started().unwrap();
            record_evaluation_usage(
                Some(completed),
                Some(&json!({"prompt_tokens": 7, "completion_tokens": 3})),
                Some("actual-model"),
            );
            let _failed = record_evaluation_request_started().unwrap();
        })
        .await;

        assert_eq!(recorder.response_count(), 1);
        let records = recorder.take_records();
        assert_eq!(records.len(), 2);
        assert!(records[0].is_complete);
        assert!(!records[1].response_received);
        assert!(!records[1].is_complete);
    }
}
