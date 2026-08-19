//! TUI 黑盒测试使用的独立本地假服务。
//!
//! 服务按请求的 `stream` 字段输出 Anthropic-compatible SSE 或非流式 JSON，并为启动时的
//! maintainer inbox pull 返回空列表、router scope overview 返回空快照；
//! `background-process` 模式额外驱动一次真实 `code_run` 回环，
//! `process-control` 模式覆盖进程轮询去重、终止与自然失败的完整 TUI 回归，
//! `slow-structured` 模式为 compact queue 键盘优先级保留稳定的压缩中窗口。
//! 它不复用生产 DTO，避免协议测试形成“自己测试自己”。

use std::convert::Infallible;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use futures::{stream, Stream};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

type SseItem = Result<Event, Infallible>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ResponseMode {
    #[default]
    StreamingText,
    BackgroundProcess,
    ProcessControl,
    SlowStructured,
    Http429,
}

#[derive(Clone, Copy, Debug)]
struct ServerState {
    response_mode: ResponseMode,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (ready_file, response_mode) = parse_args()?;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("绑定 fake Anthropic server 端口失败")?;
    let addr = listener
        .local_addr()
        .context("读取 fake Anthropic server 地址失败")?;
    let app = Router::new()
        .route("/v1/messages", post(messages))
        .route("/inbox/pull", post(empty_inbox))
        .route("/claims/scopes/overview", post(empty_scopes_overview))
        .with_state(ServerState { response_mode });

    tokio::fs::write(&ready_file, addr.port().to_string())
        .await
        .with_context(|| format!("写 fake server ready file 失败: {}", ready_file.display()))?;
    axum::serve(listener, app)
        .await
        .context("运行 fake Anthropic server 失败")
}

fn parse_args() -> anyhow::Result<(PathBuf, ResponseMode)> {
    let mut ready_file = None;
    let mut response_mode = ResponseMode::StreamingText;
    let mut args = std::env::args_os().skip(1);
    while let Some(flag) = args.next() {
        match flag.to_str() {
            Some("--ready-file") if ready_file.is_none() => {
                ready_file = args.next().map(PathBuf::from);
            }
            Some("--response-mode") => {
                response_mode = match args.next().as_deref().and_then(|value| value.to_str()) {
                    Some("streaming-text") => ResponseMode::StreamingText,
                    Some("background-process") => ResponseMode::BackgroundProcess,
                    Some("process-control") => ResponseMode::ProcessControl,
                    Some("slow-structured") => ResponseMode::SlowStructured,
                    Some("http-429") => ResponseMode::Http429,
                    _ => anyhow::bail!("unsupported --response-mode"),
                };
            }
            _ => anyhow::bail!("unsupported argument"),
        }
    }
    let ready_file = ready_file.context("--ready-file 缺少目标路径")?;
    Ok((ready_file, response_mode))
}

async fn empty_inbox() -> Json<Value> {
    Json(json!([]))
}

async fn empty_scopes_overview() -> Json<Value> {
    Json(json!({"scopes": []}))
}

async fn messages(State(state): State<ServerState>, Json(request): Json<Value>) -> Response {
    if state.response_mode == ResponseMode::Http429 {
        return http_429_response();
    }

    if request.get("stream").and_then(Value::as_bool) == Some(true) {
        return match state.response_mode {
            ResponseMode::StreamingText => streaming_messages().into_response(),
            ResponseMode::BackgroundProcess if request_is_session_recap(&request) => {
                session_recap_messages().into_response()
            }
            ResponseMode::BackgroundProcess if request_contains_tool_result(&request) => {
                background_process_completed_messages().into_response()
            }
            ResponseMode::BackgroundProcess => background_process_tool_messages().into_response(),
            ResponseMode::ProcessControl => process_control_messages(&request),
            ResponseMode::SlowStructured => short_streaming_messages().into_response(),
            ResponseMode::Http429 => http_429_response(),
        };
    }

    if state.response_mode == ResponseMode::SlowStructured {
        tokio::time::sleep(Duration::from_secs(15)).await;
        return Json(slow_compaction_message()).into_response();
    }
    Json(non_streaming_message()).into_response()
}

fn http_429_response() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({
            "error": {
                "type": "rate_limit_error",
                "message": "fake HTTP 429"
            },
            "type": "error"
        })),
    )
        .into_response()
}

fn request_contains_tool_result(request: &Value) -> bool {
    request
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|message| message.get("content").and_then(Value::as_array))
        .flatten()
        .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
}

fn request_is_session_recap(request: &Value) -> bool {
    request
        .get("system")
        .is_some_and(|system| system.to_string().contains("复盘阶段"))
}

fn tool_result_content<'a>(request: &'a Value, tool_use_id: &str) -> Option<&'a str> {
    request
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|message| message.get("content").and_then(Value::as_array))
        .flatten()
        .find_map(|block| {
            (block.get("type").and_then(Value::as_str) == Some("tool_result")
                && block.get("tool_use_id").and_then(Value::as_str) == Some(tool_use_id))
            .then(|| block.get("content").and_then(Value::as_str))
            .flatten()
        })
}

fn parsed_tool_result(request: &Value, tool_use_id: &str) -> Option<Value> {
    tool_result_content(request, tool_use_id).and_then(|content| serde_json::from_str(content).ok())
}

fn tool_result_process_id(request: &Value, tool_use_id: &str) -> String {
    parsed_tool_result(request, tool_use_id)
        .and_then(|result| {
            result
                .pointer("/output/process_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| "missing-process-id".to_string())
}

fn non_streaming_message() -> Value {
    let recap = json!({
        "new_claims": [],
        "updated_claims": [],
        "used_claim_ids": [],
        "new_disputes": [],
    });
    json!({
        "id": "msg_fake_finalize",
        "type": "message",
        "role": "assistant",
        "model": "fake-streaming-model",
        "content": [{"type": "text", "text": recap.to_string()}],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    })
}

fn slow_compaction_message() -> Value {
    let outcome = json!({
        "committed_summary": "fake compact queue regression summary",
        "active_turn_summary": null,
    });
    json!({
        "id": "msg_fake_compaction",
        "type": "message",
        "role": "assistant",
        "model": "fake-compact-queue-model",
        "content": [{"type": "text", "text": outcome.to_string()}],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    })
}

fn streaming_messages() -> Sse<impl Stream<Item = SseItem>> {
    let (tx, rx) = mpsc::channel::<SseItem>(16);
    tokio::spawn(stream_anthropic_response(tx));
    let response_stream = stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|event| (event, rx))
    });
    Sse::new(response_stream)
}

fn short_streaming_messages() -> Sse<impl Stream<Item = SseItem>> {
    let (tx, rx) = mpsc::channel::<SseItem>(16);
    tokio::spawn(stream_short_response(tx));
    let response_stream = stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|event| (event, rx))
    });
    Sse::new(response_stream)
}

async fn stream_short_response(tx: mpsc::Sender<SseItem>) {
    for event in [
        json!({
            "type": "message_start",
            "message": {"usage": {"input_tokens": 1}}
        }),
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }),
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "text_delta",
                "text": "fake compact queue turn completed"
            }
        }),
    ] {
        if !send_event(&tx, event).await {
            return;
        }
    }

    // 给 tmux 留出稳定的 Running 帧，避免下一轮按键抢在 App 接纳当前 submission 前进入。
    tokio::time::sleep(Duration::from_secs(1)).await;

    for event in [
        json!({"type": "content_block_stop", "index": 0}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1}
        }),
        json!({"type": "message_stop"}),
    ] {
        if !send_event(&tx, event).await {
            return;
        }
    }
}

fn background_process_tool_messages() -> Sse<impl Stream<Item = SseItem>> {
    sse_from_events(vec![
        json!({
            "type": "message_start",
            "message": {"usage": {"input_tokens": 1}}
        }),
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {
                "type": "tool_use",
                "id": "toolu_fake_background_process",
                "name": "code_run",
                "input": {}
            }
        }),
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "input_json_delta",
                "partial_json": "{\"description\":\"Start the background-process fixture.\",\"script\":\"sleep 30\",\"yield_time_ms\":1000}"
            }
        }),
        json!({"type": "content_block_stop", "index": 0}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 1}
        }),
        json!({"type": "message_stop"}),
    ])
}

fn background_process_completed_messages() -> Sse<impl Stream<Item = SseItem>> {
    sse_from_events(vec![
        json!({
            "type": "message_start",
            "message": {"usage": {"input_tokens": 1}}
        }),
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }),
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "text_delta",
                "text": "Background process started."
            }
        }),
        json!({"type": "content_block_stop", "index": 0}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1}
        }),
        json!({"type": "message_stop"}),
    ])
}

fn session_recap_messages() -> Sse<impl Stream<Item = SseItem>> {
    let recap = json!({
        "new_claims": [],
        "updated_claims": [],
        "used_claim_ids": [],
        "new_disputes": [],
    });
    sse_from_events(vec![
        json!({
            "type": "message_start",
            "message": {"usage": {"input_tokens": 1}}
        }),
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }),
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "text_delta",
                "text": recap.to_string()
            }
        }),
        json!({"type": "content_block_stop", "index": 0}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1}
        }),
        json!({"type": "message_stop"}),
    ])
}

fn process_control_messages(request: &Value) -> Response {
    const SINGLE_START: &str = "toolu_process_single_start";
    const DUPLICATE_POLL: &str = "toolu_process_duplicate_poll";
    const DUPLICATE_TERMINATE: &str = "toolu_process_duplicate_terminate";
    const SURVIVOR_LIST: &str = "toolu_process_survivor_list";
    const SINGLE_TERMINATE: &str = "toolu_process_single_terminate";
    const NATURAL_EXIT: &str = "toolu_process_natural_exit";
    const PAIR_START_A: &str = "toolu_process_pair_start_a";
    const PAIR_START_B: &str = "toolu_process_pair_start_b";
    const PAIR_POLL_A: &str = "toolu_process_pair_poll_a";
    const PAIR_POLL_B: &str = "toolu_process_pair_poll_b";
    const PAIR_TERMINATE_A: &str = "toolu_process_pair_terminate_a";
    const PAIR_TERMINATE_B: &str = "toolu_process_pair_terminate_b";
    const FINAL_LIST: &str = "toolu_process_final_list";

    if let Some(final_list) = parsed_tool_result(request, FINAL_LIST) {
        let duplicate_poll = parsed_tool_result(request, DUPLICATE_POLL).unwrap_or_default();
        let duplicate_terminate =
            parsed_tool_result(request, DUPLICATE_TERMINATE).unwrap_or_default();
        let survivor_list = parsed_tool_result(request, SURVIVOR_LIST).unwrap_or_default();
        let single_terminate = parsed_tool_result(request, SINGLE_TERMINATE).unwrap_or_default();
        let natural_exit = parsed_tool_result(request, NATURAL_EXIT).unwrap_or_default();
        let pair_poll_a = parsed_tool_result(request, PAIR_POLL_A).unwrap_or_default();
        let pair_poll_b = parsed_tool_result(request, PAIR_POLL_B).unwrap_or_default();
        let pair_terminate_a = parsed_tool_result(request, PAIR_TERMINATE_A).unwrap_or_default();
        let pair_terminate_b = parsed_tool_result(request, PAIR_TERMINATE_B).unwrap_or_default();
        let single_process_id = tool_result_process_id(request, SINGLE_START);

        let duplicate_poll_ok =
            result_matches(&duplicate_poll, true, "process_running", None, None);
        let duplicate_terminate_rejected =
            result_matches(&duplicate_terminate, false, "dispatch_failure", None, None)
                && duplicate_terminate
                    .get("error")
                    .and_then(Value::as_str)
                    .is_some_and(|error| error.contains("already called for this process"));
        let survivor_running = survivor_list
            .pointer("/output/processes")
            .and_then(Value::as_array)
            .is_some_and(|processes| {
                processes.iter().any(|process| {
                    process.get("process_id").and_then(Value::as_str)
                        == Some(single_process_id.as_str())
                        && process.get("state").and_then(Value::as_str) == Some("running")
                })
            });
        let terminate_ok =
            result_matches(&single_terminate, true, "process_terminated", Some(9), None)
                && single_terminate
                    .pointer("/output/success")
                    .and_then(Value::as_bool)
                    == Some(false);
        let natural_failed = result_matches(&natural_exit, false, "process_exit", None, Some(7))
            && natural_exit
                .pointer("/output/success")
                .and_then(Value::as_bool)
                == Some(false);
        let pair_polls_ok = result_matches(&pair_poll_a, true, "process_running", None, None)
            && result_matches(&pair_poll_b, true, "process_running", None, None);
        let pair_terminates_ok =
            result_matches(&pair_terminate_a, true, "process_terminated", Some(9), None)
                && result_matches(&pair_terminate_b, true, "process_terminated", Some(9), None);
        let no_live_processes = final_list
            .pointer("/output/processes")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty);
        return text_messages(&format!(
            "PROCESS_CONTROL_RESULT duplicate_poll_ok={duplicate_poll_ok} \
duplicate_terminate_rejected={duplicate_terminate_rejected} survivor_running={survivor_running}\n\
terminate_ok={terminate_ok} natural_failed={natural_failed} pair_polls_ok={pair_polls_ok}\n\
pair_terminates_ok={pair_terminates_ok} no_live_processes={no_live_processes}"
        ))
        .into_response();
    }

    if parsed_tool_result(request, PAIR_TERMINATE_A).is_some()
        && parsed_tool_result(request, PAIR_TERMINATE_B).is_some()
    {
        return tool_messages(FINAL_LIST, "process_list", json!({})).into_response();
    }

    if parsed_tool_result(request, PAIR_POLL_A).is_some()
        && parsed_tool_result(request, PAIR_POLL_B).is_some()
    {
        return tool_messages_many(vec![
            (
                PAIR_TERMINATE_A,
                "write_stdin",
                json!({
                    "process_id": tool_result_process_id(request, PAIR_START_A),
                    "terminate": true,
                    "yield_time_ms": 3_000
                }),
            ),
            (
                PAIR_TERMINATE_B,
                "write_stdin",
                json!({
                    "process_id": tool_result_process_id(request, PAIR_START_B),
                    "terminate": true,
                    "yield_time_ms": 3_000
                }),
            ),
        ])
        .into_response();
    }

    if parsed_tool_result(request, PAIR_START_A).is_some()
        && parsed_tool_result(request, PAIR_START_B).is_some()
    {
        return tool_messages_many(vec![
            (
                PAIR_POLL_A,
                "write_stdin",
                json!({
                    "process_id": tool_result_process_id(request, PAIR_START_A),
                    "yield_time_ms": 250
                }),
            ),
            (
                PAIR_POLL_B,
                "write_stdin",
                json!({
                    "process_id": tool_result_process_id(request, PAIR_START_B),
                    "yield_time_ms": 250
                }),
            ),
        ])
        .into_response();
    }

    if parsed_tool_result(request, NATURAL_EXIT).is_some() {
        return tool_messages_many(vec![
            (
                PAIR_START_A,
                "code_run",
                json!({
                    "script": "printf PAIR_A_READY; sleep 30",
                    "yield_time_ms": 250
                }),
            ),
            (
                PAIR_START_B,
                "code_run",
                json!({
                    "script": "printf PAIR_B_READY; sleep 30",
                    "yield_time_ms": 250
                }),
            ),
        ])
        .into_response();
    }

    if parsed_tool_result(request, SINGLE_TERMINATE).is_some() {
        return tool_messages(
            NATURAL_EXIT,
            "code_run",
            json!({
                "script": "printf diagnostic >&2; exit 7",
                "yield_time_ms": 1_000
            }),
        )
        .into_response();
    }

    if parsed_tool_result(request, SURVIVOR_LIST).is_some() {
        return tool_messages(
            SINGLE_TERMINATE,
            "write_stdin",
            json!({
                "process_id": tool_result_process_id(request, SINGLE_START),
                "terminate": true,
                "yield_time_ms": 3_000
            }),
        )
        .into_response();
    }

    if parsed_tool_result(request, DUPLICATE_POLL).is_some()
        && parsed_tool_result(request, DUPLICATE_TERMINATE).is_some()
    {
        return tool_messages(SURVIVOR_LIST, "process_list", json!({})).into_response();
    }

    if parsed_tool_result(request, SINGLE_START).is_some() {
        let process_id = tool_result_process_id(request, SINGLE_START);
        return tool_messages_many(vec![
            (
                DUPLICATE_POLL,
                "write_stdin",
                json!({
                    "process_id": process_id,
                    "yield_time_ms": 250
                }),
            ),
            (
                DUPLICATE_TERMINATE,
                "write_stdin",
                json!({
                    "process_id": tool_result_process_id(request, SINGLE_START),
                    "terminate": true,
                    "yield_time_ms": 3_000
                }),
            ),
        ])
        .into_response();
    }

    tool_messages(
        SINGLE_START,
        "code_run",
        json!({
            "script": "printf SINGLE_READY; sleep 30",
            "yield_time_ms": 250
        }),
    )
    .into_response()
}

fn result_matches(
    result: &Value,
    ok: bool,
    outcome: &str,
    signal: Option<i64>,
    exit_code: Option<i64>,
) -> bool {
    result.get("ok").and_then(Value::as_bool) == Some(ok)
        && result.pointer("/outcome/kind").and_then(Value::as_str) == Some(outcome)
        && signal.is_none_or(|signal| {
            result.pointer("/outcome/signal").and_then(Value::as_i64) == Some(signal)
        })
        && exit_code.is_none_or(|exit_code| {
            result.pointer("/outcome/exit_code").and_then(Value::as_i64) == Some(exit_code)
        })
}

fn tool_messages(
    tool_use_id: &str,
    tool_name: &str,
    input: Value,
) -> Sse<impl Stream<Item = SseItem>> {
    tool_messages_many(vec![(tool_use_id, tool_name, input)])
}

fn tool_messages_many(tools: Vec<(&str, &str, Value)>) -> Sse<impl Stream<Item = SseItem>> {
    let mut events = vec![json!({
        "type": "message_start",
        "message": {"usage": {"input_tokens": 1}}
    })];
    for (index, (tool_use_id, tool_name, mut input)) in tools.into_iter().enumerate() {
        if let Some(object) = input.as_object_mut() {
            let description = match tool_name {
                "code_run" => Some("Run the deterministic process-control fixture command."),
                "write_stdin" => Some("Poll or control the deterministic fixture process."),
                _ => None,
            };
            if let Some(description) = description {
                object.insert("description".into(), Value::String(description.into()));
            }
        }
        events.extend([
            json!({
                "type": "content_block_start",
                "index": index,
                "content_block": {
                    "type": "tool_use",
                    "id": tool_use_id,
                    "name": tool_name,
                    "input": {}
                }
            }),
            json!({
                "type": "content_block_delta",
                "index": index,
                "delta": {
                    "type": "input_json_delta",
                    "partial_json": input.to_string()
                }
            }),
            json!({"type": "content_block_stop", "index": index}),
        ]);
    }
    events.extend([
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 1}
        }),
        json!({"type": "message_stop"}),
    ]);
    sse_from_events(events)
}

fn text_messages(text: &str) -> Sse<impl Stream<Item = SseItem>> {
    sse_from_events(vec![
        json!({
            "type": "message_start",
            "message": {"usage": {"input_tokens": 1}}
        }),
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }),
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "text_delta",
                "text": text
            }
        }),
        json!({"type": "content_block_stop", "index": 0}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1}
        }),
        json!({"type": "message_stop"}),
    ])
}

fn sse_from_events(events: Vec<Value>) -> Sse<impl Stream<Item = SseItem>> {
    Sse::new(stream::iter(
        events
            .into_iter()
            .map(|payload| Ok(Event::default().data(payload.to_string()))),
    ))
}

async fn stream_anthropic_response(tx: mpsc::Sender<SseItem>) {
    for event in [
        json!({
            "type": "message_start",
            "message": {"usage": {"input_tokens": 1}}
        }),
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }),
    ] {
        if !send_event(&tx, event).await {
            return;
        }
    }

    for line in 0..120 {
        if !send_event(
            &tx,
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {
                    "type": "text_delta",
                    "text": format!("[fake stream] line {line}\n")
                }
            }),
        )
        .await
        {
            return;
        }
    }

    // 保持 turn 处于 running，让 tmux 在没有并发重绘的稳定帧上检查 resize。
    tokio::time::sleep(Duration::from_secs(5)).await;

    for event in [
        json!({"type": "content_block_stop", "index": 0}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 120}
        }),
        json!({"type": "message_stop"}),
    ] {
        if !send_event(&tx, event).await {
            return;
        }
    }
}

async fn send_event(tx: &mpsc::Sender<SseItem>, payload: Value) -> bool {
    tx.send(Ok(Event::default().data(payload.to_string())))
        .await
        .is_ok()
}
