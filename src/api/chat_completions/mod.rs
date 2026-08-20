//! OpenAI-compatible Chat Completions 协议层。
//!
//! 本模块只负责通用 `/chat/completions` HTTP、DTO、SSE 解析和 retry。
//! 上层 provider adapter / router rerank 负责业务 prompt、canonical message 转换和输出解释。

mod client;
mod protocol;
mod streaming;

pub(crate) use client::is_stream_failure;
pub use client::{ChatCompletionsClient, ChatCompletionsError, ChatStreamEvent};
pub use protocol::{
    ChatCompletionChoice, ChatCompletionMessage, ChatCompletionRequest, ChatCompletionResponse,
    ChatContentPart, ChatFileData, ChatFinishReason, ChatImageUrl, ChatMessage, ChatMessageContent,
    ChatStreamOptions, ChatTool, ChatToolCall, ChatToolCallFunction,
};
