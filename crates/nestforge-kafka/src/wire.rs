//! Kafka 记录线格式与 NestForge 信封的转换。
//!
//! pattern→topic 语义：topic = pattern。记录用 **header** 标注类型：
//! - `message`：值为 `MessageEnvelope` JSON（请求-响应，可带回复元数据）
//! - `event`：值为 `EventEnvelope` JSON（即发即忘）
//! - `reply` / `reply-error`：值为响应信封 JSON（回发到 `META_REPLY_TOPIC`）
//!
//! 回复信息通过信封的 `TransportMetadata` 保留键传递（见下方 `META_*`）。

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};

use nestforge_microservices::{EventEnvelope, MessageEnvelope, TransportMetadata};

/// 记录 header 中标识类型的键。
pub const KIND_KEY: &str = "nestforge-kind";
pub const KIND_MESSAGE: &str = "message";
pub const KIND_EVENT: &str = "event";
pub const KIND_REPLY: &str = "reply";
pub const KIND_REPLY_ERROR: &str = "reply-error";

/// 元数据保留键：回复目标 topic 与关联 id。
pub const META_REPLY_TOPIC: &str = "nestforge.reply_topic";
pub const META_CORRELATION_ID: &str = "nestforge.correlation_id";

/// 构造带 kind header 的记录头。
pub fn kind_headers(kind: &str) -> BTreeMap<String, Vec<u8>> {
    let mut headers = BTreeMap::new();
    headers.insert(KIND_KEY.to_string(), kind.as_bytes().to_vec());
    headers
}

/// 从记录头读出 kind。
pub fn kind_of(headers: &BTreeMap<String, Vec<u8>>) -> Option<String> {
    headers
        .get(KIND_KEY)
        .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
}

pub fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).context("failed to serialize record payload")
}

pub fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).context("failed to deserialize record payload")
}

/// 从 `TransportMetadata` 读出保留键（可能缺失）。
pub fn metadata_get(metadata: &TransportMetadata, key: &str) -> Option<String> {
    metadata.values.get(key).cloned()
}

/// 构建一个不含保留键、只含用户元数据的副本（用于 handler 的 ctx）。
pub fn user_metadata(metadata: &TransportMetadata) -> TransportMetadata {
    TransportMetadata {
        values: metadata
            .values
            .iter()
            .filter(|(key, _)| {
                *key != META_REPLY_TOPIC && *key != META_CORRELATION_ID
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    }
}

/// 在信封元数据上附加回复目标与关联 id。
pub fn with_reply_meta(mut envelope_metadata: TransportMetadata, reply_topic: &str, correlation_id: &str) -> TransportMetadata {
    envelope_metadata
        .values
        .insert(META_REPLY_TOPIC.to_string(), reply_topic.to_string());
    envelope_metadata
        .values
        .insert(META_CORRELATION_ID.to_string(), correlation_id.to_string());
    envelope_metadata
}

/// 响应信封：message handler 的返回值回传给请求方。
pub fn reply_envelope(pattern: &str, payload: serde_json::Value, correlation_id: &str) -> Result<Vec<u8>> {
    let metadata = with_reply_meta(TransportMetadata::default(), "", correlation_id);
    let envelope = MessageEnvelope {
        pattern: pattern.to_string(),
        payload,
        metadata,
    };
    encode_json(&envelope)
}

/// 错误响应信封。
pub fn error_reply_envelope(pattern: &str, error: &str, correlation_id: &str) -> Result<Vec<u8>> {
    reply_envelope(
        pattern,
        serde_json::json!({ "message": error }),
        correlation_id,
    )
}

/// 解析响应信封（reply / reply-error 共用 `MessageEnvelope` 结构）。
pub fn parse_reply(bytes: &[u8]) -> Result<MessageEnvelope> {
    decode_json(bytes)
}

/// 把消息信封编码成 event 记录值。
pub fn encode_event(envelope: &EventEnvelope) -> Result<Vec<u8>> {
    encode_json(envelope)
}

/// 把消息信封编码成 message 记录值。
pub fn encode_message(envelope: &MessageEnvelope) -> Result<Vec<u8>> {
    encode_json(envelope)
}

/// 解析消息信封。
pub fn parse_message(bytes: &[u8]) -> Result<MessageEnvelope> {
    decode_json(bytes)
}

/// 解析事件信封。
pub fn parse_event(bytes: &[u8]) -> Result<EventEnvelope> {
    decode_json(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nestforge_microservices::{EventEnvelope, MessageEnvelope, TransportMetadata};
    use serde_json::json;

    #[test]
    fn message_envelope_roundtrip() {
        let mut metadata = TransportMetadata::new().insert("a", "1");
        metadata = with_reply_meta(metadata, "replies.topic", "cid-1");
        let envelope = MessageEnvelope {
            pattern: "calc.add".to_string(),
            payload: json!({"a": 1, "b": 2}),
            metadata: metadata.clone(),
        };

        let bytes = encode_message(&envelope).expect("encode");
        let decoded = parse_message(&bytes).expect("decode");
        assert_eq!(decoded.pattern, "calc.add");
        assert_eq!(decoded.payload, json!({"a": 1, "b": 2}));
        assert_eq!(
            metadata_get(&decoded.metadata, META_REPLY_TOPIC).as_deref(),
            Some("replies.topic")
        );
        assert_eq!(
            metadata_get(&decoded.metadata, META_CORRELATION_ID).as_deref(),
            Some("cid-1")
        );
    }

    #[test]
    fn event_envelope_roundtrip() {
        let envelope = EventEnvelope {
            pattern: "users.created".to_string(),
            payload: json!({"id": 7}),
            metadata: TransportMetadata::default(),
        };
        let decoded = parse_event(&encode_event(&envelope).expect("encode")).expect("decode");
        assert_eq!(decoded.pattern, "users.created");
        assert_eq!(decoded.payload, json!({"id": 7}));
    }

    #[test]
    fn user_metadata_strips_reserved_keys() {
        let mut metadata = TransportMetadata::new().insert("env", "test");
        metadata = with_reply_meta(metadata, "replies.topic", "cid-1");
        let clean = user_metadata(&metadata);
        assert_eq!(clean.values.get("env").map(String::as_str), Some("test"));
        assert!(clean.values.get(META_REPLY_TOPIC).is_none());
        assert!(clean.values.get(META_CORRELATION_ID).is_none());
    }

    #[test]
    fn kind_headers_roundtrip() {
        let headers = kind_headers(KIND_EVENT);
        assert_eq!(kind_of(&headers).as_deref(), Some(KIND_EVENT));
    }
}
