//! Kafka 出站微服务客户端。
//!
//! 实现 `MicroserviceClient`：
//! - `emit(pattern, payload)`：EventEnvelope 生产到 `topic = pattern`（即发即忘）。
//! - `send(pattern, payload)`：MessageEnvelope 生产到 `topic = pattern`，随后监听本客户端
//!   唯一的 reply topic，按 correlation id 取回响应（request-reply）。
//!
//! 回复通道依赖 broker 自动建 topic（`auto.create.topics.enable`）或预先建好
//! `reply_topic_prefix + 随机后缀` 对应的 topic。偏移不落盘，重启从最早重新读。

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::partition::PartitionClient;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use nestforge_microservices::{EventEnvelope, MessageEnvelope, MicroserviceClient, TransportMetadata};

use crate::config::KafkaConfig;
use crate::transport::{build_record, KafkaTransport};
use crate::wire::{
    encode_event, encode_message, kind_of, metadata_get, parse_reply, with_reply_meta,
    KIND_EVENT, KIND_MESSAGE, KIND_REPLY, KIND_REPLY_ERROR, META_CORRELATION_ID,
};

type PendingReply = tokio::sync::oneshot::Sender<Result<Value, String>>;

/// Kafka 出站客户端（可 Clone，适合注册为 DI provider）。
#[derive(Clone)]
pub struct KafkaMicroserviceClient {
    transport: KafkaTransport,
    default_metadata: TransportMetadata,
    reply_topic: String,
    pending: Arc<std::sync::Mutex<HashMap<String, PendingReply>>>,
    reply_task: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl KafkaMicroserviceClient {
    /// 惰性建连构造（首调用才真正连接 broker）。
    pub fn new(config: KafkaConfig) -> Self {
        let reply_topic = format!("{}{}", config.reply_topic_prefix, unique_suffix());
        Self {
            transport: KafkaTransport::new(config),
            default_metadata: TransportMetadata::default(),
            reply_topic,
            pending: Arc::new(std::sync::Mutex::new(HashMap::new())),
            reply_task: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// 为每次调用附加默认元数据。
    pub fn with_default_metadata(mut self, metadata: TransportMetadata) -> Self {
        self.default_metadata = metadata;
        self
    }

    /// 返回底层传输访问层（可复用连接）。
    pub fn transport(&self) -> &KafkaTransport {
        &self.transport
    }

    /// 显式连接 broker（可选；produce 时会自动惰性连接）。
    pub async fn connect(&self) -> Result<()> {
        self.transport.client().await.map(|_| ())
    }

    /// 返回本客户端唯一的 reply topic 名。
    pub fn reply_topic(&self) -> &str {
        &self.reply_topic
    }
}

impl MicroserviceClient for KafkaMicroserviceClient {
    fn send<Payload, Response>(
        &self,
        pattern: impl Into<String>,
        payload: Payload,
    ) -> Pin<Box<dyn Future<Output = Result<Response>> + Send>>
    where
        Payload: Serialize + Send + 'static,
        Response: DeserializeOwned + Send + 'static,
    {
        let pattern = pattern.into();
        let topic = self.transport.config().topic_for(&pattern);
        let partition = self.transport.config().default_partition;
        let timeout = self.transport.config().request_timeout;
        let correlation_id = next_correlation_id();
        let reply_topic = self.reply_topic.clone();
        let default_metadata = self.default_metadata.clone();
        let transport = self.transport.clone();
        let pending = Arc::clone(&self.pending);
        let reply_task = Arc::clone(&self.reply_task);

        let (tx, rx) = tokio::sync::oneshot::channel();
        pending
            .lock()
            .unwrap()
            .insert(correlation_id.clone(), tx);

        Box::pin(async move {
            // 确保唯一的 reply 消费任务在跑（并发 send 只建一次）。
            {
                let mut guard = reply_task.lock().await;
                if guard.is_none() {
                    let pc = transport.partition_client(&reply_topic, 0).await?;
                    let task = tokio::spawn(reply_loop(pc, Arc::clone(&pending)));
                    *guard = Some(task);
                }
            }

            let value = serde_json::to_value(&payload)
                .context("failed to serialize outbound message payload")?;
            let metadata = with_reply_meta(default_metadata, &reply_topic, &correlation_id);
            let envelope = MessageEnvelope {
                pattern: pattern.clone(),
                payload: value,
                metadata,
            };
            transport
                .produce(
                    &topic,
                    partition,
                    build_record(KIND_MESSAGE, encode_message(&envelope)?, None),
                )
                .await
                .with_context(|| format!("failed to produce message to `{topic}`"))?;

            let result = tokio::time::timeout(timeout, rx).await;
            pending.lock().unwrap().remove(&correlation_id);

            match result {
                Err(_) => Err(anyhow!(
                    "kafka reply timed out after {timeout:?} for pattern `{pattern}`"
                )),
                Ok(Err(_)) => Err(anyhow!("kafka reply channel closed for pattern `{pattern}`")),
                Ok(Ok(Err(message))) => Err(anyhow!("remote error for `{pattern}`: {message}")),
                Ok(Ok(Ok(value))) => serde_json::from_value(value)
                    .context("failed to deserialize kafka reply response"),
            }
        })
    }

    fn emit<Payload>(
        &self,
        pattern: impl Into<String>,
        payload: Payload,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
    where
        Payload: Serialize + Send + 'static,
    {
        let pattern = pattern.into();
        let topic = self.transport.config().topic_for(&pattern);
        let partition = self.transport.config().default_partition;
        let transport = self.transport.clone();
        let default_metadata = self.default_metadata.clone();

        Box::pin(async move {
            let value = serde_json::to_value(&payload)
                .context("failed to serialize outbound event payload")?;
            let envelope = EventEnvelope {
                pattern: pattern.clone(),
                payload: value,
                metadata: default_metadata,
            };
            transport
                .produce(
                    &topic,
                    partition,
                    build_record(KIND_EVENT, encode_event(&envelope)?, None),
                )
                .await
                .with_context(|| format!("failed to produce event to `{topic}`"))?;
            Ok(())
        })
    }
}

/// 后台 reply 消费循环：把到达本客户端 reply topic 的响应按 correlation id 分发。
async fn reply_loop(
    pc: Arc<PartitionClient>,
    pending: Arc<std::sync::Mutex<HashMap<String, PendingReply>>>,
) {
    let mut stream = StreamConsumerBuilder::new(pc, StartOffset::Earliest)
        .with_max_wait_ms(500)
        .build();

    while let Some(item) = stream.next().await {
        match item {
            Ok((record_and_offset, _watermark)) => {
                let kind = kind_of(&record_and_offset.record.headers).unwrap_or_default();
                let value_bytes = record_and_offset.record.value.as_deref().unwrap_or(&[]);
                match kind.as_str() {
                    KIND_REPLY | KIND_REPLY_ERROR => {
                        let envelope = match parse_reply(value_bytes) {
                            Ok(envelope) => envelope,
                            Err(_) => continue,
                        };
                        let Some(correlation_id) =
                            metadata_get(&envelope.metadata, META_CORRELATION_ID)
                        else {
                            continue;
                        };
                        let mut guard = pending.lock().unwrap();
                        if let Some(tx) = guard.remove(&correlation_id) {
                            let outcome = if kind == KIND_REPLY {
                                Ok(envelope.payload)
                            } else {
                                Err(error_message(&envelope.payload))
                            };
                            let _ = tx.send(outcome);
                        }
                    }
                    _ => {}
                }
            }
            Err(error) => {
                eprintln!("kafka reply consumer error: {error}");
                break;
            }
        }
    }
}

fn error_message(payload: &Value) -> String {
    payload
        .get("message")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "unknown kafka reply error".to_string())
}

fn next_correlation_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

fn unique_suffix() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}
