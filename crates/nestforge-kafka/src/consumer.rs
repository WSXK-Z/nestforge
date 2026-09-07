//! Kafka 入站微服务消费者。
//!
//! 订阅一组 topic（pattern→topic），把记录分发进 `MicroserviceRegistry`：
//! - `message` 记录 → `dispatch_message`；若携带回复元数据，把结果回发到 reply topic。
//! - `event` 记录 → `dispatch_event`（即发即忘）。
//!
//! 说明：rskafka 无消费组，偏移由 `StreamConsumer` 在内存中推进，重启后按订阅配置的
//! 起点重新开始。每个 (topic, partition) 一个后台任务。

use anyhow::{Context, Result};
use futures::StreamExt;
use nestforge_core::Container;
use nestforge_microservices::{
    MessageEnvelope, MicroserviceContext, MicroserviceRegistry,
};
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::record::Record;

use crate::config::{KafkaConfig, KafkaStartOffset, KafkaSubscription};
use crate::transport::{build_record, KafkaTransport};
use crate::wire::{
    error_reply_envelope, kind_of, metadata_get, parse_event, parse_message, reply_envelope,
    user_metadata, KIND_EVENT, KIND_MESSAGE, KIND_REPLY, KIND_REPLY_ERROR, META_CORRELATION_ID,
    META_REPLY_TOPIC,
};

/// 本传输在 `MicroserviceContext` 中使用的 transport 标识。
pub const KAFKA_TRANSPORT: &str = "kafka";

/// 入站消费者（可订阅多条 topic 后 `spawn`）。
#[derive(Clone)]
pub struct KafkaMicroserviceConsumer {
    transport: KafkaTransport,
    subscriptions: Vec<KafkaSubscription>,
}

impl KafkaMicroserviceConsumer {
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            transport: KafkaTransport::new(config),
            subscriptions: Vec::new(),
        }
    }

    pub fn subscribe(mut self, subscription: KafkaSubscription) -> Self {
        self.subscriptions.push(subscription);
        self
    }

    pub fn subscriptions(&self) -> &[KafkaSubscription] {
        &self.subscriptions
    }

    pub fn transport(&self) -> &KafkaTransport {
        &self.transport
    }

    /// 连接 broker 并为每条订阅启动一个后台消费任务。
    pub async fn spawn(self, container: Container, registry: MicroserviceRegistry) -> Result<KafkaConsumerHandle> {
        if self.subscriptions.is_empty() {
            anyhow::bail!("no kafka subscriptions configured");
        }
        // 先显式连接，把 broker 错误尽早暴露给调用方。
        self.transport.client().await?;

        let mut tasks = Vec::new();
        for subscription in self.subscriptions {
            let pc = self
                .transport
                .partition_client(&subscription.topic, subscription.partition)
                .await
                .with_context(|| {
                    format!(
                        "failed to open subscription {}:{}",
                        subscription.topic, subscription.partition
                    )
                })?;
            let start_offset = match subscription.start {
                KafkaStartOffset::Earliest => StartOffset::Earliest,
                KafkaStartOffset::Latest => StartOffset::Latest,
                KafkaStartOffset::At(offset) => StartOffset::At(offset),
            };
            let max_wait_ms = subscription.max_wait_ms;
            let container = container.clone();
            let registry = registry.clone();
            let transport = self.transport.clone();

            tasks.push(tokio::spawn(async move {
                let mut stream = StreamConsumerBuilder::new(pc, start_offset)
                    .with_max_wait_ms(max_wait_ms)
                    .build();
                while let Some(item) = stream.next().await {
                    match item {
                        Ok((record_and_offset, _watermark)) => {
                            handle_record(
                                &container,
                                &registry,
                                &transport,
                                record_and_offset.record,
                            )
                            .await;
                        }
                        Err(error) => {
                            eprintln!("kafka consumer error on {}: {error}", subscription.topic);
                            break;
                        }
                    }
                }
            }));
        }

        Ok(KafkaConsumerHandle { tasks })
    }
}

/// 入站消费任务的句柄。
pub struct KafkaConsumerHandle {
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl KafkaConsumerHandle {
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// 停止所有后台消费任务。
    pub fn shutdown(self) {
        for task in self.tasks {
            task.abort();
        }
    }
}

/// 处理单条记录：按 kind 分发给注册表，并在 message 需要回复时回发结果。
async fn handle_record(
    container: &Container,
    registry: &MicroserviceRegistry,
    transport: &KafkaTransport,
    record: Record,
) {
    let kind = kind_of(&record.headers).unwrap_or_default();
    let value_bytes = record.value.as_deref().unwrap_or(&[]);

    match kind.as_str() {
        KIND_MESSAGE => {
            let envelope = match parse_message(value_bytes) {
                Ok(envelope) => envelope,
                Err(_) => return,
            };
            dispatch_inbound_message(container, registry, transport, envelope).await;
        }
        KIND_EVENT => {
            let envelope = match parse_event(value_bytes) {
                Ok(envelope) => envelope,
                Err(_) => return,
            };
            let pattern = envelope.pattern.clone();
            let context = MicroserviceContext::new(
                container.clone(),
                KAFKA_TRANSPORT,
                pattern,
                user_metadata(&envelope.metadata),
            );
            if let Err(error) = registry.dispatch_event(envelope, context).await {
                eprintln!("kafka event handler failed: {error}");
            }
        }
        _ => {}
    }
}

async fn dispatch_inbound_message(
    container: &Container,
    registry: &MicroserviceRegistry,
    transport: &KafkaTransport,
    envelope: MessageEnvelope,
) {
    let pattern = envelope.pattern.clone();
    let reply_topic = metadata_get(&envelope.metadata, META_REPLY_TOPIC);
    let correlation_id = metadata_get(&envelope.metadata, META_CORRELATION_ID);
    let context = MicroserviceContext::new(
        container.clone(),
        KAFKA_TRANSPORT,
        pattern.clone(),
        user_metadata(&envelope.metadata),
    );

    match registry.dispatch_message(envelope, context).await {
        Ok(payload) => {
            if let (Some(reply_topic), Some(correlation_id)) = (&reply_topic, &correlation_id) {
                let value = reply_envelope(&pattern, payload, correlation_id);
                match value {
                    Ok(bytes) => {
                        let _ = transport
                            .produce(
                                reply_topic,
                                0,
                                build_record(KIND_REPLY, bytes, None),
                            )
                            .await;
                    }
                    Err(error) => eprintln!("kafka failed to build reply: {error}"),
                }
            }
        }
        Err(error) => {
            if let (Some(reply_topic), Some(correlation_id)) = (&reply_topic, &correlation_id) {
                let message = error.to_string();
                if let Ok(bytes) = error_reply_envelope(&pattern, &message, correlation_id) {
                    let _ = transport
                        .produce(
                            reply_topic,
                            0,
                            build_record(KIND_REPLY_ERROR, bytes, None),
                        )
                        .await;
                }
            } else {
                eprintln!("kafka message handler failed for `{pattern}`: {error}");
            }
        }
    }
}
