//! NestForge Kafka 传输器（基于 rskafka）。
//!
//! 在 NestForge 的微服务注册表之上提供 Kafka 传输（pattern→topic）：
//!
//! - 出站 [`KafkaMicroserviceClient`]：实现 `MicroserviceClient`
//!   - `emit(pattern, payload)` → `EventEnvelope` 生产到 `topic = pattern`
//!   - `send(pattern, payload)` → `MessageEnvelope` 生产到 `topic = pattern`，
//!     经本客户端唯一的 reply topic 按 correlation id 取回响应
//! - 入站 [`KafkaMicroserviceConsumer`]：订阅一组 topic，按记录头 `kind` 分发进
//!   `MicroserviceRegistry`，并回发 message 的结果
//!
//! 前提：目标 broker 开启自动建 topic（`auto.create.topics.enable`）或主题已预先创建；
//! 偏移在内存推进、不落盘。rskafka 为低层客户端（无消费组 / 自动 rebalance）。

mod config;
mod consumer;
mod transport;
mod wire;

pub use config::{
    subscription, KafkaCompression, KafkaConfig, KafkaStartOffset, KafkaSubscription,
};
pub use consumer::{
    KafkaConsumerHandle, KafkaMicroserviceConsumer, KAFKA_TRANSPORT,
};
pub use transport::KafkaTransport;
pub use client::KafkaMicroserviceClient;
mod client;
