//! 共享的低层 rskafka 访问层：惰性建连 + (topic, partition) 客户端缓存。
//!
//! 出站客户端与入站消费者共用同一个 `KafkaTransport`（可 Clone），
//! 这样回复/转发能复用同一组 broker 连接。

use std::{
    collections::HashMap,
    sync::Arc,
};

use anyhow::{Context, Result};
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::{partition::PartitionClient, Client, ClientBuilder};
use rskafka::record::Record;
use tokio::sync::Mutex;

use crate::config::KafkaConfig;

/// 一条 Kafka 记录（值序列化由调用方完成）。
pub fn build_record(kind: &str, value: Vec<u8>, key: Option<Vec<u8>>) -> Record {
    Record {
        key,
        value: Some(value),
        headers: crate::wire::kind_headers(kind),
        timestamp: chrono::Utc::now(),
    }
}

/// 惰性 broker 客户端与分区缓存。
#[derive(Clone)]
pub struct KafkaTransport {
    config: KafkaConfig,
    client: Arc<Mutex<Option<Arc<Client>>>>,
    partitions: Arc<Mutex<HashMap<(String, i32), Arc<PartitionClient>>>>,
}

impl KafkaTransport {
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            config,
            client: Arc::new(Mutex::new(None)),
            partitions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn config(&self) -> &KafkaConfig {
        &self.config
    }

    /// 惰性建立（或复用）到 broker 的连接。
    pub async fn client(&self) -> Result<Arc<Client>> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(Arc::clone(client));
        }
        let mut builder = ClientBuilder::new(self.config.brokers.clone());
        if let Some(client_id) = self.config.client_id.as_deref() {
            builder = builder.client_id(client_id.to_owned());
        }
        let client = builder
            .build()
            .await
            .context("failed to connect to kafka brokers")?;
        let client = Arc::new(client);
        *guard = Some(Arc::clone(&client));
        Ok(client)
    }

    /// 取得（或缓存创建）某 (topic, partition) 的分区客户端。
    pub async fn partition_client(&self, topic: &str, partition: i32) -> Result<Arc<PartitionClient>> {
        let key = (topic.to_string(), partition);
        {
            let guard = self.partitions.lock().await;
            if let Some(pc) = guard.get(&key) {
                return Ok(Arc::clone(pc));
            }
        }

        let client = self.client().await?;
        let pc = client
            .partition_client(topic.to_string(), partition, UnknownTopicHandling::Retry)
            .await
            .with_context(|| format!("failed to open partition {topic}:{partition}"))?;
        let pc = Arc::new(pc);

        let mut guard = self.partitions.lock().await;
        guard.insert(key, Arc::clone(&pc));
        Ok(pc)
    }

    /// 向指定分区生产一条记录。
    pub async fn produce(
        &self,
        topic: &str,
        partition: i32,
        record: Record,
    ) -> Result<()> {
        let pc = self.partition_client(topic, partition).await?;
        pc.produce(vec![record], self.config.compression.into())
            .await
            .with_context(|| format!("failed to produce to {topic}:{partition}"))?;
        Ok(())
    }
}
