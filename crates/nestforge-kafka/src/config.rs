//! Kafka 传输器配置与订阅描述。

use std::time::Duration;

use rskafka::client::partition::Compression;

/// Kafka broker 连接与传输参数。
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// bootstrap broker 列表，形如 `"host:port"`。
    pub brokers: Vec<String>,
    /// 可选 client id，默认由 rskafka 提供。
    pub client_id: Option<String>,
    /// 生产时使用的压缩算法。
    pub compression: KafkaCompression,
    /// 默认写入分区（pattern→topic 时默认 0）。
    pub default_partition: i32,
    /// 出站 `send`（request-reply）等待响应的超时。
    pub request_timeout: Duration,
    /// 可选的 topic 前缀（pattern→topic 映射：`topic = prefix + pattern`）。
    pub topic_prefix: String,
    /// 回复 topic 前缀；客户端会追加随机后缀以得到唯一 topic。
    pub reply_topic_prefix: String,
}

impl KafkaConfig {
    /// 用 broker 列表创建配置。
    pub fn new(brokers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            brokers: brokers.into_iter().map(Into::into).collect(),
            client_id: None,
            compression: KafkaCompression::NoCompression,
            default_partition: 0,
            request_timeout: Duration::from_secs(30),
            topic_prefix: String::new(),
            reply_topic_prefix: "nestforge.reply.".to_string(),
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn with_compression(mut self, compression: KafkaCompression) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_default_partition(mut self, partition: i32) -> Self {
        self.default_partition = partition;
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.topic_prefix = prefix.into();
        self
    }

    pub fn with_reply_topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.reply_topic_prefix = prefix.into();
        self
    }

    /// 把 pattern 映射为 Kafka topic（pattern→topic，支持前缀）。
    pub fn topic_for(&self, pattern: &str) -> String {
        format!("{}{}", self.topic_prefix, pattern)
    }
}

/// 生产压缩算法（映射到 rskafka 的 `Compression`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaCompression {
    #[default]
    NoCompression,
    Gzip,
    Lz4,
    Snappy,
    Zstd,
}

impl From<KafkaCompression> for Compression {
    fn from(value: KafkaCompression) -> Self {
        match value {
            KafkaCompression::NoCompression => Compression::NoCompression,
            KafkaCompression::Gzip => Compression::Gzip,
            KafkaCompression::Lz4 => Compression::Lz4,
            KafkaCompression::Snappy => Compression::Snappy,
            KafkaCompression::Zstd => Compression::Zstd,
        }
    }
}

/// 消费起点。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaStartOffset {
    /// 最早可用记录。
    Earliest,
    /// 只处理新数据。
    Latest,
    /// 指定 offset。
    At(i64),
}

/// 一条 topic 订阅（含分区与起点）。
#[derive(Debug, Clone)]
pub struct KafkaSubscription {
    pub topic: String,
    pub partition: i32,
    pub start: KafkaStartOffset,
    /// 单次 fetch 最大等待毫秒。
    pub max_wait_ms: i32,
}

impl KafkaSubscription {
    /// 订阅某 topic（partition 0，从 earliest 开始）。
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partition: 0,
            start: KafkaStartOffset::Earliest,
            max_wait_ms: 500,
        }
    }

    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }

    pub fn start(mut self, start: KafkaStartOffset) -> Self {
        self.start = start;
        self
    }

    pub fn max_wait_ms(mut self, max_wait_ms: i32) -> Self {
        self.max_wait_ms = max_wait_ms;
        self
    }
}

/// 便捷：把一个 pattern（topic）展开成单分区订阅。
pub fn subscription(pattern: impl Into<String>) -> KafkaSubscription {
    KafkaSubscription::new(pattern)
}
