# Kafka

NestForge 提供基于 **rskafka**（纯 Rust、免 C 工具链）的 Kafka 传输器（`nestforge-kafka` crate，feature `kafka`）。

## 启用

```toml
nestforge = { version = "1", features = ["kafka"] }   # 隐式启用 microservices
```

## 语义：pattern → topic

- **topic = pattern**（可选 `with_topic_prefix` 前缀）。
- 出站 `send` / `emit` 发往 `topic = pattern`。
- 入站按记录头的 `nestforge-kind` 分发：`message` → `dispatch_message`，`event` → `dispatch_event`。
- `send` 是 request-reply：请求携带 reply topic + correlation id，入站处理完把结果回发。

## 出站：KafkaMicroserviceClient

```rust
use nestforge::prelude::*;

let client = nestforge::KafkaMicroserviceClient::new(
    nestforge::KafkaConfig::new(["localhost:9092"])
        .with_request_timeout(std::time::Duration::from_secs(30)),
);

let count: usize = client.send("users.count", ()).await?;         // request-reply
client.emit("users.created", CreateUserEvent { id: 7 }).await?;   // fire-and-forget
```

- 惰性建连；`KafkaMicroserviceClient` 可 Clone，适合注册为 DI provider。
- 每个客户端实例有唯一 reply topic（`reply_topic_prefix + 随机后缀`）。

## 入站：KafkaMicroserviceConsumer

```rust
let handle = nestforge::KafkaMicroserviceConsumer::new(
    nestforge::KafkaConfig::new(["localhost:9092"]),
)
.subscribe(nestforge::KafkaSubscription::new("users.count"))
.subscribe(nestforge::KafkaSubscription::new("users.created"))
.spawn(container, registry)   // container: DI Container；registry: MicroserviceRegistry
.await?;

handle.shutdown();  // 停止后台消费任务
```

入站 handler 通过 `ctx.resolve::<T>()` 使用 DI；`ctx.transport()` 为 `"kafka"`。

## 限制与注意

- rskafka 无消费组 / rebalance / 事务；偏移在内存推进，**不落盘**，重启按订阅起点（默认 earliest）重读。
- 默认每个 topic 只处理 partition 0（`KafkaConfig::default_partition`），消费端与生产端需保持一致。
- 需 broker 开启自动建 topic（`auto.create.topics.enable`）或预先创建主题；主题创建对 `send` 的 reply topic 尤其必要。
- 端到端测试：设置 `KAFKA_CONNECT=host:port` 后运行
  `cargo test -p nestforge-kafka --test kafka_e2e`（未设置时自动跳过）。
