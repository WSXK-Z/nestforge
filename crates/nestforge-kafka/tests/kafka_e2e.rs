//! Kafka 传输器端到端测试（需真实 broker）。
//!
//! 设置 `KAFKA_CONNECT=host:port` 后运行；未设置时测试自动跳过（返回 ok）。
//! 需要 broker 开启自动建 topic。

use nestforge_core::Container;
use nestforge_kafka::{
    KafkaConfig, KafkaMicroserviceClient, KafkaMicroserviceConsumer, KafkaSubscription,
};
use nestforge_microservices::{MicroserviceClient, MicroserviceRegistry, TransportMetadata};

fn broker_from_env() -> Option<String> {
    std::env::var("KAFKA_CONNECT").ok()
}

fn unique_topic(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos}")
}

#[tokio::test]
async fn kafka_send_and_emit_roundtrip() {
    let Some(broker) = broker_from_env() else {
        eprintln!("KAFKA_CONNECT not set; skipping kafka e2e test");
        return;
    };

    // pattern→topic：handler 键与 topic 同名。
    let message_topic = unique_topic("nestforge.e2e.msg");
    let event_topic = unique_topic("nestforge.e2e.evt");
    let container = Container::new();
    let registry = MicroserviceRegistry::builder()
        .message(message_topic.clone(), |payload: String, _ctx| async move {
            Ok(payload)
        })
        .event(event_topic.clone(), |_payload: (), _ctx| async move { Ok(()) })
        .build();

    let consumer = KafkaMicroserviceConsumer::new(KafkaConfig::new([broker.clone()]))
        .subscribe(KafkaSubscription::new(message_topic.clone()))
        .subscribe(KafkaSubscription::new(event_topic.clone()));

    let handle = consumer
        .spawn(container, registry)
        .await
        .expect("consumer should spawn");
    assert_eq!(handle.task_count(), 2);

    let client = KafkaMicroserviceClient::new(
        KafkaConfig::new([broker.clone()]).with_request_timeout(std::time::Duration::from_secs(10)),
    )
    .with_default_metadata(TransportMetadata::new().insert("env", "test"));

    // 给消费者一点时间建立连接。
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    client
        .emit(&event_topic, ())
        .await
        .expect("emit should succeed");

    // request-reply：入站把结果回发到客户端唯一 reply topic。
    let reply: String = client
        .send(&message_topic, "hello".to_string())
        .await
        .expect("send should succeed");
    assert_eq!(reply, "hello");

    handle.shutdown();
}
