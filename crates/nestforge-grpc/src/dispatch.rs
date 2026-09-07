//! NestForge 通用 gRPC 微服务通道。
//!
//! 在强类型 gRPC（proto）之上提供一层"按 pattern + JSON 载荷"的通用通道，
//! 使任意两个 NestForge 节点无需为每个 RPC 手写 tonic 服务即可互调：
//!
//! - 入站：`GrpcDispatchService` 实现生成的 `NestForgeDispatch` trait，
//!   把远端 `Invoke`/`Emit` 信封分发进本节点的 `MicroserviceRegistry`。
//! - 出站：`GrpcMicroserviceClient` 实现 `MicroserviceClient` trait，
//!   通过同一通道把 `send`/`emit` 发往远端节点的注册表。

use std::{collections::HashMap, future::Future, pin::Pin};

use anyhow::{anyhow, Context, Result};
use nestforge_core::Container;
use nestforge_microservices::{
    EventEnvelope, MessageEnvelope, MicroserviceClient, MicroserviceContext,
    MicroserviceRegistry, TransportMetadata,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

/// 本通道在 `MicroserviceContext` 中使用的 transport 标识。
pub const GRPC_TRANSPORT: &str = "grpc";

/// `proto/nestforge.proto` 生成的 tonic / prost 绑定。
pub mod proto {
    tonic::include_proto!("nestforgedispatch");
}

pub use proto::nest_forge_dispatch_client::NestForgeDispatchClient;
pub use proto::nest_forge_dispatch_server::{NestForgeDispatch, NestForgeDispatchServer};
pub use proto::{EmitRequest, EmitResponse, InvokeRequest, InvokeResponse};

// ---------------------------------------------------------------------------
// metadata 转换
// ---------------------------------------------------------------------------

fn metadata_to_map(metadata: &TransportMetadata) -> HashMap<String, String> {
    metadata
        .values
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn metadata_from_map(values: HashMap<String, String>) -> TransportMetadata {
    TransportMetadata {
        values: values.into_iter().collect(),
    }
}

fn map_dispatch_error(err: anyhow::Error) -> Status {
    Status::internal(err.to_string())
}

// ---------------------------------------------------------------------------
// 入站：GrpcDispatchService
// ---------------------------------------------------------------------------

/// 入站通用通道服务：把远端发来的 `Invoke`/`Emit` 分发给本地注册表。
///
/// 需要同时提供本节点的 DI 容器（让 pattern handler 能 `ctx.resolve<T>()`）
/// 与本地 `MicroserviceRegistry`。
#[derive(Clone)]
pub struct GrpcDispatchService {
    container: Container,
    registry: MicroserviceRegistry,
}

impl GrpcDispatchService {
    /// 用 DI 容器与注册表构建入站服务。
    pub fn new(container: Container, registry: MicroserviceRegistry) -> Self {
        Self {
            container,
            registry,
        }
    }

    /// 返回 DI 容器引用。
    pub fn container(&self) -> &Container {
        &self.container
    }

    /// 返回注册表引用。
    pub fn registry(&self) -> &MicroserviceRegistry {
        &self.registry
    }

    /// 包装成 tonic 的 `NestForgeDispatchServer`，可直接 `.add_service(...)`。
    pub fn into_server(self) -> NestForgeDispatchServer<Self> {
        NestForgeDispatchServer::new(self)
    }
}

#[tonic::async_trait]
impl NestForgeDispatch for GrpcDispatchService {
    async fn invoke(
        &self,
        request: Request<InvokeRequest>,
    ) -> Result<Response<InvokeResponse>, Status> {
        let inner = request.into_inner();
        let payload = serde_json::from_slice(&inner.payload)
            .map_err(|err| Status::invalid_argument(format!("invalid JSON payload: {err}")))?;
        let metadata = metadata_from_map(inner.metadata);
        let envelope = MessageEnvelope {
            pattern: inner.pattern,
            payload,
            metadata: metadata.clone(),
        };
        let context = MicroserviceContext::new(
            self.container.clone(),
            GRPC_TRANSPORT,
            envelope.pattern.clone(),
            metadata,
        );

        let response = self
            .registry
            .dispatch_message(envelope, context)
            .await
            .map_err(map_dispatch_error)?;

        let payload = serde_json::to_vec(&response)
            .map_err(|err| Status::internal(format!("failed to serialize response: {err}")))?;
        Ok(Response::new(InvokeResponse { payload }))
    }

    async fn emit(&self, request: Request<EmitRequest>) -> Result<Response<EmitResponse>, Status> {
        let inner = request.into_inner();
        let payload = serde_json::from_slice(&inner.payload)
            .map_err(|err| Status::invalid_argument(format!("invalid JSON payload: {err}")))?;
        let metadata = metadata_from_map(inner.metadata);
        let envelope = EventEnvelope {
            pattern: inner.pattern,
            payload,
            metadata: metadata.clone(),
        };
        let context = MicroserviceContext::new(
            self.container.clone(),
            GRPC_TRANSPORT,
            envelope.pattern.clone(),
            metadata,
        );

        self.registry
            .dispatch_event(envelope, context)
            .await
            .map_err(map_dispatch_error)?;

        Ok(Response::new(EmitResponse {}))
    }
}

// ---------------------------------------------------------------------------
// 出站：GrpcMicroserviceClient
// ---------------------------------------------------------------------------

/// 出站通用通道客户端：实现 `MicroserviceClient`，把 `send`/`emit` 发往远端节点。
///
/// `GrpcMicroserviceClient` 可 `Clone`，适合注册为 DI provider 或共享给多处调用。
/// tonic 的 `Channel` 具备自动重连能力，断开后会透明重连。
#[derive(Clone)]
pub struct GrpcMicroserviceClient {
    addr: String,
    client: NestForgeDispatchClient<Channel>,
    default_metadata: TransportMetadata,
}

impl GrpcMicroserviceClient {
    /// 建立到远端通用通道的连接（立即握手，失败即返回错误）。
    ///
    /// `addr` 形如 `"http://127.0.0.1:50051"`；未带 scheme 时自动补 `http://`。
    pub async fn connect(addr: impl Into<String>) -> Result<Self> {
        let addr = normalize_addr(addr.into());
        let channel = tonic::transport::Endpoint::from_shared(addr.clone())
            .with_context(|| format!("invalid gRPC address `{addr}`"))?
            .connect()
            .await
            .with_context(|| format!("failed to connect gRPC channel to `{addr}`"))?;

        Ok(Self {
            addr,
            client: NestForgeDispatchClient::new(channel),
            default_metadata: TransportMetadata::default(),
        })
    }

    /// 惰性建连：返回即用，首次调用时才真正握手。
    /// 适合放在 provider 工厂中同步构造。
    pub fn connect_lazy(addr: impl Into<String>) -> Result<Self> {
        let addr = normalize_addr(addr.into());
        let channel = Channel::from_shared(addr.clone())
            .with_context(|| format!("invalid gRPC address `{addr}`"))?
            .connect_lazy();

        Ok(Self {
            addr,
            client: NestForgeDispatchClient::new(channel),
            default_metadata: TransportMetadata::default(),
        })
    }

    /// 为每次调用附加默认元数据。
    pub fn with_default_metadata(mut self, metadata: TransportMetadata) -> Self {
        self.default_metadata = metadata;
        self
    }

    /// 返回目标地址。
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// 返回底层生成的 tonic 客户端（需要直接调用其它 proto 服务时可取用）。
    pub fn client(&self) -> &NestForgeDispatchClient<Channel> {
        &self.client
    }
}

impl MicroserviceClient for GrpcMicroserviceClient {
    fn send<Payload, Response>(
        &self,
        pattern: impl Into<String>,
        payload: Payload,
    ) -> Pin<Box<dyn Future<Output = Result<Response>> + Send>>
    where
        Payload: Serialize + Send + 'static,
        Response: DeserializeOwned + Send + 'static,
    {
        let mut client = self.client.clone();
        let pattern = pattern.into();
        let metadata = metadata_to_map(&self.default_metadata);

        Box::pin(async move {
            let payload = serde_json::to_vec(&payload)
                .context("failed to serialize outbound message payload")?;
            let response = client
                .invoke(InvokeRequest {
                    pattern,
                    payload,
                    metadata,
                })
                .await
                .map_err(|err| anyhow!("gRPC invoke failed: {}", err.message()))?
                .into_inner();

            serde_json::from_slice(&response.payload)
                .context("failed to deserialize outbound message response")
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
        let mut client = self.client.clone();
        let pattern = pattern.into();
        let metadata = metadata_to_map(&self.default_metadata);

        Box::pin(async move {
            let payload = serde_json::to_vec(&payload)
                .context("failed to serialize outbound event payload")?;
            client
                .emit(EmitRequest {
                    pattern,
                    payload,
                    metadata,
                })
                .await
                .map_err(|err| anyhow!("gRPC emit failed: {}", err.message()))?;
            Ok(())
        })
    }
}

fn normalize_addr(addr: String) -> String {
    if addr.contains("://") {
        addr
    } else {
        format!("http://{addr}")
    }
}
