# gRPC

NestForge includes optional gRPC transport support through the `nestforge-grpc` crate.

## Enable The Feature

```toml
nestforge = { version = "1", features = ["grpc"] }
```

## Core Pieces

- `NestForgeGrpcFactory::<AppModule>`
- `GrpcServerConfig`
- `GrpcContext`
- re-exported `tonic` and `prost`
- `examples/hello-nestforge-grpc` for a complete tonic setup

## Minimal Bootstrap

```rust
use nestforge::NestForgeGrpcFactory;

NestForgeGrpcFactory::<AppModule>::create()?
    .with_addr("127.0.0.1:50051")
    .listen_with(|ctx, addr| async move {
        tonic::transport::Server::builder()
            // .add_service(MyGeneratedServer::new(MyGrpcService::new(ctx)))
            .serve(addr)
            .await
    })
    .await?;
```

## Dependency Resolution Inside Services

`GrpcContext` gives generated tonic service implementations access to the NestForge container:

```rust
#[derive(Clone)]
struct GreeterService {
    ctx: nestforge::GrpcContext,
}

impl GreeterService {
    fn new(ctx: nestforge::GrpcContext) -> Self {
        Self { ctx }
    }
}
```

Then resolve shared providers as needed:

```rust
let config = self.ctx.resolve::<AppConfig>()?;
```

## Microservice Registry Adapter

If you enable both `grpc` and `microservices`, a tonic service can delegate pattern handling into `MicroserviceRegistry`:

```rust
let response = nestforge::dispatch_grpc_message(
    &self.ctx,
    &registry,
    "users.count",
    (),
    nestforge::TransportMetadata::new(),
)
.await?;
```

`dispatch_grpc_event(...)` is also available for fire-and-forget patterns.

The `hello-nestforge-grpc` example now routes `say_hello` through this adapter with the `hello.say` pattern.

## Codegen Setup

The gRPC-first example uses a standard tonic build pipeline:

- `proto/greeter.proto` defines the service contract
- `build.rs` compiles proto files during the cargo build
- `tonic::include_proto!(...)` loads the generated bindings

Example `build.rs`:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/greeter.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/greeter.proto");
    Ok(())
}
```

This follows the default tonic toolchain, so `protoc` needs to be available when you build the example.

## Transport Shape

This setup mirrors the NestJS transport approach more than the HTTP controller approach:

- your module graph and providers still come from NestForge
- tonic-generated services remain the transport boundary
- `NestForgeGrpcFactory` wires DI and runtime address handling around that service layer

## Example App

Run the gRPC-first example from the workspace root:

```bash
cargo run -p hello-nestforge-grpc
```

It listens on `127.0.0.1:50051` and exposes the generated `Greeter` service.

## Generic Microservice Channel (Inbound + Outbound)

Beyond the typed, hand-written tonic service path above, `nestforge-grpc` ships a
**transport-agnostic generic channel** (`proto/nestforge.proto`) so any two NestForge nodes
can exchange `pattern + JSON payload` messages without writing a proto service per RPC.

### Inbound: `GrpcDispatchService`

A ready-made tonic service that dispatches remote `Invoke`/`Emit` envelopes into the local
`MicroserviceRegistry` (handlers keep using DI via `ctx.resolve<T>()`):

```rust
// 在 listen_with 里把入站通道挂到同一根 tonic Server 上
NestForgeGrpcFactory::<AppModule>::create()?
    .with_addr("127.0.0.1:50051")
    .listen_with(|ctx, addr| async move {
        let patterns = ctx.resolve::<GrpcPatterns>()?;   // 你的注册表 provider
        let dispatch = nestforge::GrpcDispatchService::new(
            ctx.container().clone(),
            patterns.registry().clone(),
        );
        nestforge::tonic::transport::Server::builder()
            .add_service(GreeterServer::new(GreeterGrpcService::new(ctx))) // typed RPC 可共存
            .add_service(dispatch.into_server())                            // 通用通道
            .serve(addr)
            .await
    })
    .await?;
```

### Outbound: `GrpcMicroserviceClient`

Implements `MicroserviceClient`, so `send`/`emit` go over the wire to a remote node:

```rust
use nestforge::MicroserviceClient;

let client = nestforge::GrpcMicroserviceClient::connect_lazy("127.0.0.1:50051")?
    .with_default_metadata(TransportMetadata::new().insert("env", "test"));

let count: usize = client.send("users.count", ()).await?;   // 请求-响应
client.emit("users.created", CreateUserEvent { user_id: 7 }).await?; // 即发即忘
```

- `connect(addr)` 立即握手；`connect_lazy(addr)` 首调用才建连（适合 provider 工厂）。
- tonic `Channel` 自动重连；地址未带 scheme 时自动补 `http://`。
- 入站 `MicroserviceContext.transport()` 为 `"grpc"`；默认元数据会透传到远端 handler 的 `ctx.metadata()`。
