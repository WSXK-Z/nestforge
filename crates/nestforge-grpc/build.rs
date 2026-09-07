fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 使用随 crate 分发的 vendored protoc，避免依赖系统 protoc。
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/nestforge.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/nestforge.proto");
    Ok(())
}
