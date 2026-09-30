// The CSI protocol, compiled without an external protoc so any cargo can build
// the agent.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/csi.proto");
    let descriptors = protox::compile(["csi.proto"], ["proto"])?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .generate_default_stubs(true)
        .compile_fds(descriptors)?;
    Ok(())
}
