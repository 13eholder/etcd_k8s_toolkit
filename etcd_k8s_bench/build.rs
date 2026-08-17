fn main() {
    println!("cargo:rerun-if-changed=../proto");
    tonic_prost_build::configure()
        .compile_protos(&["../proto/rpc.proto"], &["../proto"])
        .expect("failed to compile proto");
}
