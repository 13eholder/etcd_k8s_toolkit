fn main() {
    tonic_prost_build::configure()
        .out_dir("src/pb")
        .compile_protos(&["proto/rpc.proto"], &["proto"])
        .expect("failed to compile proto!");
}
