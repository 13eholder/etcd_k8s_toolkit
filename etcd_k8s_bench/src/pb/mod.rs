#[allow(dead_code)]
pub mod authpb {
    include!(concat!(env!("OUT_DIR"), "/authpb.rs"));
}

pub mod mvccpb {
    include!(concat!(env!("OUT_DIR"), "/mvccpb.rs"));
}

#[allow(dead_code)]
pub mod etcdserverpb {
    include!(concat!(env!("OUT_DIR"), "/etcdserverpb.rs"));
}
