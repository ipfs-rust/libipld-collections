use criterion::black_box;
use ipld_vec::{mock::Store, Vector};
use libipld::{DefaultPrefix, Ipld};

type IpldVec = Vector<DefaultPrefix, Store>;

fn main() {
    // push: 1024xi128; n: 4; width: 256; size: 4096
    let mut vec = IpldVec::new(256).unwrap();
    for i in 0..1024 {
        vec.push(Ipld::Integer(i as i128)).unwrap();
    }
    black_box(vec);
}
