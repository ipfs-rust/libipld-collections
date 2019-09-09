use criterion::black_box;
use ipld_collections::{mock::MemStore, DefaultPrefix, Ipld, List};

fn main() {
    // push: 1024xi128; n: 4; width: 256; size: 4096
    let mut list = List::<DefaultPrefix, MemStore>::new(256).unwrap();
    for i in 0..1024 {
        list.push(Ipld::Integer(i as i128)).unwrap();
    }
    black_box(list);
}
