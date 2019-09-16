use block_cache::BlockCache;
use criterion::black_box;
use ipld_collections::{mock::MemStore, BlockStore, DefaultHash, Ipld, List};

fn main() {
    let store = BlockStore::new(16);
    // push: 1024xi128; n: 4; width: 256; size: 4096
    let mut list = List::<MemStore, BlockCache, DefaultHash>::new(store, 256).unwrap();
    for i in 0..1024 {
        list.push(Ipld::Integer(i as i128)).unwrap();
    }
    black_box(list);
}
