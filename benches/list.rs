use async_std::task;
use block_cache::BlockCache;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ipld_collections::{create_store, create_list, List};
use libipld::{BlockStore, DefaultHash, Ipld, mock::MemStore};
use std::sync::Arc;

fn baseline(c: &mut Criterion) {
    c.bench_function("Create Vec 1024xi128. size: 1024 * 16", |b| {
        b.iter(|| {
            let mut data = Vec::with_capacity(1024);
            for i in 0..1024 {
                data.push(Ipld::Integer(i as i128));
            }
            black_box(data);
        })
    });
}

fn from(c: &mut Criterion) {
    let mut data = Vec::with_capacity(1024);
    for i in 0..1024 {
        data.push(Ipld::Integer(i as i128));
    }
    
    let store = task::block_on(create_store(16)).unwrap();

    c.bench_function("from: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            task::block_on(async {
                let vec = List::<_, _, DefaultHash>::from(store.clone(), 256, data.clone())
                    .await
                    .unwrap();
                black_box(vec);
            });
        })
    });
}

fn push_mem(c: &mut Criterion) {
    let store = Arc::new(BlockStore::new(MemStore::default(), BlockCache::new(16)));

    c.bench_function("push mem: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            task::block_on(async {
                let mut list = List::<MemStore, BlockCache, DefaultHash>::new(store.clone(), 256).await.unwrap();
                for i in 0..1024 {
                    list.push(Ipld::Integer(i as i128)).await.unwrap();
                }
                black_box(list);
            });
        })
    });
}

fn push_fs(c: &mut Criterion) {
    let store = task::block_on(create_store(16)).unwrap();

    c.bench_function("push fs: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            task::block_on(async {
                let mut list = create_list(store.clone(), 256).await.unwrap();
                for i in 0..1024 {
                    list.push(Ipld::Integer(i as i128)).await.unwrap();
                }
                black_box(list);
            });
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = baseline, from, push_mem, push_fs
}

criterion_main!(benches);
