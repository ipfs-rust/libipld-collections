use async_std::task;
use block_cache::BlockCache;
use block_store::BlockStore;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ipld_collections::{BlockStore as Store, DefaultHash, Ipld, List};
use tempdir::TempDir;
use std::sync::Arc;

type MemList = List<BlockStore, BlockCache, DefaultHash>;

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

    let tmp = TempDir::new("from").unwrap();
    let store = Arc::new(Store::new(tmp.path().into(), 16));

    c.bench_function("from: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            task::block_on(async {
                let vec = MemList::from(store.clone(), 256, data.clone()).await.unwrap();
                black_box(vec);
            });
        })
    });

    tmp.close().unwrap();
}

fn push(c: &mut Criterion) {
    let tmp = TempDir::new("push").unwrap();
    let store = Arc::new(Store::new(tmp.path().into(), 16));
    
    c.bench_function(
        "default push: 1024xi128; n: 4; width: 256; size: 4096",
        |b| {
            b.iter(|| {
                task::block_on(async {
                    let mut list = MemList::new(store.clone(), 256).await.unwrap();
                    for i in 0..1024 {
                        list.push(Ipld::Integer(i as i128)).await.unwrap();
                    }
                    black_box(list);
                });
            })
        },
    );

    tmp.close().unwrap();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = baseline, from, push
}

criterion_main!(benches);
