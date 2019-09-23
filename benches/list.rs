use async_std::task;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ipld_collections::List;
use ipld_daemon::BlockStore;
use libipld::{BufStore, DefaultHash as H, Ipld, MemStore};
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

fn from_mem(c: &mut Criterion) {
    let mut data = Vec::with_capacity(1024);
    for i in 0..1024 {
        data.push(Ipld::Integer(i as i128));
    }

    let store = Arc::new(MemStore::default());

    c.bench_function("from mem: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            task::block_on(async {
                let vec = List::<_, H>::from(store.clone(), "bench_from_mem", 256, data.clone())
                    .await
                    .unwrap();
                black_box(vec);
            });
        })
    });
}

fn from_fs(c: &mut Criterion) {
    let mut data = Vec::with_capacity(1024);
    for i in 0..1024 {
        data.push(Ipld::Integer(i as i128));
    }

    let store = task::block_on(BlockStore::connect("ipld_collections")).unwrap();
    let store = Arc::new(BufStore::new(store, 16, 16));

    c.bench_function("from fs: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            task::block_on(async {
                let list = List::<_, H>::from(store.clone(), "bench_from_fs", 256, data.clone())
                    .await
                    .unwrap();
                list.flush().await.unwrap();
            });
        })
    });
}

fn push_mem(c: &mut Criterion) {
    let store = Arc::new(MemStore::default());

    c.bench_function("push mem: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            task::block_on(async {
                let list = List::<_, H>::new(store.clone(), "bench_push_mem", 256)
                    .await
                    .unwrap();
                for i in 0..1024 {
                    list.push(Ipld::Integer(i as i128)).await.unwrap();
                }
                list.flush().await.unwrap();
            });
        })
    });
}

fn push_fs(c: &mut Criterion) {
    let store = task::block_on(BlockStore::connect("ipld_collections")).unwrap();
    let store = Arc::new(BufStore::new(store, 16, 16));

    c.bench_function("push fs: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            task::block_on(async {
                let list = List::<_, H>::new(store.clone(), "bench_push_fs", 256)
                    .await
                    .unwrap();
                for i in 0..1024 {
                    list.push(Ipld::Integer(i as i128)).await.unwrap();
                }
                list.flush().await.unwrap();
            });
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = baseline, from_mem, from_fs, push_mem, push_fs
}

criterion_main!(benches);
