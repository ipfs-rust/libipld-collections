use block_cache::BlockCache;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ipld_collections::{mock::MemStore, BlockStore, DefaultHash, Ipld, List};

type MemList = List<MemStore, BlockCache, DefaultHash>;

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

    c.bench_function("from: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            let store = BlockStore::new(16);
            let vec = MemList::from(store, 256, data.clone()).unwrap();
            black_box(vec);
        })
    });
}

fn push(c: &mut Criterion) {
    c.bench_function(
        "default push: 1024xi128; n: 4; width: 256; size: 4096",
        |b| {
            b.iter(|| {
                let store = BlockStore::new(16);
                let mut list = MemList::new(store, 256).unwrap();
                for i in 0..1024 {
                    list.push(Ipld::Integer(i as i128)).unwrap();
                }
                black_box(list);
            })
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(30);
    targets = baseline, from, push
}

criterion_main!(benches);
