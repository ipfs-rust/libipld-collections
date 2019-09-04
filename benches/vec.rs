use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ipld_vec::{mock::Store, Vector};
use libipld::{DefaultPrefix, Ipld};

type IpldVec = Vector<DefaultPrefix, Store>;

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
            let vec = IpldVec::from(256, data.clone()).unwrap();
            black_box(vec);
        })
    });
}

fn push(c: &mut Criterion) {
    c.bench_function("push: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            let mut vec = IpldVec::new(256).unwrap();
            for i in 0..1024 {
                vec.push(Ipld::Integer(i as i128)).unwrap();
            }
            black_box(vec);
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = baseline, from, push
}

criterion_main!(benches);
