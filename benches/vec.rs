use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ipld_vec::{mock::Store, Vector};
use libipld::{Ipld, Prefix};

struct Default;
impl Prefix for Default {
    type Codec = libipld::DagCbor;
    type Hash = libipld::Blake2s;
}

struct Json;
impl Prefix for Json {
    type Codec = libipld::DagJson;
    type Hash = libipld::Blake2b;
}

struct Sha2;
impl Prefix for Sha2 {
    type Codec = libipld::DagCbor;
    type Hash = libipld::Sha2_256;
}

type DefaultVec = Vector<Default, Store>;
type JsonVec = Vector<Json, Store>;
type Sha2Vec = Vector<Sha2, Store>;

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
            let vec = DefaultVec::from(256, data.clone()).unwrap();
            black_box(vec);
        })
    });
}

fn push(c: &mut Criterion) {
    c.bench_function(
        "default push: 1024xi128; n: 4; width: 256; size: 4096",
        |b| {
            b.iter(|| {
                let mut vec = DefaultVec::new(256).unwrap();
                for i in 0..1024 {
                    vec.push(Ipld::Integer(i as i128)).unwrap();
                }
                black_box(vec);
            })
        },
    );
}

fn push_json(c: &mut Criterion) {
    c.bench_function("json push: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            let mut vec = JsonVec::new(256).unwrap();
            for i in 0..1024 {
                vec.push(Ipld::Integer(i as i128)).unwrap();
            }
            black_box(vec);
        })
    });
}

fn push_sha2(c: &mut Criterion) {
    c.bench_function("sha2 push: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            let mut vec = Sha2Vec::new(256).unwrap();
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
    targets = baseline, from, push, push_json, push_sha2
}

criterion_main!(benches);
