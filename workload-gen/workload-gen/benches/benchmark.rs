use std::io::sink;
use criterion::{criterion_group, criterion_main, Criterion};
use workload_gen::{write_operations, spec::WorkloadSpec};

fn bench_1m_i__1m_i_1m_u() {
    let spec_str = include_str!("../test_specs/benchmarks/1m_i-1m_i_1m_u.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    write_operations(&mut sink(), &spec).unwrap();
}
fn bench_1m_i__1m_i_1m_d() {
    let spec_str = include_str!("../test_specs/benchmarks/1m_i-1m_i-1m_d.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    write_operations(&mut sink(), &spec).unwrap();
}
fn bench_1m_i__1m_i_1m_pq() {
    let spec_str = include_str!("../test_specs/benchmarks/1m_i-1m_i_1m_pq.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
     write_operations(&mut sink(), &spec).unwrap();
}
fn bench_10k_i__10k_i_10k_rq() {
    let spec_str = include_str!("../test_specs/benchmarks/10k_i-10k_i_10k_rq.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    write_operations(&mut sink(), &spec).unwrap();
}
fn bench_10k_i__100k_i_100_rq() {
    let spec_str = include_str!("../test_specs/benchmarks/10k_i-100k_i_100_rq.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    write_operations(&mut sink(), &spec).unwrap();
}
fn bench_10k_i__100_i_10k_rq() {
    let spec_str = include_str!("../test_specs/benchmarks/100k_i-100_i_10k_rq.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    write_operations(&mut sink(), &spec).unwrap();
}

fn bench_10k_i__10k_rq() {
    let spec_str = include_str!("../test_specs/benchmarks/1m_i-1m_rq.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    write_operations(&mut sink(), &spec).unwrap();
}



fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("insert + update", |b| b.iter(|| bench_1m_i__1m_i_1m_u()));
    c.bench_function("insert + delete", |b| b.iter(|| bench_1m_i__1m_i_1m_d()));
    c.bench_function("insert + point query", |b| b.iter(|| bench_1m_i__1m_i_1m_pq()));
    c.bench_function("insert + range query (even)", |b| b.iter(|| bench_10k_i__10k_i_10k_rq()));
    c.bench_function("insert + range query (heavy i)", |b| b.iter(|| bench_10k_i__100k_i_100_rq()));
    c.bench_function("insert + range query (heavy rq)", |b| b.iter(|| bench_10k_i__100_i_10k_rq()));
    c.bench_function("range query", |b| b.iter(|| bench_10k_i__10k_rq()));
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = criterion_benchmark
);
criterion_main!(benches);
