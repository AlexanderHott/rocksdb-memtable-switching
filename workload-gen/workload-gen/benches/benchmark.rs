use std::io::sink;
use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;
use workload_gen::{write_operations, spec::WorkloadSpec};

fn bench_1m_i__1m_u() {
    let spec_str = include_str!("../test_specs/benchmarks/1m_i-1m_u.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    write_operations(&mut sink(), &spec).unwrap();
}
fn bench_1m_i__1m_d() {
    let spec_str = include_str!("../test_specs/benchmarks/1m_i-1m_d.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    let operations = write_operations(&mut sink(), &spec).unwrap();
}
fn bench_1m_i__1m_pq() {
    let spec_str = include_str!("../test_specs/benchmarks/1m_i-1m_pq.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    let operations = write_operations(&mut sink(), &spec).unwrap();
}
fn bench_1m_i__1m_rq() {
    let spec_str = include_str!("../test_specs/benchmarks/1m_i-1m_rq.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    let operations = write_operations(&mut sink(), &spec).unwrap();
}



fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("1m_i__1m_u", |b| b.iter(|| bench_1m_i__1m_u()));
    // c.bench_function("1m_i__1m_d", |b| b.iter(|| bench_1m_i__1m_d()));
    c.bench_function("1m_i__1m_pq", |b| b.iter(|| bench_1m_i__1m_pq()));
    c.bench_function("1m_i__1m_rq", |b| b.iter(|| bench_1m_i__1m_rq()));
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(100));
    targets = criterion_benchmark
);
criterion_main!(benches);
