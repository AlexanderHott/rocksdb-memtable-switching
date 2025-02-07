use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;
use workload_gen::{generate_operations2, spec::WorkloadSpec};

fn bench_1m_i() {
    let spec_str = include_str!("../test_specs/1m_i.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    let operations = generate_operations2(spec).unwrap();
}

fn bench_1m_i_1m_rq() {
    let spec_str = include_str!("../test_specs/1m_i-1m_rq.json");
    let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
    let operations = generate_operations2(spec).unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("1m_i", |b| b.iter(|| bench_1m_i()));
    c.bench_function("1m_i-1m_rq", |b| b.iter(|| bench_1m_i_1m_rq()));
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(100));
    targets = criterion_benchmark
);
criterion_main!(benches);
